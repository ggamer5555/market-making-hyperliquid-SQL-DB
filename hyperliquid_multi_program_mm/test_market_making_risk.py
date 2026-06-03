from __future__ import annotations

import json
import math
import sqlite3
import tempfile
import unittest
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import settings
from sqlalchemy import select
from common import ceil_to_step, floor_to_step, utc_ms
from connectors.base import ExchangeConnector, MarketTrade, OpenOrder, OrderBookLevel, OrderEdit, OrderResult, TradeFill
from scripts.market_making import DesiredOrder, MarketMakingScript


class FakeConnector(ExchangeConnector):
    """Offline connector used to verify strategy sizing without live orders."""

    name = "fake"
    account_address = "0xtest"

    def __init__(self) -> None:
        self.orders: List[OpenOrder] = []
        self.submissions: List[Dict[str, Any]] = []
        self.edit_batches: List[List[OrderEdit]] = []
        self.leverage_updates: List[Dict[str, Any]] = []
        self.market_trades: List[MarketTrade] = []
        self.recent_market_trade_requests: List[Dict[str, Any]] = []
        self.fills: List[TradeFill] = []
        self.fill_fetch_count = 0
        self.place_attempts: List[Dict[str, Any]] = []
        self.cancel_count = 0
        self.force_open_order_fetch_count = 0
        self.reject_next_order = False
        self.next_exchange_order_id = 1
        self.mid = 100.0
        self.position = 0.0
        self.position_fetch_count = 0
        self.reject_cross_leverage = False
        self.reject_isolated_leverage_with_rate_limit = False
        self.rate_limit_raw: Dict[str, Any] = {}

    @property
    def signer_address(self) -> str:
        return "0xtest"

    @property
    def post_only_time_in_force(self) -> str:
        return "Alo"

    def symbol_for_market(self, market: str) -> str:
        return f"{market}/USDC:USDC"

    def make_client_order_id(self) -> str:
        return "0x" + uuid.uuid4().hex

    def fetch_bbo(self, market: str) -> Tuple[float, float, float]:
        bids, asks = self.fetch_order_book(market, depth=1)
        return bids[0].price, asks[0].price, self.mid

    def fetch_order_book(
        self, market: str, depth: Optional[int] = None
    ) -> Tuple[List[OrderBookLevel], List[OrderBookLevel]]:
        bids = [
            OrderBookLevel(self.mid - offset, 100.0, 1, {})
            for offset in (0.01, 0.10, 0.20, 0.30)
        ]
        asks = [
            OrderBookLevel(self.mid + offset, 100.0, 1, {})
            for offset in (0.01, 0.10, 0.20, 0.30)
        ]
        return bids[:depth], asks[:depth]

    def fetch_open_orders(self, market: Optional[str] = None, *, force: bool = False) -> List[OpenOrder]:
        if force:
            self.force_open_order_fetch_count += 1
        return [order for order in self.orders if market is None or order.market == market]

    def fetch_position_size(self, market: str) -> Tuple[float, Dict[str, Any]]:
        self.position_fetch_count += 1
        return self.position, {"szi": self.position}

    def fetch_positions(self) -> List[Dict[str, Any]]:
        return []

    def fetch_recent_fills(
        self,
        start_ms: int,
        end_ms: Optional[int] = None,
        aggregate_by_time: bool = False,
    ) -> List[TradeFill]:
        self.fill_fetch_count += 1
        return [
            fill
            for fill in self.fills
            if fill.timestamp_ms is None or fill.timestamp_ms >= start_ms
        ]

    def fetch_cached_market_trades(self, market: str, since_timestamp_ms: int = 0) -> List[MarketTrade]:
        return [
            trade
            for trade in self.market_trades
            if trade.market == market.upper()
            and (trade.timestamp_ms is None or trade.timestamp_ms >= since_timestamp_ms)
        ]

    def fetch_recent_market_trades(self, market: str, limit: int = 2000) -> List[MarketTrade]:
        self.recent_market_trade_requests.append({"market": market, "limit": limit})
        return self.market_trades[-limit:] if limit > 0 else list(self.market_trades)

    def fetch_account_summary(self) -> Dict[str, Any]:
        return {}

    def fetch_user_rate_limit(self) -> Dict[str, Any]:
        return self.rate_limit_raw

    def fetch_market_max_leverage(self, market: str) -> int:
        return 3

    def set_market_leverage(self, market: str, leverage: int, *, is_cross: bool = True) -> Dict[str, Any]:
        update = {"market": market, "leverage": leverage, "is_cross": is_cross}
        self.leverage_updates.append(update)
        if is_cross and self.reject_cross_leverage:
            raise RuntimeError("set leverage rejected market=xyz:AMD errors=[] raw={'status': 'err', 'response': 'Cross margin is not allowed for this asset.'}")
        if not is_cross and self.reject_isolated_leverage_with_rate_limit:
            raise RuntimeError("set leverage rejected market=xyz:AMD errors=[] raw={'status': 'err', 'response': 'Too many cumulative requests sent (37332 > 37315) for cumulative volume traded $27316.78. Place taker orders to free up 1 request per USDC traded.'}")
        return update

    def round_size(self, market: str, size: float) -> float:
        return floor_to_step(size, 0.01)

    def round_size_up(self, market: str, size: float) -> float:
        return ceil_to_step(size, 0.01)

    def round_price(self, market: str, side: str, price: float) -> float:
        return round(price, 2)

    def price_step(self, market: str, price: float) -> float:
        return 0.01

    def place_limit_order(
        self,
        market: str,
        side: str,
        size: float,
        price: float,
        *,
        reduce_only: bool = False,
        post_only: bool = True,
        client_order_id: Optional[str] = None,
    ) -> OrderResult:
        client_order_id = client_order_id or self.make_client_order_id()
        submission = {
            "market": market,
            "side": side,
            "size": size,
            "price": price,
            "notional": size * price,
            "reduce_only": reduce_only,
        }
        self.place_attempts.append(submission)
        if self.reject_next_order:
            self.reject_next_order = False
            raise RuntimeError("Post only order would have immediately matched")
        exchange_order_id = str(self.next_exchange_order_id)
        self.next_exchange_order_id += 1
        self.submissions.append(submission)
        self.orders.append(
            OpenOrder(
                exchange=self.name,
                market=market,
                symbol=self.symbol_for_market(market),
                exchange_order_id=exchange_order_id,
                client_order_id=client_order_id,
                side=side,
                price=price,
                size=size,
                timestamp_ms=utc_ms(),
                raw=submission,
            )
        )
        return OrderResult(
            exchange=self.name,
            market=market,
            symbol=self.symbol_for_market(market),
            side=side,
            price=price,
            size=size,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            status="open",
            raw=submission,
        )

    def cancel_order(
        self,
        market: str,
        *,
        client_order_id: Optional[str] = None,
        exchange_order_id: Optional[str] = None,
    ) -> Tuple[bool, Dict[str, Any]]:
        self.cancel_count += 1
        before = len(self.orders)
        self.orders = [
            order
            for order in self.orders
            if not (
                order.market == market
                and (client_order_id is None or order.client_order_id == client_order_id)
                and (exchange_order_id is None or order.exchange_order_id == exchange_order_id)
            )
        ]
        return len(self.orders) < before, {"status": "cancelled"}

    def bulk_edit_orders(self, edits: List[OrderEdit]) -> List[OrderResult]:
        self.edit_batches.append(edits)
        results: List[OrderResult] = []
        for edit in edits:
            for order in self.orders:
                if order.exchange_order_id != edit.order.exchange_order_id:
                    continue
                order.price = edit.price
                order.size = edit.size
                order.raw = {"reduceOnly": edit.reduce_only}
                results.append(
                    OrderResult(
                        exchange=self.name,
                        market=order.market,
                        symbol=order.symbol,
                        side=order.side,
                        price=order.price,
                        size=order.size,
                        client_order_id=order.client_order_id or "",
                        exchange_order_id=order.exchange_order_id,
                        status="open",
                        raw=order.raw,
                    )
                )
        return results


class MarketMakingRiskTest(unittest.TestCase):
    def setUp(self) -> None:
        self.old_settings = {
            name: getattr(settings, name)
            for name in (
                "TARGET_ORDER_NOTIONAL_USD",
                "MIN_OPEN_ORDER_NOTIONAL_USD",
                "MAX_LONG_INVENTORY_NOTIONAL_USD",
                "MAX_SHORT_INVENTORY_NOTIONAL_USD",
                "ORDERS_PER_SIDE",
                "MARKETS",
                "COINS",
                "BULK_EDIT_INTERVAL_S",
                "SYNC_MAX_LEVERAGE",
                "FETCH_FILLS_S",
                "FORCE_REST_REFRESH_AFTER_ORDER_REJECT",
                "CANCEL_ON_FILL_FORCE_REST_REFRESH",
                "USE_EV_SPREAD_PRICING",
                "EV_MIN_HALF_SPREAD_BPS",
                "EV_MAX_HALF_SPREAD_BPS",
                "EV_HALF_SPREAD_STEP_BPS",
                "EV_MAX_FILLS_PER_HOUR",
                "EV_TRADE_LOOKBACK_S",
                "EV_SWEEP_GROUP_MS",
                "EV_DEPTH_WITHIN_MID_PCT",
                "EV_USE_CURRENT_BOOK_DEPTH",
                "EV_MIN_TRADE_SAMPLES",
                "EV_REQUIRE_READY_BEFORE_OPENING",
                "EV_STARTUP_USE_STORED_MARKET_DATA",
                "EV_STARTUP_WAIT_TIMEOUT_S",
                "EV_STARTUP_FALLBACK_AFTER_TIMEOUT",
                "EV_REUSE_LAST_CHOICE_WHEN_NOT_READY",
                "EV_CACHED_CHOICE_MAX_AGE_S",
                "EV_STARTUP_BACKFILL_RECENT_MARKET_TRADES",
                "EV_STARTUP_RECENT_MARKET_TRADES_LIMIT",
                "USER_FILLS_BACKFILL_LOOKBACK_HOURS",
                "USER_FILLS_AGGREGATE_BY_TIME",
                "CANCEL_SYMBOL_ORDERS_ON_FILL",
                "CANCEL_ON_FILL_GUARD_S",
                "MAKER_FEE_BPS_PER_SIDE",
                "EV_MARKOUT_BPS",
                "MARKET_DATA_DB_PATH",
                "USE_LOB_VWAP_FAIR_PRICE",
                "MIN_HALF_SPREAD_BPS",
                "MAX_HALF_SPREAD_BPS",
                "MIN_QUOTE_SPREAD_BPS",
            )
        }
        settings.TARGET_ORDER_NOTIONAL_USD = 10.0
        settings.MIN_OPEN_ORDER_NOTIONAL_USD = 10.0
        settings.ORDERS_PER_SIDE = 2
        settings.MARKETS = ["BTC"]
        settings.COINS = settings.MARKETS
        settings.PRIMARY_SQL_URL = "sqlite:///:memory:"
        settings.SQLITE_URL = "sqlite:///:memory:"
        settings.REPLICATION_SPOOL_FILE = "./unused_fake_test_spool.jsonl"
        settings.SYNC_MAX_LEVERAGE = True
        settings.FETCH_FILLS_S = 60.0
        settings.FORCE_REST_REFRESH_AFTER_ORDER_REJECT = False
        settings.CANCEL_ON_FILL_FORCE_REST_REFRESH = False
        settings.EV_REQUIRE_READY_BEFORE_OPENING = False
        settings.EV_STARTUP_USE_STORED_MARKET_DATA = False

    def tearDown(self) -> None:
        for name, value in self.old_settings.items():
            setattr(settings, name, value)

    def run_scenario(self, inventory: float) -> List[Dict[str, Any]]:
        connector = FakeConnector()
        script = MarketMakingScript(connector)
        script.db.create_all()
        script.positions["BTC"] = (inventory, {})
        script.run_coin("BTC")
        return connector.submissions

    def test_opening_quotes_round_up_to_ten_dollars(self) -> None:
        orders = self.run_scenario(0.0)
        self.assertEqual({order["side"] for order in orders}, {"buy", "sell"})
        self.assertEqual(len(orders), 2)
        self.assertTrue(all(not order["reduce_only"] for order in orders))
        self.assertTrue(all(order["notional"] >= 10.0 for order in orders))
        self.assertLessEqual(next(order["price"] for order in orders if order["side"] == "buy"), 99.95)
        self.assertGreaterEqual(next(order["price"] for order in orders if order["side"] == "sell"), 100.05)

    def test_startup_ev_gate_waits_before_opening_orders(self) -> None:
        settings.USE_EV_SPREAD_PRICING = True
        settings.EV_REQUIRE_READY_BEFORE_OPENING = True
        settings.EV_STARTUP_USE_STORED_MARKET_DATA = False
        settings.EV_REUSE_LAST_CHOICE_WHEN_NOT_READY = False
        connector = FakeConnector()
        script = MarketMakingScript(connector)
        script.db.create_all()
        script.positions["BTC"] = (0.0, {})

        script.run_coin("BTC")

        self.assertEqual(connector.submissions, [])
        self.assertTrue(script.latest_market_diagnostics["BTC"]["ev_startup_waiting"])

    def write_startup_ev_history(self, db_path: Path) -> None:
        base_ts = utc_ms() - 3_000
        with sqlite3.connect(str(db_path)) as conn:
            conn.executescript(
                """
                CREATE TABLE market_trades (
                    trade_key TEXT PRIMARY KEY,
                    coin TEXT NOT NULL,
                    trade_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL NOT NULL,
                    size REAL NOT NULL,
                    timestamp_ms INTEGER,
                    raw_json TEXT NOT NULL
                );
                CREATE TABLE market_snapshots (
                    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    coin TEXT NOT NULL,
                    captured_at_ms INTEGER NOT NULL,
                    mid_price REAL NOT NULL,
                    diagnostics_json TEXT NOT NULL,
                    bids_json TEXT NOT NULL,
                    asks_json TEXT NOT NULL
                );
                """
            )
            for index in range(3):
                ts_ms = base_ts + index * 1_000
                conn.execute(
                    """
                    INSERT INTO market_snapshots
                        (coin, captured_at_ms, mid_price, diagnostics_json, bids_json, asks_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "BTC",
                        ts_ms - 10,
                        100.0,
                        json.dumps({"fair_price": 100.0}),
                        json.dumps([{"price": 99.95, "size": 0.01}]),
                        json.dumps([{"price": 100.05, "size": 0.01}]),
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO market_trades
                        (trade_key, coin, trade_id, side, price, size, timestamp_ms, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"trade-{index}",
                        "BTC",
                        f"trade-{index}",
                        "buy",
                        100.60,
                        5.0,
                        ts_ms,
                        "{}",
                    ),
                )
            conn.commit()

    def test_startup_ev_loaded_from_recorded_data_allows_opening_orders(self) -> None:
        settings.USE_EV_SPREAD_PRICING = True
        settings.EV_REQUIRE_READY_BEFORE_OPENING = True
        settings.EV_STARTUP_USE_STORED_MARKET_DATA = True
        settings.EV_REUSE_LAST_CHOICE_WHEN_NOT_READY = True
        settings.EV_MIN_TRADE_SAMPLES = 1
        settings.EV_TRADE_LOOKBACK_S = 600.0
        settings.EV_SWEEP_GROUP_MS = 100.0
        settings.EV_DEPTH_WITHIN_MID_PCT = 0.001
        settings.EV_USE_CURRENT_BOOK_DEPTH = False
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            settings.MARKET_DATA_DB_PATH = str(db_path)
            self.write_startup_ev_history(db_path)
            connector = FakeConnector()
            script = MarketMakingScript(connector)
            script.db.create_all()
            script.positions["BTC"] = (0.0, {})

            script.load_startup_ev_choices()
            script.run_coin("BTC")

        self.assertGreater(len(connector.submissions), 0)
        diagnostics = script.latest_market_diagnostics["BTC"]
        self.assertTrue(diagnostics["ev_applied"])
        self.assertTrue(diagnostics["ev_reused_cached_choice"])
        self.assertEqual(diagnostics["ev_choice"]["half_spread_bps"], 50.0)

    def test_five_dollar_long_closes_reduce_only(self) -> None:
        orders = self.run_scenario(0.05)
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0]["side"], "sell")
        self.assertTrue(orders[0]["reduce_only"])
        self.assertTrue(math.isclose(orders[0]["size"], 0.05))
        self.assertLess(orders[0]["notional"], 10.0)

    def test_two_levels_per_side_use_one_bulk_edit_without_cancel_replace(self) -> None:
        settings.MAX_LONG_INVENTORY_NOTIONAL_USD = 20.0
        settings.MAX_SHORT_INVENTORY_NOTIONAL_USD = 20.0
        settings.BULK_EDIT_INTERVAL_S = 0.0
        connector = FakeConnector()
        script = MarketMakingScript(connector)
        script.db.create_all()
        script.positions["BTC"] = (0.0, {})
        script.run_coin("BTC")
        original_oids = {order.exchange_order_id for order in connector.orders}
        self.assertEqual(len(original_oids), 4)

        connector.mid = 101.0
        script.run_coin("BTC")

        self.assertEqual(connector.cancel_count, 0)
        self.assertEqual(len(connector.edit_batches), 1)
        self.assertEqual(len(connector.edit_batches[0]), 4)
        self.assertEqual({order.exchange_order_id for order in connector.orders}, original_oids)

    def test_remaining_order_is_cancelled_after_other_side_fills(self) -> None:
        settings.BULK_EDIT_INTERVAL_S = 0.0
        settings.USE_EV_SPREAD_PRICING = False
        settings.USE_LOB_VWAP_FAIR_PRICE = False
        connector = FakeConnector()
        script = MarketMakingScript(connector)
        script.db.create_all()
        script.positions["BTC"] = (0.0, {})
        script.run_coin("BTC")
        self.assertEqual({order.side for order in connector.orders}, {"buy", "sell"})

        connector.orders = [order for order in connector.orders if order.side == "sell"]
        connector.position = 0.1
        position_fetches_before = connector.position_fetch_count

        script.run_coin("BTC")

        self.assertGreater(connector.position_fetch_count, position_fetches_before)
        self.assertEqual(connector.cancel_count, 1)
        self.assertEqual(connector.orders, [])
        self.assertEqual(connector.edit_batches, [])
        self.assertEqual(len(connector.submissions), 2)

    def test_sell_edit_is_clamped_to_post_only_safe_price(self) -> None:
        settings.BULK_EDIT_INTERVAL_S = 0.0
        connector = FakeConnector()
        script = MarketMakingScript(connector)
        script.db.create_all()
        connector.orders = [
            OpenOrder(
                exchange=connector.name,
                market="BTC",
                symbol=connector.symbol_for_market("BTC"),
                exchange_order_id="1",
                client_order_id=connector.make_client_order_id(),
                side="sell",
                price=100.50,
                size=0.1,
                timestamp_ms=utc_ms(),
                raw={"reduceOnly": True},
            )
        ]
        quote = script.model.quote("BTC", 99.99, 100.01, 0.1, symbol=connector.symbol_for_market("BTC"))

        script.sync_orders("BTC", [DesiredOrder(0, "sell", 99.50, 0.1, True)], quote)

        self.assertEqual(len(connector.edit_batches), 1)
        edit = connector.edit_batches[0][0]
        self.assertEqual(edit.order.side, "sell")
        self.assertTrue(edit.reduce_only)
        self.assertGreaterEqual(edit.price, 100.0)
        self.assertLess(edit.price, 100.01)

    def test_open_order_shrink_cancels_remaining_orders_even_if_inventory_is_stale(self) -> None:
        settings.BULK_EDIT_INTERVAL_S = 0.0
        settings.USE_EV_SPREAD_PRICING = False
        settings.USE_LOB_VWAP_FAIR_PRICE = False
        connector = FakeConnector()
        script = MarketMakingScript(connector)
        script.db.create_all()
        script.positions["BTC"] = (0.0, {})
        script.run_coin("BTC")
        self.assertEqual({order.side for order in connector.orders}, {"buy", "sell"})

        connector.orders = [order for order in connector.orders if order.side == "sell"]
        connector.position = 0.0
        connector.mid = 101.0
        position_fetches_before = connector.position_fetch_count

        script.run_coin("BTC")

        self.assertGreater(connector.position_fetch_count, position_fetches_before)
        self.assertGreaterEqual(connector.force_open_order_fetch_count, 1)
        self.assertEqual(connector.cancel_count, 1)
        self.assertEqual(connector.orders, [])
        self.assertEqual(connector.edit_batches, [])
        self.assertEqual(len(connector.submissions), 2)
        self.assertEqual(len(connector.place_attempts), 2)

    def test_missing_opening_order_rechecks_live_inventory_before_placing(self) -> None:
        settings.BULK_EDIT_INTERVAL_S = 0.0
        settings.USE_EV_SPREAD_PRICING = False
        settings.USE_LOB_VWAP_FAIR_PRICE = False
        connector = FakeConnector()
        script = MarketMakingScript(connector)
        script.db.create_all()
        script.positions["BTC"] = (0.0, {})
        script.run_coin("BTC")
        self.assertEqual({order.side for order in connector.orders}, {"buy", "sell"})

        connector.orders = [order for order in connector.orders if order.side == "buy"]
        script.last_open_order_keys["BTC"] = script.open_order_keys(connector.orders)
        connector.position = -0.1
        script.positions["BTC"] = (0.0, {})
        position_fetches_before = connector.position_fetch_count

        script.run_coin("BTC")

        self.assertGreater(connector.position_fetch_count, position_fetches_before)
        self.assertEqual(len(connector.edit_batches), 1)
        self.assertEqual(len(connector.edit_batches[0]), 1)
        edit = connector.edit_batches[0][0]
        self.assertEqual(edit.order.side, "buy")
        self.assertTrue(edit.reduce_only)
        self.assertTrue(math.isclose(edit.size, 0.1))
        self.assertEqual(len(connector.submissions), 2)
        self.assertEqual(len(connector.place_attempts), 2)
        self.assertEqual({order.side for order in connector.orders}, {"buy"})

    def test_fill_poll_cancels_open_orders_for_filled_symbol(self) -> None:
        connector = FakeConnector()
        script = MarketMakingScript(connector)
        script.db.create_all()
        now = utc_ms()
        connector.orders = [
            OpenOrder("fake", "BTC", connector.symbol_for_market("BTC"), "1", "0x1", "buy", 99.0, 0.1, now, {}),
            OpenOrder("fake", "BTC", connector.symbol_for_market("BTC"), "2", "0x2", "sell", 101.0, 0.1, now, {}),
        ]
        connector.fills = [
            TradeFill(
                exchange="fake",
                market="BTC",
                symbol=connector.symbol_for_market("BTC"),
                exchange_order_id="1",
                client_order_id="0x1",
                trade_id="fill-1",
                side="buy",
                price=100.0,
                size=0.1,
                fee=0.0,
                fee_currency="USDC",
                timestamp_ms=now,
                raw={},
            )
        ]

        script.poll_fills_if_due()

        self.assertEqual(connector.cancel_count, 2)
        self.assertEqual(connector.orders, [])

    def test_fill_poll_can_be_disabled_to_avoid_rest_calls(self) -> None:
        settings.FETCH_FILLS_S = 0.0
        connector = FakeConnector()
        script = MarketMakingScript(connector)
        script.db.create_all()

        script.poll_fills_if_due()

        self.assertEqual(connector.fill_fetch_count, 0)

    def test_cancel_on_fill_guard_blocks_bulk_edit_race(self) -> None:
        settings.BULK_EDIT_INTERVAL_S = 0.0
        settings.USE_EV_SPREAD_PRICING = False
        settings.USE_LOB_VWAP_FAIR_PRICE = False
        connector = FakeConnector()
        script = MarketMakingScript(connector)
        script.db.create_all()
        now = utc_ms()
        connector.orders = [
            OpenOrder("fake", "BTC", connector.symbol_for_market("BTC"), "1", "0x1", "buy", 99.0, 0.1, now, {}),
            OpenOrder("fake", "BTC", connector.symbol_for_market("BTC"), "2", "0x2", "sell", 101.0, 0.1, now, {}),
        ]
        quote = script.model.quote("BTC", 99.99, 100.01, 0.0, symbol=connector.symbol_for_market("BTC"))
        script.mark_cancel_on_fill_guard("BTC", "test_fill_race")

        script.sync_orders(
            "BTC",
            [
                DesiredOrder(0, "buy", 98.0, 0.1, False),
                DesiredOrder(0, "sell", 102.0, 0.1, False),
            ],
            quote,
        )

        self.assertEqual(connector.edit_batches, [])
        self.assertEqual({order.exchange_order_id for order in connector.orders}, {"1", "2"})

    def test_post_only_rejection_refreshes_state_and_stops_more_placements(self) -> None:
        settings.FORCE_REST_REFRESH_AFTER_ORDER_REJECT = True
        connector = FakeConnector()
        script = MarketMakingScript(connector)
        script.db.create_all()
        script.positions["BTC"] = (0.0, {})
        connector.reject_next_order = True

        script.run_coin("BTC")

        self.assertEqual(len(connector.place_attempts), 1)
        self.assertEqual(len(connector.submissions), 0)
        self.assertGreaterEqual(connector.force_open_order_fetch_count, 1)
        self.assertGreaterEqual(connector.position_fetch_count, 1)

    def test_post_only_rejection_can_skip_forced_rest_refresh_in_low_api_mode(self) -> None:
        settings.FORCE_REST_REFRESH_AFTER_ORDER_REJECT = False
        connector = FakeConnector()
        script = MarketMakingScript(connector)
        script.db.create_all()
        script.positions["BTC"] = (0.0, {})
        connector.reject_next_order = True

        script.run_coin("BTC")

        self.assertEqual(len(connector.place_attempts), 1)
        self.assertEqual(len(connector.submissions), 0)
        self.assertEqual(connector.force_open_order_fetch_count, 0)

    def test_max_leverage_is_set_and_margin_target_is_stored(self) -> None:
        connector = FakeConnector()
        script = MarketMakingScript(connector)
        script.db.create_all()
        market = settings.MARKETS[0].upper()

        script.sync_max_leverage(force=True)

        self.assertEqual(connector.leverage_updates, [{"market": market, "leverage": 3, "is_cross": True}])
        self.assertAlmostEqual(script.opening_margin_target_usd(market), 10.0 / 3.0)
        table = script.db.table("market_leverage")
        with script.db.engines["primary"].begin() as conn:
            row = conn.execute(select(table).where(table.c.coin == market)).mappings().one()
        self.assertEqual(row["configured_leverage"], 3)
        self.assertEqual(row["margin_mode"], "cross")
        self.assertAlmostEqual(row["target_order_exposure_usd"], 10.0)
        self.assertAlmostEqual(row["target_order_margin_usd"], 10.0 / 3.0)

    def test_leverage_sync_is_deferred_when_action_rate_limit_is_exhausted(self) -> None:
        connector = FakeConnector()
        connector.rate_limit_raw = {"nRequestsUsed": 37362, "nRequestsCap": 37323, "nRequestsSurplus": 0}
        script = MarketMakingScript(connector)
        script.db.create_all()
        market = settings.MARKETS[0].upper()

        script.sync_max_leverage(force=True)

        self.assertEqual(connector.leverage_updates, [])
        self.assertAlmostEqual(script.opening_margin_target_usd(market), 10.0)
        table = script.db.table("market_leverage")
        with script.db.engines["primary"].begin() as conn:
            row = conn.execute(select(table).where(table.c.coin == market)).mappings().one()
        self.assertEqual(row["configured_leverage"], 1)
        self.assertEqual(row["margin_mode"], "rate_limit_pending")
        raw = json.loads(row["raw_json"])
        self.assertEqual(raw["rate_limit_summary"]["remaining_requests"], -39.0)

    def test_cross_leverage_rejection_falls_back_to_isolated_margin(self) -> None:
        connector = FakeConnector()
        connector.reject_cross_leverage = True
        script = MarketMakingScript(connector)
        script.db.create_all()
        market = settings.MARKETS[0].upper()

        script.sync_max_leverage(force=True)

        self.assertEqual(
            connector.leverage_updates,
            [
                {"market": market, "leverage": 3, "is_cross": True},
                {"market": market, "leverage": 3, "is_cross": False},
            ],
        )
        table = script.db.table("market_leverage")
        with script.db.engines["primary"].begin() as conn:
            row = conn.execute(select(table).where(table.c.coin == market)).mappings().one()
        self.assertEqual(row["configured_leverage"], 3)
        self.assertEqual(row["margin_mode"], "isolated")
        raw = json.loads(row["raw_json"])
        self.assertIn("Cross margin is not allowed", raw["cross_rejected"])
        self.assertFalse(raw["isolated_response"]["is_cross"])

    def test_isolated_fallback_rate_limit_uses_conservative_pending_leverage(self) -> None:
        connector = FakeConnector()
        connector.reject_cross_leverage = True
        connector.reject_isolated_leverage_with_rate_limit = True
        script = MarketMakingScript(connector)
        script.db.create_all()
        market = settings.MARKETS[0].upper()

        script.sync_max_leverage(force=True)

        self.assertEqual(
            connector.leverage_updates,
            [
                {"market": market, "leverage": 3, "is_cross": True},
                {"market": market, "leverage": 3, "is_cross": False},
            ],
        )
        self.assertAlmostEqual(script.opening_margin_target_usd(market), 10.0)
        table = script.db.table("market_leverage")
        with script.db.engines["primary"].begin() as conn:
            row = conn.execute(select(table).where(table.c.coin == market)).mappings().one()
        self.assertEqual(row["configured_leverage"], 1)
        self.assertEqual(row["margin_mode"], "isolated_pending")
        self.assertAlmostEqual(row["target_order_margin_usd"], 10.0)
        raw = json.loads(row["raw_json"])
        self.assertIn("Cross margin is not allowed", raw["cross_rejected"])
        self.assertIn("Too many cumulative requests", raw["isolated_sync_deferred"])

    def test_ev_pricing_uses_fake_public_trades_and_records_diagnostics(self) -> None:
        settings.USE_EV_SPREAD_PRICING = True
        settings.USE_LOB_VWAP_FAIR_PRICE = False
        settings.EV_MIN_TRADE_SAMPLES = 1
        settings.EV_TRADE_LOOKBACK_S = 3600.0
        settings.EV_MIN_HALF_SPREAD_BPS = 1.0
        settings.EV_MAX_HALF_SPREAD_BPS = 20.0
        settings.EV_HALF_SPREAD_STEP_BPS = 1.0
        settings.EV_MAX_FILLS_PER_HOUR = 200.0
        settings.EV_USE_CURRENT_BOOK_DEPTH = True
        settings.MAKER_FEE_BPS_PER_SIDE = 1.5
        settings.MIN_HALF_SPREAD_BPS = 1.0
        settings.MAX_HALF_SPREAD_BPS = 50.0
        connector = FakeConnector()
        now = utc_ms()
        connector.market_trades = [
            MarketTrade("fake", "BTC", "t1", "buy", 100.20, 300.0, now - 3_000_000, {}),
            MarketTrade("fake", "BTC", "t2", "sell", 99.70, 300.0, now - 2_000_000, {}),
            MarketTrade("fake", "BTC", "t3", "buy", 100.01, 300.0, now - 1_000_000, {}),
        ]
        script = MarketMakingScript(connector)
        script.db.create_all()
        script.positions["BTC"] = (0.0, {})

        script.run_coin("BTC")

        diagnostics = script.market_diagnostics("BTC")
        self.assertTrue(diagnostics["ev_applied"])
        self.assertEqual(diagnostics["spread_source"], "ev")
        self.assertEqual(diagnostics["ev_source"], "live_current_book")
        self.assertEqual(diagnostics["ev_depth_mode"], "current_book")
        self.assertFalse(diagnostics["ev_uses_historical_book_depth"])
        self.assertGreaterEqual(diagnostics["ev_sweep_count"], 2)
        self.assertEqual(diagnostics["ev_choice"]["half_spread_bps"], 20.0)
        self.assertEqual(len(diagnostics["ev_curve"]), 20)
        self.assertAlmostEqual(diagnostics["half_spread_bps"], 20.0)

    def test_five_dollar_short_closes_reduce_only(self) -> None:
        orders = self.run_scenario(-0.05)
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0]["side"], "buy")
        self.assertTrue(orders[0]["reduce_only"])
        self.assertTrue(math.isclose(orders[0]["size"], 0.05))
        self.assertLess(orders[0]["notional"], 10.0)


if __name__ == "__main__":
    unittest.main()
