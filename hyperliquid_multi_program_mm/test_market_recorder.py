from __future__ import annotations

import json
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import settings
from connectors.base import MarketTrade, OpenOrder, OrderBookLevel
from market_recorder import MarketDataRecorder


def level(price: float, size: float) -> OrderBookLevel:
    return OrderBookLevel(price=price, size=size, order_count=1, raw={})


class FakeRecorderExchange:
    name = "fake"

    def __init__(self) -> None:
        self.mid = 100.0
        self.orders = [
            OpenOrder("fake", "BTC", "BTC/USDC:USDC", "1", "0x1", "buy", 99.0, 0.1, None, {}),
            OpenOrder("fake", "BTC", "BTC/USDC:USDC", "2", "0x2", "sell", 101.0, 0.1, None, {}),
        ]
        self.trades = [
            MarketTrade("fake", "BTC", "trade-1", "buy", 100.0, 0.2, 123456, {}),
        ]
        self.account_summary_calls = 0
        self.spot_balance_calls = 0
        self.position_calls = 0

    def fetch_cached_order_book(
        self, market: str, depth: Optional[int] = None
    ) -> Tuple[List[OrderBookLevel], List[OrderBookLevel]]:
        bids = [level(self.mid - 0.01, 2.0), level(self.mid - 0.02, 3.0)]
        asks = [level(self.mid + 0.01, 4.0), level(self.mid + 0.02, 5.0)]
        return bids[:depth], asks[:depth]

    def fetch_cached_open_orders(self, market: str) -> List[OpenOrder]:
        return list(self.orders)

    def fetch_last_cached_market_trade(self, market: str) -> Optional[MarketTrade]:
        return self.trades[-1]

    def fetch_cached_market_trades(self, market: str, since_timestamp_ms: int = 0) -> List[MarketTrade]:
        return [trade for trade in self.trades if trade.timestamp_ms is None or trade.timestamp_ms >= since_timestamp_ms]

    def fetch_recent_market_trades(self, market: str, limit: int = 2000) -> List[MarketTrade]:
        return self.trades[-limit:] if limit > 0 else list(self.trades)

    def fetch_account_summary(self) -> Dict[str, Any]:
        self.account_summary_calls += 1
        return {
            "marginSummary": {"accountValue": "1234.56", "totalMarginUsed": "78.9"},
            "withdrawable": "1111.22",
        }

    def fetch_spot_balances(self) -> List[Dict[str, Any]]:
        self.spot_balance_calls += 1
        return [
            {"asset": "BTC", "total": "0.2"},
            {"asset": "USDC", "total": "1500"},
        ]

    def fetch_positions(self) -> List[Dict[str, Any]]:
        self.position_calls += 1
        return [
            {"position": {"coin": "BTC", "szi": "0.05"}},
        ]

    def fetch_order_book_cache_metadata(self, market: str) -> Dict[str, Any]:
        return {"exchange_timestamp_ms": 123000, "received_at_ms": 123001, "age_ms": 7}

    def symbol_for_market(self, market: str) -> str:
        return f"{market}/USDC:USDC"

    def price_step(self, market: str, price: float) -> float:
        return 0.01

    def size_step(self, market: str) -> float:
        return 0.001

    def round_price(self, market: str, side: str, price: float) -> float:
        return round(price, 2)


class MarketDataRecorderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.old_account_rest_enabled = settings.MARKET_DATA_ACCOUNT_REST_ENABLED
        self.old_equity_recording_enabled = settings.ACCOUNT_EQUITY_RECORDING_ENABLED

    def tearDown(self) -> None:
        settings.MARKET_DATA_ACCOUNT_REST_ENABLED = self.old_account_rest_enabled
        settings.ACCOUNT_EQUITY_RECORDING_ENABLED = self.old_equity_recording_enabled

    def test_record_once_persists_depth_trade_orders_and_model_diagnostics(self) -> None:
        settings.MARKET_DATA_ACCOUNT_REST_ENABLED = True
        settings.ACCOUNT_EQUITY_RECORDING_ENABLED = True
        diagnostics = {
            "desired_bid_prices": [99.0],
            "desired_ask_prices": [101.0],
            "volatility": 0.0002,
            "gamma": 0.05,
            "k": 100.0,
            "reservation_price": 100.1,
            "q_norm": 0.25,
            "lob_bid_percentile_price": 99.98,
            "lob_ask_percentile_price": 100.02,
            "lob_guard_bid_price": 99.99,
            "lob_guard_ask_price": 100.01,
            "lob_price_step": 0.01,
            "ev_applied": True,
            "ev_choice": {
                "half_spread_bps": 5.0,
                "estimated_fills_per_hour": 12.0,
                "ev_per_hour": 0.01,
            },
            "ev_curve": [
                {"half_spread_bps": 1.0, "ev_per_hour": -0.001},
                {"half_spread_bps": 5.0, "ev_per_hour": 0.01},
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "market_data.sqlite")
            exchange = FakeRecorderExchange()
            recorder = MarketDataRecorder(exchange, lambda market: diagnostics, lambda market: 0.1, db_path)
            recorder.initialize()
            recorder.record_once("BTC")
            exchange.mid = 100.1
            time.sleep(0.01)
            recorder.record_once("BTC")

            with closing(sqlite3.connect(db_path)) as conn:
                rows = conn.execute("SELECT * FROM market_snapshots ORDER BY snapshot_id").fetchall()
                columns = [description[0] for description in conn.execute("SELECT * FROM market_snapshots LIMIT 0").description]
                snapshot = dict(zip(columns, rows[-1]))
                trade_count = conn.execute("SELECT COUNT(*) FROM market_trades").fetchone()[0]

        self.assertEqual(len(rows), 2)
        self.assertEqual(trade_count, 1)
        self.assertEqual(json.loads(snapshot["my_bid_prices_json"]), [99.0])
        self.assertEqual(json.loads(snapshot["my_ask_prices_json"]), [101.0])
        self.assertEqual(len(json.loads(snapshot["bids_json"])), 2)
        self.assertEqual(len(json.loads(snapshot["asks_json"])), 2)
        self.assertEqual(snapshot["last_trade_price"], 100.0)
        self.assertEqual(snapshot["inventory_base"], 0.1)
        self.assertEqual(snapshot["account_equity_usd"], 1234.56)
        self.assertEqual(snapshot["account_available_usd"], 1111.22)
        self.assertEqual(snapshot["account_margin_used_usd"], 78.9)
        self.assertEqual(snapshot["account_spot_balance_usd"], 1500.0 + 0.2 * 100.1)
        self.assertEqual(snapshot["account_open_orders_notional_usd"], 99.0 * 0.1 + 101.0 * 0.1)
        self.assertEqual(snapshot["account_position_notional_usd"], 0.05 * 100.1)
        self.assertEqual(json.loads(snapshot["account_spot_balances_json"]), [{"asset": "BTC", "total": "0.2"}, {"asset": "USDC", "total": "1500"}])
        self.assertEqual(json.loads(snapshot["account_open_orders_json"]), [
            {"exchange": "fake", "market": "BTC", "symbol": "BTC/USDC:USDC", "exchange_order_id": "1", "client_order_id": "0x1", "side": "buy", "price": 99.0, "size": 0.1, "timestamp_ms": None, "raw": {}},
            {"exchange": "fake", "market": "BTC", "symbol": "BTC/USDC:USDC", "exchange_order_id": "2", "client_order_id": "0x2", "side": "sell", "price": 101.0, "size": 0.1, "timestamp_ms": None, "raw": {}}
        ])
        self.assertEqual(json.loads(snapshot["account_positions_json"]), [{"position": {"coin": "BTC", "szi": "0.05"}}])

        self.assertEqual(snapshot["volatility"], 0.0002)
        self.assertEqual(snapshot["gamma"], 0.05)
        self.assertEqual(snapshot["k"], 100.0)
        self.assertEqual(snapshot["q_norm"], 0.25)
        self.assertEqual(snapshot["lob_bid_percentile_price"], 99.98)
        self.assertEqual(snapshot["lob_ask_percentile_price"], 100.02)
        self.assertEqual(snapshot["exchange_price_step"], 0.01)
        self.assertEqual(snapshot["exchange_size_step"], 0.001)
        self.assertEqual(snapshot["book_bid_level_count"], 2)
        self.assertEqual(snapshot["book_ask_level_count"], 2)
        self.assertAlmostEqual(snapshot["book_bid_vwap_price"], (100.09 * 2.0 + 100.08 * 3.0) / 5.0)
        self.assertAlmostEqual(snapshot["book_ask_vwap_price"], (100.11 * 4.0 + 100.12 * 5.0) / 9.0)
        self.assertIsNotNone(snapshot["book_vwap_price"])
        self.assertIsNotNone(snapshot["book_vwap_2s"])
        self.assertIsNotNone(snapshot["book_vwap_10s"])
        self.assertIsNotNone(snapshot["book_vwap_60s"])
        self.assertEqual(snapshot["book_bid_total_size"], 5.0)
        self.assertEqual(snapshot["book_ask_total_size"], 9.0)
        self.assertAlmostEqual(snapshot["book_bid_total_notional"], 100.09 * 2.0 + 100.08 * 3.0)
        self.assertAlmostEqual(snapshot["book_ask_total_notional"], 100.11 * 4.0 + 100.12 * 5.0)
        self.assertAlmostEqual(snapshot["my_quote_spread_bps"], (101.0 - 99.0) / 100.1 * 10000.0)
        self.assertAlmostEqual(snapshot["desired_quote_spread_bps"], (101.0 - 99.0) / 100.1 * 10000.0)
        self.assertAlmostEqual(json.loads(snapshot["lob_bid_percentiles_json"])["0.1"], 100.09)
        self.assertAlmostEqual(json.loads(snapshot["lob_ask_percentiles_json"])["0.5"], 100.11)
        self.assertAlmostEqual(json.loads(snapshot["lob_bid_percentiles_json"])["1"], 100.09)
        self.assertAlmostEqual(json.loads(snapshot["lob_ask_percentiles_json"])["25"], 100.11)
        saved_diagnostics = json.loads(snapshot["diagnostics_json"])
        self.assertTrue(saved_diagnostics["ev_applied"])
        self.assertEqual(saved_diagnostics["ev_choice"]["half_spread_bps"], 5.0)
        self.assertEqual(len(saved_diagnostics["ev_curve"]), 2)
        self.assertGreater(snapshot["market_speed_bps_per_s"], 0.0)

    def test_record_once_uses_cached_data_without_account_rest_by_default(self) -> None:
        settings.MARKET_DATA_ACCOUNT_REST_ENABLED = False
        settings.ACCOUNT_EQUITY_RECORDING_ENABLED = False
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "market_data.sqlite")
            exchange = FakeRecorderExchange()
            recorder = MarketDataRecorder(exchange, lambda market: {}, lambda market: 0.1, db_path)
            recorder.initialize()

            recorder.record_once("BTC")

            self.assertEqual(exchange.account_summary_calls, 0)
            self.assertEqual(exchange.spot_balance_calls, 0)
            self.assertEqual(exchange.position_calls, 0)
            with closing(sqlite3.connect(db_path)) as conn:
                conn.row_factory = sqlite3.Row
                snapshot = conn.execute("SELECT * FROM market_snapshots").fetchone()
                self.assertIsNone(snapshot["account_equity_usd"])
                self.assertIsNone(snapshot["account_spot_balance_usd"])
                self.assertAlmostEqual(snapshot["account_position_notional_usd"], 0.1 * 100.0)
                self.assertEqual(json.loads(snapshot["account_spot_balances_json"]), [])
                self.assertEqual(json.loads(snapshot["account_positions_json"]), [])

    def test_backfill_recent_market_trades_saves_and_deduplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "market_data.sqlite")
            exchange = FakeRecorderExchange()
            recorder = MarketDataRecorder(exchange, lambda market: {}, lambda market: 0.0, db_path)

            first_inserted = recorder.backfill_recent_market_trades("BTC", limit=2000)
            second_inserted = recorder.backfill_recent_market_trades("BTC", limit=2000)

            with closing(sqlite3.connect(db_path)) as conn:
                trade_count = conn.execute("SELECT COUNT(*) FROM market_trades").fetchone()[0]

        self.assertEqual(first_inserted, 1)
        self.assertEqual(second_inserted, 0)
        self.assertEqual(trade_count, 1)

    def test_lob_percentiles_ignore_levels_outside_mid_band(self) -> None:
        percentiles = MarketDataRecorder.lob_percentiles(
            [level(99.0, 1.0), level(90.0, 999.0)],
            100.0,
            is_bid=True,
        )

        self.assertEqual(percentiles["25"], 99.0)

    def test_initialize_migrates_existing_snapshot_table(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "old_market_data.sqlite")
            with closing(sqlite3.connect(db_path)) as conn:
                conn.execute(
                    "CREATE TABLE market_snapshots "
                    "(snapshot_id INTEGER PRIMARY KEY, coin TEXT, captured_at_ms INTEGER)"
                )
                conn.commit()
            recorder = MarketDataRecorder(FakeRecorderExchange(), lambda market: {}, lambda market: 0.0, db_path)

            recorder.initialize()

            with closing(sqlite3.connect(db_path)) as conn:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(market_snapshots)")}
            self.assertIn("book_vwap_price", columns)
            self.assertIn("account_equity_usd", columns)
            self.assertIn("account_available_usd", columns)
            self.assertIn("account_margin_used_usd", columns)
            self.assertIn("book_vwap_2s", columns)
            self.assertIn("book_vwap_10s", columns)
            self.assertIn("book_vwap_60s", columns)
            self.assertIn("book_bid_total_size", columns)
            self.assertIn("book_ask_total_size", columns)
            self.assertIn("my_quote_spread_bps", columns)
            self.assertIn("desired_quote_spread_bps", columns)
            self.assertIn("lob_bid_percentiles_json", columns)
            self.assertIn("lob_ask_percentiles_json", columns)


if __name__ == "__main__":
    unittest.main()
