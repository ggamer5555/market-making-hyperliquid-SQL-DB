from __future__ import annotations

import json
import logging
import threading
import unittest

from connectors.base import OpenOrder, OrderEdit
from connectors.hyperliquid_connector import HyperliquidConnector
from scripts.orderbook import BookResolution


class FakeSdkExchange:
    def __init__(self) -> None:
        self.requests = []
        self.leverage_updates = []
        self.order_requests = []

    def order(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        self.order_requests.append((args, kwargs))
        return {"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 789}}]}}}

    def bulk_modify_orders_new(self, requests):  # type: ignore[no-untyped-def]
        self.requests.append(requests)
        return {"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 123}}]}}}

    def update_leverage(self, leverage: int, market: str, is_cross: bool = True):  # type: ignore[no-untyped-def]
        self.leverage_updates.append((leverage, market, is_cross))
        return {"status": "ok"}


class FakeInfo:
    def __init__(self) -> None:
        self.open_order_calls = 0
        self.user_state_calls = 0
        self.l2_snapshot_calls = 0
        self.post_requests = []
        self.user_fills_by_time_requests = []

    def open_orders(self, address: str, dex: str = ""):  # type: ignore[no-untyped-def]
        self.open_order_calls += 1
        if dex == "xyz":
            return [{"coin": "xyz:AMD", "oid": 456, "side": "A", "limitPx": "531.2", "sz": "0.1"}]
        return [{"coin": "NIL", "oid": 123, "side": "B", "limitPx": "0.0649", "sz": "200"}]

    def user_state(self, address: str, dex: str = ""):  # type: ignore[no-untyped-def]
        self.user_state_calls += 1
        if dex == "xyz":
            return {"assetPositions": [{"position": {"coin": "xyz:AMD", "szi": "-0.1"}}]}
        return {"assetPositions": [{"position": {"coin": "NIL", "szi": "12.5"}}]}

    def l2_snapshot(self, market: str):  # type: ignore[no-untyped-def]
        self.l2_snapshot_calls += 1
        return {
            "coin": market,
            "levels": [
                [{"px": "0.0649", "sz": "200", "n": 1}],
                [{"px": "0.0651", "sz": "200", "n": 1}],
            ],
        }

    def post(self, path: str, body):  # type: ignore[no-untyped-def]
        self.post_requests.append((path, body))
        if body.get("type") == "l2Book":
            return {
                "coin": body.get("coin"),
                "levels": [
                    [{"px": "531.0", "sz": "0.1", "n": 1}],
                    [{"px": "531.2", "sz": "0.1", "n": 1}],
                ],
            }
        if body.get("type") == "recentTrades":
            return [
                {"side": "B", "px": "0.065", "sz": "10", "time": 1000, "tid": 1},
                {"coin": body.get("coin"), "side": "A", "px": "0.064", "sz": "20", "time": 2000, "tid": 2},
            ]
        return []

    def user_fills_by_time(self, address: str, start_time: int, end_time=None, aggregate_by_time=False):  # type: ignore[no-untyped-def]
        self.user_fills_by_time_requests.append((address, start_time, end_time, aggregate_by_time))
        return [
            {
                "coin": "NIL",
                "side": "B",
                "px": "0.065",
                "sz": "10",
                "time": 1234,
                "tid": 99,
                "oid": 123,
                "fee": "0.001",
                "feeToken": "USDC",
            }
        ]

    def user_rate_limit(self, address: str):  # type: ignore[no-untyped-def]
        return {"user": address, "nRequestsUsed": 37359, "nRequestsCap": 37322}

    def perp_dexs(self):  # type: ignore[no-untyped-def]
        return [None, {"name": "xyz"}]

    def meta(self, dex: str = ""):  # type: ignore[no-untyped-def]
        if dex == "xyz":
            return {"universe": [{"name": "xyz:AMD", "szDecimals": 4, "maxLeverage": 10}]}
        return {"universe": [{"name": "NIL", "szDecimals": 2, "maxLeverage": 3}, {"name": "BTC", "szDecimals": 5, "maxLeverage": 40}]}


class FakeWebSocket:
    def __init__(self) -> None:
        self.messages = []

    def send(self, message: str) -> None:
        self.messages.append(json.loads(message))


class HyperliquidConnectorOfflineTest(unittest.TestCase):
    @staticmethod
    def connector() -> HyperliquidConnector:
        connector = HyperliquidConnector.__new__(HyperliquidConnector)
        connector.log = logging.getLogger("test")
        connector.account_address = "0xtest"
        connector.sdk_exchange = FakeSdkExchange()
        connector.info = FakeInfo()
        connector.perp_dex_names = ["", "xyz"]
        connector.market_to_sz_decimals = {"NIL": 2, "BTC": 5, "xyz:AMD": 4}
        connector.market_to_max_leverage = {"NIL": 3, "BTC": 40, "xyz:AMD": 10}
        connector.market_aliases = {"NIL": "NIL", "BTC": "BTC", "xyz:AMD": "xyz:AMD"}
        connector._open_orders_cache = []
        connector._open_orders_last_fetch = 0.0
        connector._open_orders_lock = threading.RLock()
        connector._order_book_cache = {}
        connector._order_book_exchange_time_ms = {}
        connector._order_book_lock = threading.RLock()
        connector._market_trades_cache = {}
        connector._market_trades_seen = {}
        connector._market_trades_lock = threading.RLock()
        connector._exchange_action_lock = threading.Lock()
        return connector

    def test_settings_style_markets_normalize_to_hyperliquid_coin_names(self) -> None:
        connector = self.connector()

        self.assertEqual(connector.normalize_market("BTC-USDC"), "BTC")
        self.assertEqual(connector.normalize_market("BTC/USDC:USDC"), "BTC")
        self.assertEqual(connector.normalize_market("xyz:AMD-USDC"), "xyz:AMD")
        self.assertEqual(connector.normalize_market("XYZ:AMD-USDC"), "xyz:AMD")
        self.assertEqual(connector.symbol_for_market("xyz:AMD-USDC"), "xyz:AMD/USDC:USDC")

    def test_bulk_edit_builds_one_sdk_batch_modify_request(self) -> None:
        connector = self.connector()
        order = OpenOrder(
            exchange="hyperliquid",
            market="NIL",
            symbol="NIL/USDC:USDC",
            exchange_order_id="123",
            client_order_id="0x1234567890abcdef1234567890abcdef",
            side="buy",
            price=0.065,
            size=200.0,
            timestamp_ms=None,
            raw={},
        )

        results = connector.bulk_edit_orders([OrderEdit(order=order, price=0.0649, size=200.0)])

        self.assertEqual(len(connector.sdk_exchange.requests), 1)
        self.assertEqual(len(connector.sdk_exchange.requests[0]), 1)
        request = connector.sdk_exchange.requests[0][0]
        self.assertEqual(request["oid"], 123)
        self.assertEqual(request["order"]["coin"], "NIL")
        self.assertTrue(request["order"]["is_buy"])
        self.assertEqual(request["order"]["order_type"], {"limit": {"tif": "Alo"}})
        self.assertEqual(results[0].exchange_order_id, "123")
        self.assertEqual(len(connector._open_orders_cache), 1)

    def test_bulk_edit_caches_replacement_order_id_from_response(self) -> None:
        connector = self.connector()
        connector.sdk_exchange.bulk_modify_orders_new = lambda requests: {
            "status": "ok",
            "response": {"data": {"statuses": [{"resting": {"oid": 456}}, {"resting": {"oid": 457}}]}},
        }
        buy = OpenOrder(
            exchange="hyperliquid",
            market="NIL",
            symbol="NIL/USDC:USDC",
            exchange_order_id="123",
            client_order_id="0x1234567890abcdef1234567890abcdef",
            side="buy",
            price=0.065,
            size=200.0,
            timestamp_ms=None,
            raw={},
        )
        sell = OpenOrder(
            exchange="hyperliquid",
            market="NIL",
            symbol="NIL/USDC:USDC",
            exchange_order_id="124",
            client_order_id="0xabcdef1234567890abcdef1234567890",
            side="sell",
            price=0.0651,
            size=200.0,
            timestamp_ms=None,
            raw={},
        )

        results = connector.bulk_edit_orders(
            [
                OrderEdit(order=buy, price=0.0649, size=200.0),
                OrderEdit(order=sell, price=0.0652, size=200.0),
            ]
        )

        self.assertEqual([result.exchange_order_id for result in results], ["456", "457"])
        self.assertEqual(
            [order.exchange_order_id for order in connector.fetch_cached_open_orders("NIL")],
            ["456", "457"],
        )

    def test_open_order_cache_reconciles_only_when_forced_or_stale(self) -> None:
        connector = self.connector()
        connector.fetch_open_orders("NIL")
        connector.fetch_open_orders("NIL")
        self.assertEqual(connector.info.open_order_calls, 1)
        connector.fetch_open_orders("NIL", force=True)
        self.assertEqual(connector.info.open_order_calls, 2)

    def test_inventory_balances_fetch_account_state_once_for_all_markets(self) -> None:
        connector = self.connector()
        balances = connector.fetch_inventory_balances(["NIL", "BTC"])
        self.assertEqual(connector.info.user_state_calls, 1)
        self.assertEqual(balances["NIL"][0], 12.5)
        self.assertEqual(balances["BTC"][0], 0.0)

    def test_fetch_user_rate_limit_uses_main_account_address(self) -> None:
        connector = self.connector()

        raw = connector.fetch_user_rate_limit()

        self.assertEqual(raw["user"], "0xtest")
        self.assertEqual(raw["nRequestsUsed"], 37359)
        self.assertEqual(raw["nRequestsCap"], 37322)

    def test_set_market_leverage_uses_exchange_reported_maximum(self) -> None:
        connector = self.connector()
        raw = connector.set_market_leverage("NIL", 3, is_cross=True)
        self.assertEqual(raw, {"status": "ok"})
        self.assertEqual(connector.sdk_exchange.leverage_updates, [(3, "NIL", True)])
        with self.assertRaisesRegex(RuntimeError, "invalid leverage"):
            connector.set_market_leverage("NIL", 4)

    def test_set_market_leverage_accepts_ok_string_response_body(self) -> None:
        connector = self.connector()
        connector.sdk_exchange.update_leverage = lambda leverage, market, is_cross=True: {
            "status": "ok",
            "response": "success",
        }

        raw = connector.set_market_leverage("xyz:AMD-USDC", 10, is_cross=True)

        self.assertEqual(raw, {"status": "ok", "response": "success"})
        self.assertEqual(connector.action_response_errors(raw), [])

    def test_top_level_action_error_is_not_treated_as_placed_order(self) -> None:
        raw = {
            "status": "err",
            "response": "Too many cumulative requests sent (37355 > 37321) for cumulative volume traded $27322.08.",
        }

        self.assertEqual(
            HyperliquidConnector.extract_order_status(raw),
            ("error", None, raw["response"]),
        )
        self.assertEqual(HyperliquidConnector.action_response_errors(raw), [raw["response"]])

        connector = self.connector()
        connector.sdk_exchange.order = lambda *args, **kwargs: raw
        with self.assertRaisesRegex(RuntimeError, "Too many cumulative requests"):
            connector.place_limit_order(
                "NIL",
                "buy",
                200.0,
                0.065,
                client_order_id="0x1234567890abcdef1234567890abcdef",
            )
        self.assertEqual(connector.fetch_cached_open_orders("NIL"), [])

    def test_builds_hip3_max_leverage_for_prefixed_market(self) -> None:
        connector = self.connector()
        connector.market_to_max_leverage = connector._build_market_to_max_leverage()
        connector.market_aliases = connector._build_market_aliases()
        self.assertEqual(connector.fetch_market_max_leverage("xyz:AMD"), 10)
        self.assertEqual(connector.fetch_market_max_leverage("xyz:AMD-USDC"), 10)
        self.assertEqual(connector.fetch_market_max_leverage("XYZ:AMD-USDC"), 10)
        with self.assertRaisesRegex(RuntimeError, "missing max leverage metadata"):
            connector.fetch_market_max_leverage("unknown:MARKET")

    def test_hip3_l2_book_uses_raw_info_coin_without_uppercasing_dex(self) -> None:
        connector = self.connector()

        bids, asks = connector.fetch_order_book("xyz:AMD-USDC", depth=1)

        self.assertEqual(connector.info.post_requests[-1], ("/info", {"type": "l2Book", "coin": "xyz:AMD"}))
        self.assertEqual(bids[0].price, 531.0)
        self.assertEqual(asks[0].price, 531.2)

    def test_websocket_l2_cache_avoids_rest_snapshot_on_hot_path(self) -> None:
        connector = self.connector()
        connector._on_streams_message(
            None,
            json.dumps(
                {
                    "channel": "l2Book",
                    "data": {
                        "coin": "NIL",
                        "levels": [
                            [{"px": "0.0648", "sz": "300", "n": 1}],
                            [{"px": "0.0652", "sz": "400", "n": 1}],
                        ],
                    },
                }
            ),
        )
        bids, asks = connector.fetch_order_book("NIL", depth=1)
        self.assertEqual(connector.info.l2_snapshot_calls, 0)
        self.assertEqual(bids[0].price, 0.0648)
        self.assertEqual(asks[0].price, 0.0652)

    def test_websocket_subscribes_to_raw_bbo_trades_and_order_updates(self) -> None:
        connector = self.connector()
        connector._stream_markets = ["NIL"]
        ws = FakeWebSocket()

        connector._on_streams_open(ws)

        subscriptions = [message["subscription"] for message in ws.messages]
        self.assertIn({"type": "l2Book", "coin": "NIL"}, subscriptions)
        self.assertIn({"type": "bbo", "coin": "NIL"}, subscriptions)
        self.assertIn({"type": "trades", "coin": "NIL"}, subscriptions)
        self.assertIn({"type": "orderUpdates", "user": "0xtest"}, subscriptions)

    def test_resolution_stream_subscribes_with_n_sig_figs(self) -> None:
        connector = self.connector()
        connector._stream_markets = ["NIL"]
        ws = FakeWebSocket()

        connector._on_resolution_stream_open(ws, BookResolution(4))

        self.assertEqual(
            ws.messages,
            [{"method": "subscribe", "subscription": {"type": "l2Book", "coin": "NIL", "nSigFigs": 4}}],
        )

    def test_websocket_bbo_patches_cached_top_level_and_wakes_waiter(self) -> None:
        connector = self.connector()
        connector._on_streams_message(
            None,
            json.dumps(
                {
                    "channel": "l2Book",
                    "data": {
                        "coin": "NIL",
                        "levels": [
                            [{"px": "0.0648", "sz": "300", "n": 1}],
                            [{"px": "0.0652", "sz": "400", "n": 1}],
                        ],
                    },
                }
            ),
        )
        previous_sequence = connector.wait_for_market_data_update(0, 0.0)

        connector._on_streams_message(
            None,
            json.dumps(
                {
                    "channel": "bbo",
                    "data": {
                        "coin": "NIL",
                        "time": 123456,
                        "bbo": [
                            {"px": "0.0649", "sz": "200", "n": 1},
                            {"px": "0.0651", "sz": "250", "n": 2},
                        ],
                    },
                }
            ),
        )

        bids, asks = connector.fetch_cached_order_book("NIL", depth=1)
        self.assertEqual(bids[0].price, 0.0649)
        self.assertEqual(asks[0].price, 0.0651)
        self.assertGreater(connector.wait_for_market_data_update(previous_sequence, 0.0), previous_sequence)

    def test_websocket_order_updates_refresh_cached_orders(self) -> None:
        connector = self.connector()
        connector._ensure_stream_state()
        order = {"coin": "NIL", "oid": 123, "side": "B", "limitPx": "0.0649", "sz": "200"}
        previous_sequence = connector._market_data_sequence
        connector._on_streams_message(
            None,
            json.dumps({"channel": "orderUpdates", "data": [{"order": order, "status": "open"}]}),
        )
        self.assertEqual(len(connector.fetch_cached_open_orders("NIL")), 1)
        self.assertGreater(connector.wait_for_market_data_update(previous_sequence, 0.0), previous_sequence)
        previous_sequence = connector._market_data_sequence
        connector._on_streams_message(
            None,
            json.dumps({"channel": "orderUpdates", "data": [{"order": order, "status": "filled"}]}),
        )
        self.assertEqual(connector.fetch_cached_open_orders("NIL"), [])
        self.assertGreater(connector.wait_for_market_data_update(previous_sequence, 0.0), previous_sequence)

    def test_websocket_public_trades_refresh_last_trade_cache(self) -> None:
        connector = self.connector()
        connector._on_streams_message(
            None,
            json.dumps(
                {
                    "channel": "trades",
                    "data": [
                        {"coin": "NIL", "side": "B", "px": "0.065", "sz": "12", "time": 123456, "tid": 987},
                    ],
                }
            ),
        )

        trade = connector.fetch_last_cached_market_trade("NIL")
        self.assertIsNotNone(trade)
        assert trade is not None
        self.assertEqual(trade.trade_id, "987")
        self.assertEqual(trade.side, "buy")
        self.assertEqual(trade.price, 0.065)
        self.assertEqual(trade.size, 12.0)

    def test_recent_market_trades_post_backfills_cache(self) -> None:
        connector = self.connector()

        trades = connector.fetch_recent_market_trades("NIL", limit=2000)

        self.assertEqual(connector.info.post_requests[-1], ("/info", {"type": "recentTrades", "coin": "NIL"}))
        self.assertEqual(len(trades), 2)
        self.assertEqual(trades[0].market, "NIL")
        self.assertEqual(trades[1].side, "sell")
        self.assertEqual(len(connector.fetch_cached_market_trades("NIL")), 2)

    def test_user_fills_by_time_uses_explicit_range_and_aggregate_flag(self) -> None:
        connector = self.connector()

        fills = connector.fetch_recent_fills(1000, 2000, aggregate_by_time=False)

        self.assertEqual(connector.info.user_fills_by_time_requests[-1], ("0xtest", 1000, 2000, False))
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0].market, "NIL")
        self.assertEqual(fills[0].trade_id, "99")

    def test_old_websocket_cancel_does_not_remove_amended_order_with_same_cloid(self) -> None:
        connector = self.connector()
        cloid = "0x1234567890abcdef1234567890abcdef"
        connector._on_streams_message(
            None,
            json.dumps(
                {
                    "channel": "orderUpdates",
                    "data": [{"order": {"coin": "NIL", "oid": 456, "cloid": cloid, "side": "B", "limitPx": "0.0648", "sz": "200"}, "status": "open"}],
                }
            ),
        )
        connector._on_streams_message(
            None,
            json.dumps(
                {
                    "channel": "orderUpdates",
                    "data": [{"order": {"coin": "NIL", "oid": 123, "cloid": cloid, "side": "B", "limitPx": "0.0649", "sz": "200"}, "status": "canceled"}],
                }
            ),
        )

        orders = connector.fetch_cached_open_orders("NIL")
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0].exchange_order_id, "456")


if __name__ == "__main__":
    unittest.main()
