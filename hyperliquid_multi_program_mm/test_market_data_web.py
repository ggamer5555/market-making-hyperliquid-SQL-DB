from __future__ import annotations

import json
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path

import market_data_web


class MarketDataWebTest(unittest.TestCase):
    def test_dashboard_exposes_interactive_keys_hover_and_subplot_auto_scale(self) -> None:
        html = (Path(__file__).resolve().parent / "web" / "index.html").read_text(encoding="utf-8")
        self.assertIn('data-series="mid"', html)
        self.assertNotIn('data-series="accountBalance"', html)
        self.assertNotIn('data-series="position"', html)
        self.assertNotIn('data-series="buyDelta"', html)
        self.assertNotIn('data-series="sellDelta"', html)
        self.assertNotIn('data-series="bidLiquidity"', html)
        self.assertNotIn('data-series="lobPercentileSpread"', html)
        self.assertNotIn('data-series="evBestSpread"', html)
        self.assertNotIn('data-series="evCurve"', html)
        self.assertNotIn('data-series="inventory"', html)
        self.assertNotIn('data-series="bookVwap"', html)
        self.assertNotIn('data-series="vwap2"', html)
        self.assertNotIn('data-series="vwap10"', html)
        self.assertNotIn('data-series="vwap60"', html)
        self.assertNotIn("Book VWAP", html)
        self.assertNotIn("VWAP 2s", html)
        self.assertNotIn("VWAP 10s", html)
        self.assertNotIn("VWAP 1m", html)
        self.assertIn('<div id="tooltip"></div>', html)
        self.assertIn("function updateTooltip(event)", html)
        self.assertIn("| auto scale", html)
        self.assertIn("function numericBounds(values", html)
        self.assertNotIn("Math.min(...prices)", html)
        self.assertNotIn("Math.max(...prices)", html)
        self.assertNotIn("Math.min(...values)", html)
        self.assertNotIn("Math.max(...values)", html)
        self.assertIn('id="mainDepthOpacity"', html)
        self.assertIn('id="lobProfileMode"', html)
        self.assertIn('id="profileOpacity"', html)
        self.assertNotIn('id="pocOpacity"', html)
        self.assertIn('id="profilePercentiles"', html)
        self.assertIn('const LOB_PERCENTILES = ["0.1","0.5","1","2","5","10","25"]', html)
        self.assertIn("function drawLobProfile(", html)
        self.assertNotIn("function drawPocProfile(", html)
        self.assertIn("function percentileSpreadBps(", html)
        self.assertIn("function drawEvChoiceSeries(", html)
        self.assertIn('id="evAskSurface"', html)
        self.assertIn('id="evBidSurface"', html)
        self.assertIn('id="accountChart"', html)
        self.assertIn('id="accountOhlcMinutes"', html)
        self.assertIn("function drawEvAskSurface()", html)
        self.assertIn("function drawEvBidSurface()", html)
        self.assertIn("function drawAccountChart()", html)
        self.assertIn("function drawEvInfoBox(", html)
        self.assertIn("function nearestEvSurfacePoint(", html)
        self.assertIn("/api/fills", html)
        self.assertIn("/api/ev-surface", html)
        self.assertIn("account_available_usd", html)
        self.assertIn("accountFills", html)
        self.assertNotIn('label:"best EV half spread bps"', html)
        self.assertIn("EV depth mode", html)
        self.assertNotIn('label:"LOB percentile spread bps: ask percentile - bid percentile"', html)
        self.assertNotIn("EV curve latest: half spread bps -> EV/hour", html)
        self.assertIn("ctx.fillRect(isBid?center-bar:center,band.top,bar,band.height)", html)

    def test_snapshot_api_samples_across_entire_selected_window(self) -> None:
        original_path = market_data_web.DB_PATH
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            with closing(sqlite3.connect(db_path)) as conn:
                conn.execute(
                    """
                    CREATE TABLE market_snapshots (
                        snapshot_id INTEGER PRIMARY KEY,
                        captured_at_ms INTEGER NOT NULL,
                        coin TEXT NOT NULL,
                        bids_json TEXT NOT NULL,
                        asks_json TEXT NOT NULL,
                        my_bid_prices_json TEXT NOT NULL,
                        my_ask_prices_json TEXT NOT NULL,
                        my_orders_json TEXT NOT NULL,
                        desired_bid_prices_json TEXT NOT NULL,
                        desired_ask_prices_json TEXT NOT NULL,
                        diagnostics_json TEXT NOT NULL,
                        lob_bid_percentiles_json TEXT NOT NULL,
                        lob_ask_percentiles_json TEXT NOT NULL
                    )
                    """
                )
                now_ms = int(time.time() * 1000)
                rows = [
                    (
                        index,
                        now_ms - (119 - index) * 1000,
                        "BTC",
                        "[]",
                        "[]",
                        "[]",
                        "[]",
                        "[]",
                        "[]",
                        "[]",
                        "{}",
                        "{}",
                        "{}",
                    )
                    for index in range(120)
                ]
                conn.executemany(
                    "INSERT INTO market_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    rows,
                )
                conn.commit()
            market_data_web.DB_PATH = db_path
            try:
                payload = market_data_web.api_snapshots({"coin": ["BTC"], "minutes": ["2"], "limit": ["10"]})
            finally:
                market_data_web.DB_PATH = original_path

        timestamps = [snapshot["captured_at_ms"] for snapshot in payload["snapshots"]]
        self.assertEqual(payload["available_count"], 120)
        self.assertLessEqual(payload["count"], 10)
        self.assertTrue(payload["sampled"])
        self.assertLessEqual(timestamps[0], int(time.time() * 1000) - 115_000)
        self.assertGreaterEqual(timestamps[-1], int(time.time() * 1000) - 2_000)

    def test_trade_api_samples_across_entire_selected_window(self) -> None:
        original_path = market_data_web.DB_PATH
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            with closing(sqlite3.connect(db_path)) as conn:
                conn.execute(
                    """
                    CREATE TABLE market_trades (
                        trade_key TEXT PRIMARY KEY,
                        coin TEXT NOT NULL,
                        trade_id TEXT,
                        side TEXT NOT NULL,
                        price REAL NOT NULL,
                        size REAL NOT NULL,
                        timestamp_ms INTEGER NOT NULL
                    )
                    """
                )
                now_ms = int(time.time() * 1000)
                rows = [
                    (
                        f"trade-{index}",
                        "BTC",
                        str(index),
                        "buy",
                        100.0 + index,
                        1.0,
                        now_ms - (119 - index) * 1000,
                    )
                    for index in range(120)
                ]
                conn.executemany(
                    "INSERT INTO market_trades VALUES (?, ?, ?, ?, ?, ?, ?)",
                    rows,
                )
                conn.commit()
            market_data_web.DB_PATH = db_path
            try:
                payload = market_data_web.api_trades({"coin": ["BTC"], "minutes": ["2"], "limit": ["10"]})
            finally:
                market_data_web.DB_PATH = original_path

        timestamps = [trade["timestamp_ms"] for trade in payload["trades"]]
        self.assertEqual(payload["available_count"], 120)
        self.assertLessEqual(len(payload["trades"]), 10)
        self.assertTrue(payload["sampled"])
        self.assertLessEqual(timestamps[0], int(time.time() * 1000) - 115_000)
        self.assertGreaterEqual(timestamps[-1], int(time.time() * 1000) - 2_000)

    def test_fills_api_samples_bot_database_fills(self) -> None:
        original_path = market_data_web.BOT_DB_PATH
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "local_redundant.sqlite"
            with closing(sqlite3.connect(db_path)) as conn:
                conn.execute(
                    """
                    CREATE TABLE fills (
                        fill_id TEXT PRIMARY KEY,
                        client_order_id TEXT,
                        exchange_order_id TEXT,
                        exchange_trade_id TEXT,
                        coin TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        price REAL NOT NULL,
                        size REAL NOT NULL,
                        notional REAL NOT NULL,
                        fee REAL,
                        fee_currency TEXT,
                        timestamp_ms INTEGER
                    )
                    """
                )
                now_ms = int(time.time() * 1000)
                rows = [
                    (
                        f"fill-{index}",
                        f"cloid-{index}",
                        f"oid-{index}",
                        f"trade-{index}",
                        "BTC",
                        "BTC/USDC:USDC",
                        "buy" if index % 2 else "sell",
                        100.0 + index,
                        0.1,
                        10.0 + index,
                        0.01,
                        "USDC",
                        now_ms - (119 - index) * 1000,
                    )
                    for index in range(120)
                ]
                conn.executemany("INSERT INTO fills VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
                conn.commit()
            market_data_web.BOT_DB_PATH = db_path
            try:
                payload = market_data_web.api_fills({"coin": ["BTC"], "minutes": ["2"], "limit": ["10"]})
            finally:
                market_data_web.BOT_DB_PATH = original_path

        timestamps = [fill["timestamp_ms"] for fill in payload["fills"]]
        self.assertEqual(payload["available_count"], 120)
        self.assertLessEqual(len(payload["fills"]), 10)
        self.assertTrue(payload["sampled"])
        self.assertLessEqual(timestamps[0], int(time.time() * 1000) - 115_000)
        self.assertGreaterEqual(timestamps[-1], int(time.time() * 1000) - 2_000)

    def test_ev_surface_api_returns_grid_and_latest_choice(self) -> None:
        original_path = market_data_web.DB_PATH
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            with closing(sqlite3.connect(db_path)) as conn:
                conn.execute(
                    """
                    CREATE TABLE market_snapshots (
                        snapshot_id INTEGER PRIMARY KEY,
                        captured_at_ms INTEGER NOT NULL,
                        coin TEXT NOT NULL,
                        diagnostics_json TEXT NOT NULL
                    )
                    """
                )
                now_ms = int(time.time() * 1000)
                diagnostics = {
                    "spread_source": "ev",
                    "ev_applied": True,
                    "ev_choice": {
                        "half_spread_bps": 7.0,
                        "estimated_fills_per_hour": 12.0,
                        "ev_per_hour": 0.01,
                    },
                }
                conn.execute(
                    "INSERT INTO market_snapshots VALUES (?, ?, ?, ?)",
                    (1, now_ms, "BTC", json.dumps(diagnostics)),
                )
                conn.commit()
            market_data_web.DB_PATH = db_path
            try:
                payload = market_data_web.api_ev_surface({"coin": ["BTC"], "minutes": ["2"]})
            finally:
                market_data_web.DB_PATH = original_path

        self.assertGreater(len(payload["rows"]), 0)
        self.assertEqual(payload["choice"]["half_spread_bps"], 7.0)
        self.assertEqual(payload["choice_source"], "ev")
        self.assertTrue(payload["ev_applied"])

    def test_ev_surface_api_prefers_ev_curve_rows(self) -> None:
        original_path = market_data_web.DB_PATH
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            with closing(sqlite3.connect(db_path)) as conn:
                conn.execute(
                    """
                    CREATE TABLE market_snapshots (
                        snapshot_id INTEGER PRIMARY KEY,
                        captured_at_ms INTEGER NOT NULL,
                        coin TEXT NOT NULL,
                        diagnostics_json TEXT NOT NULL
                    )
                    """
                )
                now_ms = int(time.time() * 1000)
                diagnostics = {
                    "spread_source": "ev",
                    "ev_applied": True,
                    "ev_choice": {
                        "half_spread_bps": 7.0,
                        "estimated_fills_per_hour": 12.0,
                        "ev_per_hour": 0.01,
                    },
                    "ev_curve": [
                        {"half_spread_bps": 1.0, "ev_per_hour": -0.002},
                        {"half_spread_bps": 5.0, "ev_per_hour": 0.012},
                        {"half_spread_bps": 10.0, "ev_per_hour": 0.021},
                    ],
                }
                conn.execute(
                    "INSERT INTO market_snapshots VALUES (?, ?, ?, ?)",
                    (1, now_ms, "BTC", json.dumps(diagnostics)),
                )
                conn.commit()
            market_data_web.DB_PATH = db_path
            try:
                payload = market_data_web.api_ev_surface({"coin": ["BTC"], "minutes": ["2"]})
            finally:
                market_data_web.DB_PATH = original_path

        self.assertEqual(payload["rows"], diagnostics["ev_curve"])
        self.assertEqual(payload["choice"]["half_spread_bps"], 7.0)
        self.assertEqual(payload["choice_source"], "ev")
        self.assertTrue(payload["ev_applied"])

    def test_ev_surface_api_returns_side_specific_rows_and_choices(self) -> None:
        original_path = market_data_web.DB_PATH
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "market_data.sqlite"
            with closing(sqlite3.connect(db_path)) as conn:
                conn.execute(
                    """
                    CREATE TABLE market_snapshots (
                        snapshot_id INTEGER PRIMARY KEY,
                        captured_at_ms INTEGER NOT NULL,
                        coin TEXT NOT NULL,
                        diagnostics_json TEXT NOT NULL
                    )
                    """
                )
                now_ms = int(time.time() * 1000)
                diagnostics = {
                    "spread_source": "ev",
                    "ev_applied": True,
                    "ev_choice": {
                        "half_spread_bps": 7.0,
                        "estimated_fills_per_hour": 12.0,
                        "ev_per_hour": 0.01,
                    },
                    "ev_curve": [
                        {"half_spread_bps": 5.0, "ev_per_hour": 0.004},
                    ],
                    "ev_curve_ask": [
                        {"half_spread_bps": 4.0, "ev_per_hour": 0.002},
                    ],
                    "ev_curve_bid": [
                        {"half_spread_bps": 13.0, "ev_per_hour": 0.003},
                    ],
                    "ev_choice_ask": {
                        "half_spread_bps": 4.0,
                        "estimated_fills_per_hour": 8.0,
                        "ev_per_hour": 0.002,
                    },
                    "ev_choice_bid": {
                        "half_spread_bps": 13.0,
                        "estimated_fills_per_hour": 9.0,
                        "ev_per_hour": 0.003,
                    },
                }
                conn.execute(
                    "INSERT INTO market_snapshots VALUES (?, ?, ?, ?)",
                    (1, now_ms, "BTC", json.dumps(diagnostics)),
                )
                conn.commit()
            market_data_web.DB_PATH = db_path
            try:
                payload = market_data_web.api_ev_surface({"coin": ["BTC"], "minutes": ["2"]})
            finally:
                market_data_web.DB_PATH = original_path

        self.assertEqual(payload["rows_ask"], diagnostics["ev_curve_ask"])
        self.assertEqual(payload["rows_bid"], diagnostics["ev_curve_bid"])
        self.assertEqual(payload["choice_ask"]["half_spread_bps"], 4.0)
        self.assertEqual(payload["choice_bid"]["half_spread_bps"], 13.0)
        self.assertEqual(payload["choice_source"], "ev")
        self.assertTrue(payload["ev_applied"])


if __name__ == "__main__":
    unittest.main()
