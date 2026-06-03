from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import settings
from common import json_dumps, normalize_market_name
from connectors.base import ExchangeConnector, MarketTrade, OrderBookLevel
from scripts.orderbook import levels_within_mid_band, percentile_price


SCHEMA_VERSION = 3

SNAPSHOT_MIGRATION_COLUMNS = {
    "account_equity_usd": "REAL",
    "account_available_usd": "REAL",
    "account_margin_used_usd": "REAL",
    "account_spot_balance_usd": "REAL",
    "account_open_orders_notional_usd": "REAL",
    "account_position_notional_usd": "REAL",
    "account_spot_balances_json": "TEXT NOT NULL DEFAULT '[]'",
    "account_open_orders_json": "TEXT NOT NULL DEFAULT '[]'",
    "account_positions_json": "TEXT NOT NULL DEFAULT '[]'",
    "book_bid_level_count": "INTEGER NOT NULL DEFAULT 0",
    "book_ask_level_count": "INTEGER NOT NULL DEFAULT 0",
    "book_bid_vwap_price": "REAL",
    "book_ask_vwap_price": "REAL",
    "book_vwap_price": "REAL",
    "book_vwap_2s": "REAL",
    "book_vwap_10s": "REAL",
    "book_vwap_60s": "REAL",
    "book_bid_total_size": "REAL",
    "book_ask_total_size": "REAL",
    "book_bid_total_notional": "REAL",
    "book_ask_total_notional": "REAL",
    "my_quote_spread_bps": "REAL",
    "desired_quote_spread_bps": "REAL",
    "lob_bid_percentiles_json": "TEXT NOT NULL DEFAULT '{}'",
    "lob_ask_percentiles_json": "TEXT NOT NULL DEFAULT '{}'",
}


def utc_iso_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000.0, timezone.utc).isoformat().replace("+00:00", "Z")


def level_dict(level: OrderBookLevel) -> Dict[str, Any]:
    return {
        "price": level.price,
        "size": level.size,
        "order_count": level.order_count,
    }


class MarketDataRecorder:
    """Append websocket-backed market snapshots without delaying quote edits."""

    def __init__(
        self,
        exchange: ExchangeConnector,
        diagnostics_provider: Callable[[str], Dict[str, Any]],
        inventory_provider: Callable[[str], float],
        db_path: Optional[str] = None,
    ) -> None:
        self.log = logging.getLogger("MarketDataRecorder")
        self.exchange = exchange
        self.diagnostics_provider = diagnostics_provider
        self.inventory_provider = inventory_provider
        self.db_path = Path(db_path or settings.MARKET_DATA_DB_PATH)
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.previous_mid: Dict[str, tuple[int, float]] = {}
        self.book_vwap_history: Dict[str, deque[tuple[int, float]]] = defaultdict(deque)
        self.last_trade_flush_ms: Dict[str, int] = {}
        self.last_cleanup = 0.0
        self.last_account_summary_fetch = 0.0
        self.last_account_summary_values: Dict[str, Optional[float]] = {
            "account_equity_usd": None,
            "account_available_usd": None,
            "account_margin_used_usd": None,
            "account_spot_balance_usd": None,
            "account_open_orders_notional_usd": None,
            "account_position_notional_usd": None,
        }

    def market_name(self, market: str) -> str:
        normalizer = getattr(self.exchange, "normalize_market", None)
        if callable(normalizer):
            return str(normalizer(market))
        return normalize_market_name(market)

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS recorder_schema_info (
                    name TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS market_snapshots (
                    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at_ms INTEGER NOT NULL,
                    captured_at TEXT NOT NULL,
                    coin TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    book_exchange_timestamp_ms INTEGER,
                    book_received_at_ms INTEGER,
                    book_age_ms INTEGER,
                    mid_price REAL NOT NULL,
                    best_bid REAL NOT NULL,
                    best_ask REAL NOT NULL,
                    top_spread_bps REAL NOT NULL,
                    last_trade_price REAL,
                    last_trade_size REAL,
                    last_trade_side TEXT,
                    last_trade_timestamp_ms INTEGER,
                    my_bid_prices_json TEXT NOT NULL,
                    my_ask_prices_json TEXT NOT NULL,
                    my_orders_json TEXT NOT NULL,
                    desired_bid_prices_json TEXT NOT NULL,
                    desired_ask_prices_json TEXT NOT NULL,
                    inventory_base REAL NOT NULL,
                    inventory_notional REAL NOT NULL,
                    account_equity_usd REAL,
                    account_available_usd REAL,
                    account_margin_used_usd REAL,
                    volatility REAL,
                    gamma REAL NOT NULL,
                    k REAL NOT NULL,
                    min_quote_spread_bps REAL NOT NULL,
                    fallback_sigma_per_s REAL NOT NULL,
                    reservation_price REAL,
                    q_norm REAL,
                    market_speed_bps_per_s REAL,
                    abs_market_speed_bps_per_s REAL,
                    lob_percentile REAL NOT NULL,
                    lob_within_mid_pct REAL NOT NULL,
                    lob_bid_percentile_price REAL,
                    lob_ask_percentile_price REAL,
                    lob_guard_bid_price REAL,
                    lob_guard_ask_price REAL,
                    exchange_price_step REAL NOT NULL,
                    lob_price_step REAL,
                    exchange_size_step REAL,
                    rounded_buy_mid REAL NOT NULL,
                    rounded_sell_mid REAL NOT NULL,
                    book_bid_level_count INTEGER NOT NULL DEFAULT 0,
                    book_ask_level_count INTEGER NOT NULL DEFAULT 0,
                    book_bid_vwap_price REAL,
                    book_ask_vwap_price REAL,
                    book_vwap_price REAL,
                    book_vwap_2s REAL,
                    book_vwap_10s REAL,
                    book_vwap_60s REAL,
                    book_bid_total_size REAL,
                    book_ask_total_size REAL,
                    book_bid_total_notional REAL,
                    book_ask_total_notional REAL,                    "account_spot_balance_usd REAL",
                    "account_open_orders_notional_usd REAL",
                    "account_position_notional_usd REAL",
                    "account_spot_balances_json TEXT NOT NULL DEFAULT '[]'",
                    "account_open_orders_json TEXT NOT NULL DEFAULT '[]'",
                    "account_positions_json TEXT NOT NULL DEFAULT '[]'",                    my_quote_spread_bps REAL,
                    desired_quote_spread_bps REAL,
                    lob_bid_percentiles_json TEXT NOT NULL DEFAULT '{}',
                    lob_ask_percentiles_json TEXT NOT NULL DEFAULT '{}',
                    bids_json TEXT NOT NULL,
                    asks_json TEXT NOT NULL,
                    diagnostics_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS ix_market_snapshots_coin_time
                    ON market_snapshots (coin, captured_at_ms);

                CREATE TABLE IF NOT EXISTS market_trades (
                    trade_key TEXT PRIMARY KEY,
                    coin TEXT NOT NULL,
                    trade_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL NOT NULL,
                    size REAL NOT NULL,
                    timestamp_ms INTEGER,
                    raw_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS ix_market_trades_coin_time
                    ON market_trades (coin, timestamp_ms);
                """
            )
            conn.execute(
                "INSERT OR REPLACE INTO recorder_schema_info (name, value) VALUES (?, ?)",
                ("schema_version", str(SCHEMA_VERSION)),
            )
            self.add_missing_snapshot_columns(conn)
            conn.commit()
        self.log.info("market-data recorder ready db=%s", self.db_path.resolve())

    @contextmanager
    def connect(self):  # type: ignore[no-untyped-def]
        conn = sqlite3.connect(self.db_path, timeout=5.0)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            yield conn
        finally:
            conn.close()

    def start(self) -> None:
        if not bool(getattr(settings, "MARKET_DATA_RECORDING_ENABLED", False)):
            return
        if self.thread is not None and self.thread.is_alive():
            return
        self.initialize()
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run, name="market_data_recorder", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=3.0)
        self.thread = None

    def _run(self) -> None:
        interval = max(float(settings.MARKET_DATA_RECORD_INTERVAL_S), 0.05)
        next_run = time.monotonic()
        while not self.stop_event.is_set():
            for market in settings.MARKETS:
                try:
                    self.record_once(self.market_name(market))
                except Exception as exc:
                    self.log.warning("snapshot skipped market=%s error=%s", market, exc)
            try:
                self.cleanup_if_due()
            except Exception as exc:
                self.log.warning("market-data cleanup failed error=%s", exc)
            next_run += interval
            self.stop_event.wait(max(0.0, next_run - time.monotonic()))
            if time.monotonic() - next_run > interval:
                next_run = time.monotonic()

    def backfill_recent_market_trades(self, market: str, limit: int = 2000) -> int:
        self.initialize()
        market = self.market_name(market)
        trades = self.exchange.fetch_recent_market_trades(market, limit=limit)
        with self.connect() as conn:
            inserted = self.persist_market_trades(conn, market, trades)
            conn.commit()
        self.log.info(
            "market trade backfill saved market=%s fetched=%s inserted=%s",
            market,
            len(trades),
            inserted,
        )
        return inserted

    def record_once(self, market: str) -> Optional[int]:
        market = self.market_name(market)
        bids, asks = self.exchange.fetch_cached_order_book(market, depth=int(settings.MARKET_DATA_BOOK_LEVEL_LIMIT))
        if not bids or not asks:
            return None
        now_ms = int(time.time() * 1000)
        best_bid = bids[0].price
        best_ask = asks[0].price
        mid = 0.5 * (best_bid + best_ask)
        diagnostics = dict(self.diagnostics_provider(market) or {})
        inventory = float(self.inventory_provider(market))
        open_orders = self.exchange.fetch_cached_open_orders(market)
        my_bids = [order.price for order in open_orders if order.side == "buy"]
        my_asks = [order.price for order in open_orders if order.side == "sell"]
        last_trade = self.exchange.fetch_last_cached_market_trade(market)
        if bool(getattr(settings, "MARKET_DATA_ACCOUNT_REST_ENABLED", False)):
            spot_balances = self.exchange.fetch_spot_balances()
            positions = self.exchange.fetch_positions()
        else:
            spot_balances = []
            positions = []
        signed_speed = self.market_speed_bps_per_s(market, now_ms, mid)
        bid_vwap = self.side_vwap(bids)
        ask_vwap = self.side_vwap(asks)
        book_vwap = self.whole_book_vwap(bids, asks)
        book_vwap_2s, book_vwap_10s, book_vwap_60s = self.rolling_book_vwaps(market, now_ms, book_vwap)
        bid_percentiles = self.lob_percentiles(bids, mid, is_bid=True)
        ask_percentiles = self.lob_percentiles(asks, mid, is_bid=False)
        desired_bids = diagnostics.get("desired_bid_prices", [])
        desired_asks = diagnostics.get("desired_ask_prices", [])
        exchange_price_step = float(self.exchange.price_step(market, mid))
        cache_meta = self.exchange.fetch_order_book_cache_metadata(market)
        account_values = self.account_summary_values()
        row = {
            "captured_at_ms": now_ms,
            "captured_at": utc_iso_from_ms(now_ms),
            "coin": market,
            "symbol": self.exchange.symbol_for_market(market),
            "book_exchange_timestamp_ms": cache_meta.get("exchange_timestamp_ms"),
            "book_received_at_ms": cache_meta.get("received_at_ms"),
            "book_age_ms": cache_meta.get("age_ms"),
            "mid_price": mid,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "top_spread_bps": (best_ask - best_bid) / mid * 10000.0,
            "last_trade_price": last_trade.price if last_trade else None,
            "last_trade_size": last_trade.size if last_trade else None,
            "last_trade_side": last_trade.side if last_trade else None,
            "last_trade_timestamp_ms": last_trade.timestamp_ms if last_trade else None,
            "my_bid_prices_json": json_dumps(my_bids),
            "my_ask_prices_json": json_dumps(my_asks),
            "my_orders_json": json_dumps([asdict(order) for order in open_orders]),
            "desired_bid_prices_json": json_dumps(desired_bids),
            "desired_ask_prices_json": json_dumps(desired_asks),
            "inventory_base": inventory,
            "inventory_notional": inventory * mid,
            "account_equity_usd": account_values.get("account_equity_usd"),
            "account_available_usd": account_values.get("account_available_usd"),
            "account_margin_used_usd": account_values.get("account_margin_used_usd"),
            "account_spot_balance_usd": self.spot_balance_notional_usd(spot_balances, market, mid),
            "account_open_orders_notional_usd": self.open_orders_notional_usd(open_orders),
            "account_position_notional_usd": self.positions_notional_usd(positions, market, mid) if positions else inventory * mid,
            "account_spot_balances_json": json_dumps(spot_balances),
            "account_open_orders_json": json_dumps([asdict(order) for order in open_orders]),
            "account_positions_json": json_dumps(positions),
            "volatility": diagnostics.get("volatility"),
            "gamma": float(diagnostics.get("gamma", settings.GAMMA)),
            "k": float(diagnostics.get("k", settings.K)),
            "min_quote_spread_bps": float(settings.MIN_QUOTE_SPREAD_BPS),
            "fallback_sigma_per_s": float(settings.FALLBACK_SIGMA_PER_S),
            "reservation_price": diagnostics.get("reservation_price"),
            "q_norm": diagnostics.get("q_norm"),
            "market_speed_bps_per_s": signed_speed,
            "abs_market_speed_bps_per_s": abs(signed_speed) if signed_speed is not None else None,
            "lob_percentile": float(settings.LOB_PERCENTILE),
            "lob_within_mid_pct": float(settings.LOB_WITHIN_MID_PCT),
            "lob_bid_percentile_price": diagnostics.get("lob_bid_percentile_price"),
            "lob_ask_percentile_price": diagnostics.get("lob_ask_percentile_price"),
            "lob_guard_bid_price": diagnostics.get("lob_guard_bid_price"),
            "lob_guard_ask_price": diagnostics.get("lob_guard_ask_price"),
            "exchange_price_step": exchange_price_step,
            "lob_price_step": diagnostics.get("lob_price_step"),
            "exchange_size_step": self.exchange_size_step(market),
            "rounded_buy_mid": self.exchange.round_price(market, "buy", mid),
            "rounded_sell_mid": self.exchange.round_price(market, "sell", mid),
            "book_bid_level_count": len(bids),
            "book_ask_level_count": len(asks),
            "book_bid_vwap_price": bid_vwap,
            "book_ask_vwap_price": ask_vwap,
            "book_vwap_price": book_vwap,
            "book_vwap_2s": book_vwap_2s,
            "book_vwap_10s": book_vwap_10s,
            "book_vwap_60s": book_vwap_60s,
            "book_bid_total_size": self.total_size(bids),
            "book_ask_total_size": self.total_size(asks),
            "book_bid_total_notional": self.total_notional(bids),
            "book_ask_total_notional": self.total_notional(asks),
            "my_quote_spread_bps": self.quote_spread_bps(my_bids, my_asks, mid),
            "desired_quote_spread_bps": self.quote_spread_bps(desired_bids, desired_asks, mid),
            "lob_bid_percentiles_json": json_dumps(bid_percentiles),
            "lob_ask_percentiles_json": json_dumps(ask_percentiles),
            "bids_json": json_dumps([level_dict(level) for level in bids]),
            "asks_json": json_dumps([level_dict(level) for level in asks]),
            "diagnostics_json": json_dumps(diagnostics),
        }
        with self.connect() as conn:
            self.flush_market_trades(conn, market)
            columns = ", ".join(row)
            placeholders = ", ".join("?" for _ in row)
            cursor = conn.execute(
                f"INSERT INTO market_snapshots ({columns}) VALUES ({placeholders})",
                tuple(row.values()),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def market_speed_bps_per_s(self, market: str, now_ms: int, mid: float) -> Optional[float]:
        previous = self.previous_mid.get(market)
        self.previous_mid[market] = (now_ms, mid)
        if previous is None:
            return None
        previous_ms, previous_mid = previous
        elapsed_s = (now_ms - previous_ms) / 1000.0
        if elapsed_s <= 0 or previous_mid <= 0:
            return None
        return (mid - previous_mid) / previous_mid * 10000.0 / elapsed_s

    @staticmethod
    def side_vwap(levels: List[OrderBookLevel]) -> Optional[float]:
        total_size = sum(max(level.size, 0.0) for level in levels)
        if total_size <= 0:
            return None
        return sum(level.price * max(level.size, 0.0) for level in levels) / total_size

    @classmethod
    def whole_book_vwap(cls, bids: List[OrderBookLevel], asks: List[OrderBookLevel]) -> Optional[float]:
        return cls.side_vwap(list(bids) + list(asks))

    def rolling_book_vwaps(
        self,
        market: str,
        now_ms: int,
        book_vwap: Optional[float],
    ) -> tuple[Optional[float], Optional[float], Optional[float]]:
        history = self.book_vwap_history[market]
        if book_vwap is not None:
            history.append((now_ms, book_vwap))
        while history and history[0][0] < now_ms - 60_000:
            history.popleft()
        return (
            self.average_since(history, now_ms - 2_000),
            self.average_since(history, now_ms - 10_000),
            self.average_since(history, now_ms - 60_000),
        )

    @staticmethod
    def average_since(history: deque[tuple[int, float]], cutoff_ms: int) -> Optional[float]:
        values = [value for timestamp_ms, value in history if timestamp_ms >= cutoff_ms]
        return sum(values) / len(values) if values else None

    @staticmethod
    def lob_percentiles(levels: List[OrderBookLevel], mid: float, *, is_bid: bool) -> Dict[str, Optional[float]]:
        filtered = levels_within_mid_band(
            levels,
            mid,
            float(settings.LOB_WITHIN_MID_PCT),
            int(settings.MARKET_DATA_BOOK_LEVEL_LIMIT),
        )
        return {
            f"{percentile * 100:g}": percentile_price(filtered, percentile, is_bid=is_bid)
            for percentile in settings.MARKET_DATA_LOB_PERCENTILES
        }

    @staticmethod
    def total_size(levels: List[OrderBookLevel]) -> float:
        return sum(max(level.size, 0.0) for level in levels)

    @staticmethod
    def total_notional(levels: List[OrderBookLevel]) -> float:
        return sum(level.price * max(level.size, 0.0) for level in levels)

    @staticmethod
    def quote_spread_bps(bids: Any, asks: Any, mid: float) -> Optional[float]:
        valid_bids = [float(price) for price in bids if price is not None and float(price) > 0]
        valid_asks = [float(price) for price in asks if price is not None and float(price) > 0]
        if not valid_bids or not valid_asks or mid <= 0:
            return None
        return (min(valid_asks) - max(valid_bids)) / mid * 10000.0

    def account_summary_values(self) -> Dict[str, Optional[float]]:
        if not bool(getattr(settings, "ACCOUNT_EQUITY_RECORDING_ENABLED", False)):
            return dict(self.last_account_summary_values)
        now = time.monotonic()
        refresh_s = max(float(getattr(settings, "ACCOUNT_EQUITY_REFRESH_S", 2.0)), 0.05)
        if now - self.last_account_summary_fetch < refresh_s:
            return dict(self.last_account_summary_values)
        self.last_account_summary_fetch = now
        try:
            summary = self.exchange.fetch_account_summary()
        except Exception as exc:
            self.log.warning("account equity snapshot skipped error=%s", exc)
            return dict(self.last_account_summary_values)

        summary = summary if isinstance(summary, dict) else {}
        margin_summary = summary.get("marginSummary")
        cross_margin_summary = summary.get("crossMarginSummary")
        margin_summary = margin_summary if isinstance(margin_summary, dict) else {}
        cross_margin_summary = cross_margin_summary if isinstance(cross_margin_summary, dict) else {}

        values = {
            "account_equity_usd": self.safe_float(
                margin_summary.get("accountValue")
                or cross_margin_summary.get("accountValue")
                or summary.get("accountValue")
            ),
            "account_available_usd": self.safe_float(
                summary.get("withdrawable")
                or summary.get("available")
                or summary.get("availableBalance")
                or margin_summary.get("available")
                or cross_margin_summary.get("available")
            ),
            "account_margin_used_usd": self.safe_float(
                margin_summary.get("totalMarginUsed")
                or cross_margin_summary.get("totalMarginUsed")
                or summary.get("totalMarginUsed")
            ),
        }
        self.last_account_summary_values = values
        return dict(values)

    @staticmethod
    def safe_float(value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _position_market_name(market: str) -> str:
        return normalize_market_name(market)

    def spot_balance_notional_usd(self, balances: List[Dict[str, Any]], market: str, mid: float) -> Optional[float]:
        if not balances:
            return None
        total = 0.0
        market_base = self.market_name(market)
        usd_like = {"USD","USDC","USDT","USDP","BUSD","DAI"}
        for balance in balances:
            asset = str(balance.get("asset") or balance.get("currency") or balance.get("coin") or "").upper()
            amount = self.safe_float(balance.get("total") or balance.get("free") or balance.get("balance")) or 0.0
            if not amount:
                continue
            if asset == market_base:
                total += abs(amount) * mid
            elif asset in usd_like:
                total += abs(amount)
        return total if total != 0.0 else None

    @staticmethod
    def open_orders_notional_usd(open_orders: List[OpenOrder]) -> Optional[float]:
        if not open_orders:
            return None
        total = 0.0
        for order in open_orders:
            if order.price is None or order.size is None:
                continue
            total += abs(order.price * order.size)
        return total if total != 0.0 else None

    @staticmethod
    def positions_notional_usd(positions: List[Dict[str, Any]], market: str, mid: float) -> Optional[float]:
        if not positions:
            return None
        total = 0.0
        market_base = MarketDataRecorder._position_market_name(market)
        for asset_position in positions:
            position = asset_position.get("position") if isinstance(asset_position, dict) else None
            if not isinstance(position, dict):
                continue
            coin = MarketDataRecorder._position_market_name(str(position.get("coin") or ""))
            if coin != market_base:
                continue
            size = MarketDataRecorder.safe_float(position.get("szi")) or 0.0
            total += abs(size) * mid
        return total if total != 0.0 else None

    @staticmethod
    def add_missing_snapshot_columns(conn: sqlite3.Connection) -> None:
        existing = {row[1] for row in conn.execute("PRAGMA table_info(market_snapshots)")}
        for column, definition in SNAPSHOT_MIGRATION_COLUMNS.items():
            if column not in existing:
                conn.execute(f"ALTER TABLE market_snapshots ADD COLUMN {column} {definition}")

    def exchange_size_step(self, market: str) -> Optional[float]:
        size_step = getattr(self.exchange, "size_step", None)
        return float(size_step(market)) if callable(size_step) else None

    def flush_market_trades(self, conn: sqlite3.Connection, market: str) -> None:
        since_ms = self.last_trade_flush_ms.get(market, 0)
        trades = self.exchange.fetch_cached_market_trades(market, since_ms)
        self.persist_market_trades(conn, market, trades)

    def persist_market_trades(self, conn: sqlite3.Connection, market: str, trades: List[MarketTrade]) -> int:
        inserted = 0
        for trade in trades:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO market_trades
                    (trade_key, coin, trade_id, side, price, size, timestamp_ms, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.market_trade_key(trade),
                    trade.market,
                    trade.trade_id,
                    trade.side,
                    trade.price,
                    trade.size,
                    trade.timestamp_ms,
                    json_dumps(trade.raw),
                ),
            )
            inserted += max(int(cursor.rowcount or 0), 0)
        timestamps = [trade.timestamp_ms for trade in trades if trade.timestamp_ms is not None]
        if timestamps:
            self.last_trade_flush_ms[market] = max(timestamps)
        return inserted

    @staticmethod
    def market_trade_key(trade: MarketTrade) -> str:
        return f"{trade.market}:{trade.timestamp_ms}:{trade.trade_id}"

    def cleanup_if_due(self) -> None:
        retention_hours = float(settings.MARKET_DATA_RETENTION_HOURS)
        if retention_hours <= 0:
            return
        now = time.time()
        if now - self.last_cleanup < float(settings.MARKET_DATA_CLEANUP_INTERVAL_S):
            return
        cutoff_ms = int((now - retention_hours * 3600.0) * 1000)
        with self.connect() as conn:
            conn.execute("DELETE FROM market_snapshots WHERE captured_at_ms < ?", (cutoff_ms,))
            conn.execute("DELETE FROM market_trades WHERE timestamp_ms < ?", (cutoff_ms,))
            conn.commit()
        self.last_cleanup = now
