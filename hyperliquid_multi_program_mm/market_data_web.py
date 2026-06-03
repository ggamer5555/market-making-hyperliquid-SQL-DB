from __future__ import annotations

import json
import mimetypes
import re
import sqlite3
import time
from math import ceil
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

import settings
from scripts.ev_spread import build_ev_surface


ROOT = Path(__file__).resolve().parent
WEB_ROOT = ROOT / "web"
DB_PATH = (ROOT / settings.MARKET_DATA_DB_PATH).resolve()
MARKET_PATTERN = re.compile(r"^[A-Z0-9:@._-]{1,32}$")
JSON_COLUMNS = {
    "my_bid_prices_json": "my_bid_prices",
    "my_ask_prices_json": "my_ask_prices",
    "my_orders_json": "my_orders",
    "desired_bid_prices_json": "desired_bid_prices",
    "desired_ask_prices_json": "desired_ask_prices",
    "account_spot_balances_json": "account_spot_balances",
    "account_open_orders_json": "account_open_orders",
    "account_positions_json": "account_positions",
    "bids_json": "bids",
    "asks_json": "asks",
    "diagnostics_json": "diagnostics",
    "lob_bid_percentiles_json": "lob_bid_percentiles",
    "lob_ask_percentiles_json": "lob_ask_percentiles",
}


def sqlite_url_to_path(url: str) -> Path:
    if url.startswith("sqlite:///"):
        raw = url[len("sqlite:///") :]
    elif url.startswith("sqlite://"):
        raw = url[len("sqlite://") :]
    else:
        raw = url
    path = Path(raw)
    return path if path.is_absolute() else (ROOT / path).resolve()


BOT_DB_PATH = sqlite_url_to_path(str(settings.SQLITE_URL))


@contextmanager
def connect():  # type: ignore[no-untyped-def]
    if not DB_PATH.exists():
        raise RuntimeError(f"market-data database does not exist yet: {DB_PATH}")
    conn = sqlite3.connect(f"file:{DB_PATH.as_posix()}?mode=ro", uri=True, timeout=5.0)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        yield conn
    finally:
        conn.close()


@contextmanager
def connect_bot_db():  # type: ignore[no-untyped-def]
    if not BOT_DB_PATH.exists():
        raise RuntimeError(f"bot database does not exist yet: {BOT_DB_PATH}")
    conn = sqlite3.connect(f"file:{BOT_DB_PATH.as_posix()}?mode=ro", uri=True, timeout=5.0)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        yield conn
    finally:
        conn.close()


def parse_market(query: Dict[str, List[str]]) -> str:
    market = str(query.get("coin", [settings.MARKETS[0]])[0]).upper()
    if not MARKET_PATTERN.fullmatch(market):
        raise RuntimeError(f"invalid coin: {market!r}")
    return market


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return (
        conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", (table_name,)).fetchone()
        is not None
    )


def parse_number(query: Dict[str, List[str]], name: str, default: float, low: float, high: float) -> float:
    value = float(query.get(name, [default])[0])
    return min(max(value, low), high)


def snapshot_row(row: sqlite3.Row) -> Dict[str, Any]:
    out = dict(row)
    out.pop("_sample_row", None)
    for column, output_name in JSON_COLUMNS.items():
        raw = out.pop(column, "[]")
        try:
            out[output_name] = json.loads(raw)
        except Exception:
            out[output_name] = []
    return out


def evenly_sample(items: List[Any], limit: int) -> List[Any]:
    if len(items) <= limit:
        return items
    if limit <= 1:
        return [items[-1]]
    last = len(items) - 1
    indexes = sorted({round(index * last / (limit - 1)) for index in range(limit)})
    return [items[index] for index in indexes]


def latest_snapshot_diagnostics(market: str, cutoff_ms: int) -> Dict[str, Any]:
    try:
        with connect() as conn:
            row = conn.execute(
                """
                SELECT diagnostics_json
                FROM market_snapshots
                WHERE coin = ? AND captured_at_ms >= ?
                ORDER BY captured_at_ms DESC
                LIMIT 1
                """,
                (market, cutoff_ms),
            ).fetchone()
    except Exception:
        return {}
    if row is None:
        return {}
    try:
        diagnostics = json.loads(row["diagnostics_json"] or "{}")
        return diagnostics if isinstance(diagnostics, dict) else {}
    except Exception:
        return {}


def api_markets() -> Dict[str, Any]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT coin, COUNT(*) AS snapshot_count,
                   MIN(captured_at_ms) AS first_snapshot_ms,
                   MAX(captured_at_ms) AS last_snapshot_ms
            FROM market_snapshots
            GROUP BY coin
            ORDER BY coin
            """
        ).fetchall()
    return {
        "database": str(DB_PATH),
        "record_interval_s": settings.MARKET_DATA_RECORD_INTERVAL_S,
        "markets": [dict(row) for row in rows],
    }


def api_snapshots(query: Dict[str, List[str]]) -> Dict[str, Any]:
    market = parse_market(query)
    minutes = parse_number(query, "minutes", settings.MARKET_DATA_WEB_DEFAULT_MINUTES, 0.1, 1440.0)
    limit = int(parse_number(query, "limit", settings.MARKET_DATA_WEB_MAX_SNAPSHOTS, 10, 5000))
    range_end_ms = int(time.time() * 1000)
    cutoff_ms = range_end_ms - int(minutes * 60_000)
    with connect() as conn:
        available_count = conn.execute(
            "SELECT COUNT(*) FROM market_snapshots WHERE coin = ? AND captured_at_ms >= ?",
            (market, cutoff_ms),
        ).fetchone()[0]
        step = max(int(ceil(available_count / max(limit, 1))), 1)
        rows = conn.execute(
            """
            SELECT *
            FROM (
                SELECT market_snapshots.*,
                       ROW_NUMBER() OVER (ORDER BY captured_at_ms) AS _sample_row
                FROM market_snapshots
                WHERE coin = ? AND captured_at_ms >= ?
            )
            WHERE _sample_row = 1 OR _sample_row = ? OR (_sample_row % ?) = 0
            ORDER BY captured_at_ms DESC
            """,
            (market, cutoff_ms, available_count, step),
        ).fetchall()
    snapshots = evenly_sample([snapshot_row(row) for row in reversed(rows)], limit)
    return {
        "coin": market,
        "minutes": minutes,
        "range_start_ms": cutoff_ms,
        "range_end_ms": range_end_ms,
        "available_count": available_count,
        "count": len(snapshots),
        "sampled": available_count > len(snapshots),
        "snapshots": snapshots,
    }


def api_trades(query: Dict[str, List[str]]) -> Dict[str, Any]:
    market = parse_market(query)
    minutes = parse_number(query, "minutes", settings.MARKET_DATA_WEB_DEFAULT_MINUTES, 0.1, 1440.0)
    limit = int(parse_number(query, "limit", 5000, 10, 20000))
    range_end_ms = int(time.time() * 1000)
    cutoff_ms = range_end_ms - int(minutes * 60_000)
    with connect() as conn:
        available_count = conn.execute(
            "SELECT COUNT(*) FROM market_trades WHERE coin = ? AND timestamp_ms >= ?",
            (market, cutoff_ms),
        ).fetchone()[0]
        step = max(int(ceil(available_count / max(limit, 1))), 1)
        rows = conn.execute(
            """
            SELECT trade_key, coin, trade_id, side, price, size, timestamp_ms
            FROM (
                SELECT trade_key, coin, trade_id, side, price, size, timestamp_ms,
                       ROW_NUMBER() OVER (ORDER BY timestamp_ms) AS _sample_row
                FROM market_trades
                WHERE coin = ? AND timestamp_ms >= ?
            )
            WHERE _sample_row = 1 OR _sample_row = ? OR (_sample_row % ?) = 0
            ORDER BY timestamp_ms DESC
            """,
            (market, cutoff_ms, available_count, step),
        ).fetchall()
    trades = evenly_sample([dict(row) for row in reversed(rows)], limit)
    return {
        "coin": market,
        "minutes": minutes,
        "range_start_ms": cutoff_ms,
        "range_end_ms": range_end_ms,
        "available_count": available_count,
        "sampled": available_count > len(trades),
        "trades": trades,
    }


def empty_fills_payload(market: str, minutes: float, cutoff_ms: int, range_end_ms: int) -> Dict[str, Any]:
    return {
        "coin": market,
        "minutes": minutes,
        "range_start_ms": cutoff_ms,
        "range_end_ms": range_end_ms,
        "available_count": 0,
        "sampled": False,
        "fills": [],
        "database": str(BOT_DB_PATH),
    }


def api_fills(query: Dict[str, List[str]]) -> Dict[str, Any]:
    market = parse_market(query)
    minutes = parse_number(query, "minutes", settings.MARKET_DATA_WEB_DEFAULT_MINUTES, 0.1, 1440.0)
    limit = int(parse_number(query, "limit", 5000, 10, 20000))
    range_end_ms = int(time.time() * 1000)
    cutoff_ms = range_end_ms - int(minutes * 60_000)
    if not BOT_DB_PATH.exists():
        return empty_fills_payload(market, minutes, cutoff_ms, range_end_ms)
    with connect_bot_db() as conn:
        if not table_exists(conn, "fills"):
            return empty_fills_payload(market, minutes, cutoff_ms, range_end_ms)
        available_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM fills
            WHERE coin = ? AND timestamp_ms IS NOT NULL AND timestamp_ms >= ?
            """,
            (market, cutoff_ms),
        ).fetchone()[0]
        step = max(int(ceil(available_count / max(limit, 1))), 1)
        rows = conn.execute(
            """
            SELECT fill_id, client_order_id, exchange_order_id, exchange_trade_id,
                   coin, symbol, side, price, size, notional, fee, fee_currency, timestamp_ms
            FROM (
                SELECT fill_id, client_order_id, exchange_order_id, exchange_trade_id,
                       coin, symbol, side, price, size, notional, fee, fee_currency, timestamp_ms,
                       ROW_NUMBER() OVER (ORDER BY timestamp_ms) AS _sample_row
                FROM fills
                WHERE coin = ? AND timestamp_ms IS NOT NULL AND timestamp_ms >= ?
            )
            WHERE _sample_row = 1 OR _sample_row = ? OR (_sample_row % ?) = 0
            ORDER BY timestamp_ms DESC
            """,
            (market, cutoff_ms, available_count, step),
        ).fetchall()
    fills = evenly_sample([dict(row) for row in reversed(rows)], limit)
    return {
        "coin": market,
        "minutes": minutes,
        "range_start_ms": cutoff_ms,
        "range_end_ms": range_end_ms,
        "available_count": available_count,
        "sampled": available_count > len(fills),
        "fills": fills,
        "database": str(BOT_DB_PATH),
    }


def api_ev_surface(query: Dict[str, List[str]]) -> Dict[str, Any]:
    market = parse_market(query)
    minutes = parse_number(query, "minutes", settings.MARKET_DATA_WEB_DEFAULT_MINUTES, 0.1, 1440.0)
    range_end_ms = int(time.time() * 1000)
    cutoff_ms = range_end_ms - int(minutes * 60_000)
    diagnostics = latest_snapshot_diagnostics(market, cutoff_ms)
    choice = diagnostics.get("ev_choice") if isinstance(diagnostics.get("ev_choice"), dict) else {}
    rows = []
    if isinstance(diagnostics.get("ev_curve"), list):
        rows = [
            row
            for row in diagnostics["ev_curve"]
            if isinstance(row, dict) and row.get("half_spread_bps") is not None and row.get("ev_per_hour") is not None
        ]
    if not rows:
        rows = build_ev_surface(
            order_notional_usd=float(settings.TARGET_ORDER_NOTIONAL_USD),
            maker_fee_bps_per_side=float(settings.MAKER_FEE_BPS_PER_SIDE),
            min_half_spread_bps=float(settings.EV_MIN_HALF_SPREAD_BPS),
            max_half_spread_bps=float(settings.EV_MAX_HALF_SPREAD_BPS),
            half_spread_step_bps=float(settings.EV_HALF_SPREAD_STEP_BPS),
            min_trades_per_hour=1.0,
            max_trades_per_hour=float(settings.EV_MAX_FILLS_PER_HOUR),
            trades_per_hour_step=1.0,
            markout_bps=float(settings.EV_MARKOUT_BPS),
        )
    return {
        "coin": market,
        "minutes": minutes,
        "range_start_ms": cutoff_ms,
        "range_end_ms": range_end_ms,
        "order_notional_usd": float(settings.TARGET_ORDER_NOTIONAL_USD),
        "maker_fee_bps_per_side": float(settings.MAKER_FEE_BPS_PER_SIDE),
        "markout_bps": float(settings.EV_MARKOUT_BPS),
        "choice": choice,
        "choice_ask": diagnostics.get("ev_choice_ask") if isinstance(diagnostics.get("ev_choice_ask"), dict) else {},
        "choice_bid": diagnostics.get("ev_choice_bid") if isinstance(diagnostics.get("ev_choice_bid"), dict) else {},
        "choice_source": diagnostics.get("spread_source"),
        "ev_applied": bool(diagnostics.get("ev_applied", False)),
        "rows": rows,
        "rows_ask": [
            row
            for row in diagnostics.get("ev_curve_ask", [])
            if isinstance(row, dict) and row.get("half_spread_bps") is not None and row.get("ev_per_hour") is not None
        ],
        "rows_bid": [
            row
            for row in diagnostics.get("ev_curve_bid", [])
            if isinstance(row, dict) and row.get("half_spread_bps") is not None and row.get("ev_per_hour") is not None
        ],
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "MarketDataWeb/1.0"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/api/markets":
                self.send_json(api_markets())
                return
            if parsed.path == "/api/snapshots":
                self.send_json(api_snapshots(parse_qs(parsed.query)))
                return
            if parsed.path == "/api/trades":
                self.send_json(api_trades(parse_qs(parsed.query)))
                return
            if parsed.path == "/api/fills":
                self.send_json(api_fills(parse_qs(parsed.query)))
                return
            if parsed.path == "/api/ev-surface":
                self.send_json(api_ev_surface(parse_qs(parsed.query)))
                return
            if parsed.path in ("/", "/index.html"):
                self.send_file(WEB_ROOT / "index.html")
                return
            self.send_error(404)
        except Exception as exc:
            self.send_json({"error": str(exc)}, status=500)

    def send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_file(self, path: Path) -> None:
        if not path.is_file():
            self.send_error(404)
            return
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", mimetypes.guess_type(path.name)[0] or "application/octet-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[market-data-web] {self.address_string()} {fmt % args}")


def main() -> int:
    host = str(settings.MARKET_DATA_WEB_HOST)
    port = int(settings.MARKET_DATA_WEB_PORT)
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Market-data viewer: http://{host}:{port}")
    print(f"Database: {DB_PATH}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping market-data viewer.")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
