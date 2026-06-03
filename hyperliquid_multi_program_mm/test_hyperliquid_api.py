from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

import settings
from common import normalize_market_name
from connectors.hyperliquid_connector import HyperliquidConnector
from hyperliquid.utils.types import Cloid


REPORT_DIR = Path("./reports")
REDACTED = "***REDACTED***"
SENSITIVE_KEY_PARTS = ("private", "secret", "signature", "password", "api_key", "apikey", "authorization")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def package_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "not installed"


def is_sensitive_key(key: Any) -> bool:
    lowered = str(key).lower().replace("-", "_")
    return any(part in lowered for part in SENSITIVE_KEY_PARTS)


def json_safe(value: Any) -> Any:
    if is_dataclass(value):
        return json_safe(asdict(value))
    if isinstance(value, dict):
        return {str(key): REDACTED if is_sensitive_key(key) else json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def truncate_lists(value: Any, max_items: Optional[int]) -> Any:
    if isinstance(value, dict):
        return {key: truncate_lists(item, max_items) for key, item in value.items()}
    if isinstance(value, list):
        items = [truncate_lists(item, max_items) for item in value]
        if max_items is None or len(items) <= max_items:
            return items
        return {
            "_report_note": "list truncated for readability; rerun with --full for every item",
            "_total_items": len(items),
            "_items_in_report": max_items,
            "_items": items[:max_items],
        }
    return value


def format_shape(value: Any, depth: int = 0) -> Any:
    if depth >= 5:
        return type(value).__name__
    if isinstance(value, dict):
        return {key: format_shape(item, depth + 1) for key, item in value.items()}
    if isinstance(value, list):
        return {
            "type": "list",
            "length": len(value),
            "item_format": format_shape(value[0], depth + 1) if value else None,
        }
    if value is None:
        return "null"
    return type(value).__name__


def pretty_json(value: Any, max_items: Optional[int]) -> str:
    safe_value = json_safe(value)
    return json.dumps(truncate_lists(safe_value, max_items), indent=2, sort_keys=True, default=str)


class TextReport:
    def __init__(self, output_path: Path, max_items: Optional[int]) -> None:
        self.output_path = output_path
        self.max_items = max_items
        self.lines: List[str] = []
        self.results: Dict[str, Any] = {}
        self.ok_count = 0
        self.error_count = 0

    def write(self, text: str = "") -> None:
        self.lines.append(text)

    def heading(self, title: str, underline: str = "=") -> None:
        self.write(title)
        self.write(underline * len(title))
        self.write()

    def call(self, name: str, description: str, fn: Callable[[], Any]) -> Any:
        started = time.perf_counter()
        self.heading(name, "-")
        self.write(f"description: {description}")
        try:
            result = fn()
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            safe_result = json_safe(result)
            self.results[name] = result
            self.ok_count += 1
            self.write("status: OK")
            self.write(f"elapsed_ms: {elapsed_ms:.1f}")
            self.write("format:")
            self.write(pretty_json(format_shape(safe_result), self.max_items))
            self.write("response:")
            self.write(pretty_json(safe_result, self.max_items))
            self.write()
            print(f"OK    {name} ({elapsed_ms:.1f} ms)")
            return result
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            self.results[name] = None
            self.error_count += 1
            self.write("status: ERROR")
            self.write(f"elapsed_ms: {elapsed_ms:.1f}")
            self.write(f"error_type: {type(exc).__name__}")
            self.write(f"error: {exc}")
            self.write("traceback:")
            self.write(traceback.format_exc().rstrip())
            self.write()
            print(f"ERROR {name}: {type(exc).__name__}: {exc}")
            return None

    def skipped(self, name: str, reason: str) -> None:
        self.heading(name, "-")
        self.write("status: SKIPPED")
        self.write(f"reason: {reason}")
        self.write()

    def save(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        text = "\n".join(self.lines) + "\n"
        if settings.PRIVATE_KEY and settings.PRIVATE_KEY in text:
            raise RuntimeError("Refusing to save report because the configured private key was not redacted.")
        self.output_path.write_text(text, encoding="utf-8")


def recent_window_ms(days: float) -> tuple[int, int]:
    end = utc_now()
    start = end - timedelta(days=days)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def recent_candle_window_ms(hours: float = 1.0) -> tuple[int, int]:
    end = utc_now()
    start = end - timedelta(hours=hours)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def unique_order_ids(*responses: Any) -> List[int]:
    out: List[int] = []
    seen = set()

    def add(raw_oid: Any) -> None:
        try:
            oid = int(raw_oid)
        except Exception:
            return
        if oid not in seen:
            seen.add(oid)
            out.append(oid)

    for response in responses:
        if not isinstance(response, list):
            continue
        for item in response:
            if not isinstance(item, dict):
                continue
            if isinstance(item.get("order"), dict):
                add(item["order"].get("oid"))
            else:
                add(item.get("oid"))
    return out


def unique_cloids(*responses: Any) -> List[str]:
    out: List[str] = []
    seen = set()
    for response in responses:
        if not isinstance(response, list):
            continue
        for item in response:
            if not isinstance(item, dict):
                continue
            order = item.get("order") if isinstance(item.get("order"), dict) else item
            cloid = order.get("cloid") if isinstance(order, dict) else None
            if isinstance(cloid, str) and cloid and cloid not in seen:
                seen.add(cloid)
                out.append(cloid)
    return out


def market_metadata(connector: HyperliquidConnector, market: str) -> Dict[str, Any]:
    market_u = connector.normalize_market(market)
    dex = connector._dex_for_market_name(market_u)
    meta = connector.info.meta(dex=dex) if dex else connector.meta
    universe = meta.get("universe", []) if isinstance(meta, dict) else []
    universe_item = next(
        (
            item
            for item in universe
            if isinstance(item, dict)
            and normalize_market_name(
                f"{dex}:{item.get('name')}" if dex and ":" not in str(item.get("name") or "") else str(item.get("name") or "")
            )
            == market_u
        ),
        None,
    )
    return {
        "input_market": market,
        "market": market_u,
        "symbol": connector.symbol_for_market(market_u),
        "dex": dex or "<default>",
        "universe_item": universe_item,
        "size_step": connector.size_step(market_u),
        "round_size_example": connector.round_size(market_u, 0.0123456789),
        "round_buy_price_example": connector.round_price(market_u, "buy", 12345.6789),
        "round_sell_price_example": connector.round_price(market_u, "sell", 12345.6789),
        "post_only_time_in_force": connector.post_only_time_in_force,
    }


def websocket_cache_sample(
    connector: HyperliquidConnector,
    markets: Sequence[str],
    timeout_s: float = 8.0,
) -> Dict[str, Any]:
    wanted = [connector.normalize_market(market) for market in markets]
    resolutions = connector._configured_book_resolutions()
    expected_resolution_keys = set(connector._configured_book_priority())
    connector.start_background_streams(wanted)
    try:
        deadline = time.time() + timeout_s
        snapshots: Dict[str, Any] = {}
        while time.time() < deadline:
            for market in wanted:
                cached = connector._cached_order_book(market, depth=settings.LOB_MAX_LEVELS)
                if cached is None:
                    continue
                bids, asks = cached
                metadata = connector.fetch_order_book_cache_metadata(market)
                snapshots[market] = {
                    "best_bid": asdict(bids[0]),
                    "best_ask": asdict(asks[0]),
                    "synthetic_bid_level_count": len(bids),
                    "synthetic_ask_level_count": len(asks),
                    "cache_metadata": metadata,
                    "last_public_trade": (
                        asdict(connector.fetch_last_cached_market_trade(market))
                        if connector.fetch_last_cached_market_trade(market) is not None
                        else None
                    ),
                }
            warmed = all(
                expected_resolution_keys.issubset(
                    set((snapshots.get(market, {}).get("cache_metadata", {}).get("resolution_level_counts") or {}).keys())
                )
                for market in wanted
            )
            if len(snapshots) == len(wanted) and warmed:
                return {
                    "subscriptions": [
                        {"method": "subscribe", "subscription": {"type": "l2Book", "coin": market}}
                        for market in wanted
                    ] + [
                        {"method": "subscribe", "subscription": {"type": "bbo", "coin": market}}
                        for market in wanted
                    ] + [
                        {"method": "subscribe", "subscription": {"type": "trades", "coin": market}}
                        for market in wanted
                    ] + [
                        {"method": "subscribe", "subscription": resolution.subscription(market)}
                        for resolution in resolutions
                        if resolution.n_sig_figs is not None
                        for market in wanted
                    ] + [
                        {
                            "method": "subscribe",
                            "subscription": {"type": "orderUpdates", "user": connector.account_address},
                        }
                    ],
                    "l2_cache_snapshots": snapshots,
                    "cached_open_orders_after_subscription": [
                        asdict(order) for order in connector.fetch_cached_open_orders()
                    ],
                    "note": "Websocket subscription only. No order action was signed or sent.",
                }
            time.sleep(0.1)
        missing = [market for market in wanted if market not in snapshots]
        raise RuntimeError(f"websocket L2 cache did not warm before timeout markets={missing}")
    finally:
        connector.stop_background_streams()


def trade_api_examples(connector: HyperliquidConnector, market: str) -> Dict[str, Any]:
    market_u = connector.normalize_market(market)
    best_bid, best_ask, _mid = connector.fetch_bbo(market_u)
    exposure = max(float(settings.TARGET_ORDER_NOTIONAL_USD), float(settings.MIN_OPEN_ORDER_NOTIONAL_USD))
    buy_price = connector.round_price(market_u, "buy", best_bid)
    sell_price = connector.round_price(market_u, "sell", best_ask)
    buy_size = connector.round_size_up(market_u, exposure / buy_price)
    sell_size = connector.round_size_up(market_u, exposure / sell_price)
    buy_cloid = "0x11111111111111111111111111111111"
    sell_cloid = "0x22222222222222222222222222222222"
    return {
        "warning": "Examples only. This report does not execute, sign, or send these write calls.",
        "initial_post_only_order": {
            "connector_method": "connector.place_limit_order",
            "connector_arguments": {
                "market": market_u,
                "side": "buy",
                "size": buy_size,
                "price": buy_price,
                "reduce_only": False,
                "post_only": True,
                "client_order_id": buy_cloid,
            },
            "sdk_method": "sdk_exchange.order",
            "sdk_arguments": {
                "market": market_u,
                "is_buy": True,
                "size": buy_size,
                "price": buy_price,
                "order_type": {"limit": {"tif": settings.POST_ONLY_TIF}},
                "reduce_only": False,
                "cloid": buy_cloid,
            },
        },
        "amend_existing_bid_and_ask_in_one_request": {
            "connector_method": "connector.bulk_edit_orders",
            "sdk_method": "sdk_exchange.bulk_modify_orders_new",
            "sdk_arguments": [
                {
                    "oid": "<CURRENT_BUY_OID>",
                    "order": {
                        "coin": market_u,
                        "is_buy": True,
                        "sz": buy_size,
                        "limit_px": buy_price,
                        "order_type": {"limit": {"tif": settings.POST_ONLY_TIF}},
                        "reduce_only": False,
                        "cloid": buy_cloid,
                    },
                },
                {
                    "oid": "<CURRENT_SELL_OID>",
                    "order": {
                        "coin": market_u,
                        "is_buy": False,
                        "sz": sell_size,
                        "limit_px": sell_price,
                        "order_type": {"limit": {"tif": settings.POST_ONLY_TIF}},
                        "reduce_only": False,
                        "cloid": sell_cloid,
                    },
                },
            ],
            "important_note": (
                "batchModify can return replacement oids. Keep the returned oids in the cache. "
                "A replacement oid is an amendment result, not a missing-side placement."
            ),
        },
        "cancel_order": {
            "connector_method": "connector.cancel_order",
            "preferred_sdk_method": "sdk_exchange.cancel_by_cloid",
            "preferred_sdk_arguments": {"market": market_u, "cloid": buy_cloid},
            "fallback_sdk_method": "sdk_exchange.cancel",
            "fallback_sdk_arguments": {"market": market_u, "oid": "<CURRENT_BUY_OID>"},
        },
        "read_only_state_used_by_strategy": {
            "inventory": f"connector.fetch_inventory_balances([{market_u!r}])",
            "resting_orders_cache": f"connector.fetch_cached_open_orders({market_u!r})",
            "resting_orders_rest_reconciliation": f"connector.fetch_open_orders({market_u!r}, force=True)",
            "order_book": f"connector.fetch_order_book({market_u!r}, depth=settings.LOB_MAX_LEVELS)",
        },
        "expected_modify_response_shape": {
            "status": "ok",
            "response": {
                "data": {
                    "statuses": [
                        {"resting": {"oid": "<REPLACEMENT_BUY_OID>"}},
                        {"resting": {"oid": "<REPLACEMENT_SELL_OID>"}},
                    ]
                }
            },
        },
    }


def raw_l2_book(connector: HyperliquidConnector, market: str, n_sig_figs: Optional[int] = None) -> Dict[str, Any]:
    market_u = connector.normalize_market(market)
    body: Dict[str, Any] = {"type": "l2Book", "coin": market_u}
    if n_sig_figs is not None:
        body["nSigFigs"] = int(n_sig_figs)
    raw = connector.info.post("/info", body)
    return raw if isinstance(raw, dict) else {"raw": raw}


def raw_candle_snapshot(
    connector: HyperliquidConnector,
    market: str,
    start_ms: int,
    end_ms: int,
    interval: str = "1m",
) -> Any:
    market_u = connector.normalize_market(market)
    return connector.info.post(
        "/info",
        {
            "type": "candleSnapshot",
            "req": {"coin": market_u, "interval": interval, "startTime": start_ms, "endTime": end_ms},
        },
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run read-only Hyperliquid API diagnostics and save response formats to a text report."
    )
    parser.add_argument("--days", type=float, default=7.0, help="Lookback window for fills and funding history. Default: 7")
    parser.add_argument("--max-items", type=int, default=50, help="Maximum list items written per response. Default: 50")
    parser.add_argument("--full", action="store_true", help="Write every list item instead of truncating long responses.")
    parser.add_argument("--max-order-statuses", type=int, default=10, help="Maximum oid and cloid status lookups. Default: 10")
    parser.add_argument(
        "--markets",
        nargs="+",
        help="Optional settings-style market names to test, e.g. BTC-USDC xyz:AMD-USDC. Default: settings.MARKETS",
    )
    parser.add_argument("--output", type=Path, help="Optional output .txt path. Default: reports/hyperliquid_api_readonly_TIMESTAMP.txt")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if args.days <= 0:
        raise RuntimeError("--days must be greater than zero")
    if args.max_items <= 0:
        raise RuntimeError("--max-items must be greater than zero")
    if args.max_order_statuses < 0:
        raise RuntimeError("--max-order-statuses must not be negative")

    timestamp = utc_now().strftime("%Y%m%d_%H%M%S_UTC")
    output_path = args.output or REPORT_DIR / f"hyperliquid_api_readonly_{timestamp}.txt"
    if output_path.suffix.lower() != ".txt":
        raise RuntimeError("--output must use a .txt extension")

    report = TextReport(output_path, None if args.full else args.max_items)
    report.heading("HYPERLIQUID READ-ONLY API DIAGNOSTIC REPORT")
    report.write("This program does not place orders, cancel orders, transfer funds, or sign exchange actions.")
    report.write("It calls read-only Hyperliquid /info methods, websocket subscriptions, and connector normalization methods.")
    report.write()
    report.write(f"created_at_utc: {utc_now_iso()}")
    report.write(f"python: {sys.version.replace(chr(10), ' ')}")
    report.write(f"hyperliquid-python-sdk: {package_version('hyperliquid-python-sdk')}")
    report.write(f"eth-account: {package_version('eth-account')}")
    report.write(f"account_address: {settings.WALLET_ADDRESS}")
    report.write("private_key: ***REDACTED***")
    requested_markets = list(args.markets or settings.MARKETS)
    report.write(f"requested_markets: {requested_markets}")
    report.write(f"list_mode: {'full' if args.full else f'max {args.max_items} items per list'}")
    report.write()

    connector = report.call(
        "connector.initialize",
        "Construct the Hyperliquid connector, validate main-account versus API-agent configuration, and load metadata.",
        HyperliquidConnector,
    )
    if not isinstance(connector, HyperliquidConnector):
        report.heading("SUMMARY")
        report.write(f"successful_calls: {report.ok_count}")
        report.write(f"failed_calls: {report.error_count}")
        report.write("result: connector initialization failed; remaining calls were not attempted")
        report.save()
        print(f"\nSaved report: {report.output_path.resolve()}")
        return 1

    info = connector.info
    account = connector.account_address
    signer = connector.signer_address
    markets = [connector.normalize_market(market) for market in requested_markets]
    configured_dexs = [""] + sorted(
        dex for dex in {connector._dex_for_market_name(market) for market in markets} if dex
    )
    start_ms, end_ms = recent_window_ms(args.days)
    candle_start_ms, candle_end_ms = recent_candle_window_ms()

    report.call("connector.identity", "Connector addresses and local configuration without secrets.", lambda: {
        "name": connector.name,
        "base_url": connector.base_url,
        "account_address": connector.account_address,
        "signer_address": connector.signer_address,
        "post_only_time_in_force": connector.post_only_time_in_force,
        "requested_markets": requested_markets,
        "normalized_markets": markets,
        "configured_dexs": [dex or "<default>" for dex in configured_dexs],
    })

    report.heading("PUBLIC MARKET DATA")
    report.call("info.meta", "Perpetual market metadata.", info.meta)
    report.call("info.meta_and_asset_ctxs", "Perpetual metadata paired with live asset contexts.", info.meta_and_asset_ctxs)
    report.call("info.all_mids", "Current midpoint prices for all markets.", info.all_mids)
    for dex in configured_dexs:
        label = dex or "default"
        report.call(f"info.meta[dex={label}]", "Perp metadata for the configured market dex.", lambda dex=dex: info.meta(dex=dex))
        report.call(
            f"info.meta_and_asset_ctxs[dex={label}]",
            "Perp metadata plus live contexts for the configured market dex.",
            lambda dex=dex: info.post("/info", {"type": "metaAndAssetCtxs", "dex": dex}),
        )
        report.call(f"info.all_mids[dex={label}]", "Current midpoint prices for the configured market dex.", lambda dex=dex: info.all_mids(dex=dex))
    report.call("info.spot_meta", "Spot-market metadata.", info.spot_meta)
    report.call("info.spot_meta_and_asset_ctxs", "Spot metadata paired with live asset contexts.", info.spot_meta_and_asset_ctxs)
    report.call("info.perp_dexs", "Available perpetual DEX metadata.", info.perp_dexs)

    report.heading("CONFIGURED MARKET DATA")
    for market in markets:
        market_u = connector.normalize_market(market)
        report.call(
            f"connector.market_metadata[{market_u}]",
            "Local connector precision, symbol, and rounding examples. No order is submitted.",
            lambda market=market_u: market_metadata(connector, market),
        )
        report.call(
            f"info.l2_snapshot[{market_u}]",
            "Raw order-book snapshot using the exact Hyperliquid coin string.",
            lambda market=market_u: raw_l2_book(connector, market),
        )
        for n_sig_figs in (5, 4, 3, 2):
            report.call(
                f"info.l2_snapshot[{market_u}, nSigFigs={n_sig_figs}]",
                "Aggregated order-book snapshot.",
                lambda market=market_u, n_sig_figs=n_sig_figs: raw_l2_book(connector, market, n_sig_figs),
            )
        report.call(
            f"info.recent_trades[{market_u}]",
            "Raw recent public trades.",
            lambda market=market_u: info.post("/info", {"type": "recentTrades", "coin": market}),
        )
        report.call(
            f"connector.fetch_bbo[{market_u}]",
            "Normalized best bid, best ask, and midpoint parsed from the raw order book.",
            lambda market=market_u: connector.fetch_bbo(market),
        )
        report.call(
            f"info.candles_snapshot[{market_u}]",
            "Raw one-minute candles from the most recent hour.",
            lambda market=market_u: raw_candle_snapshot(connector, market, candle_start_ms, candle_end_ms),
        )
    report.call(
        "connector.websocket_cache_sample",
        "Subscribe to raw and aggregated l2Book, bbo, trades, and orderUpdates, then capture synthetic cached depth. This is read-only.",
        lambda: websocket_cache_sample(connector, markets),
    )

    report.heading("RAW ACCOUNT DATA")
    account_role = report.call("info.user_role[account]", "Role reported for the configured main account.", lambda: info.user_role(account))
    signer_role = report.call("info.user_role[signer]", "Role reported for the signing wallet.", lambda: info.user_role(signer))
    report.call("info.extra_agents[account]", "Approved API-agent wallets for the configured account.", lambda: info.extra_agents(account))
    raw_user_state = report.call("info.user_state[account]", "Raw perpetual balances, margin summary, and positions.", lambda: info.user_state(account))
    report.call("info.spot_user_state[account]", "Raw spot balances.", lambda: info.spot_user_state(account))
    raw_open_orders = report.call("info.open_orders[account]", "Raw currently resting orders.", lambda: info.open_orders(account))
    raw_frontend_orders = report.call(
        "info.frontend_open_orders[account]",
        "Raw resting orders with frontend fields such as cloid, tif, and reduceOnly.",
        lambda: info.frontend_open_orders(account),
    )
    raw_open_orders_by_dex: List[Any] = []
    raw_frontend_orders_by_dex: List[Any] = []
    raw_user_states_by_dex: List[Any] = []
    for dex in configured_dexs:
        label = dex or "default"
        raw_user_states_by_dex.append(
            report.call(
                f"info.user_state[account,dex={label}]",
                "Raw perp balances and positions for the configured dex.",
                lambda dex=dex: info.user_state(account, dex=dex),
            )
        )
        raw_open_orders_by_dex.append(
            report.call(
                f"info.open_orders[account,dex={label}]",
                "Raw resting orders for the configured dex.",
                lambda dex=dex: info.open_orders(account, dex=dex),
            )
        )
        raw_frontend_orders_by_dex.append(
            report.call(
                f"info.frontend_open_orders[account,dex={label}]",
                "Raw frontend resting orders for the configured dex.",
                lambda dex=dex: info.frontend_open_orders(account, dex=dex),
            )
        )
    raw_fills = report.call("info.user_fills[account]", "Raw recent fills returned by Hyperliquid.", lambda: info.user_fills(account))
    raw_window_fills = report.call(
        "info.user_fills_by_time[account]",
        f"Raw fills in the configured {args.days:g}-day lookback window.",
        lambda: info.user_fills_by_time(account, start_ms, end_ms),
    )
    raw_historical_orders = report.call(
        "info.historical_orders[account]",
        "Raw historical orders. The SDK documents a maximum of 2000 recent entries.",
        lambda: info.historical_orders(account),
    )
    report.call("info.user_fees[account]", "Raw fee schedule and trading-volume information.", lambda: info.user_fees(account))
    report.call("info.user_rate_limit[account]", "Raw API rate-limit information.", lambda: info.user_rate_limit(account))
    report.call("info.portfolio[account]", "Raw portfolio history.", lambda: info.portfolio(account))
    report.call(
        "info.user_funding_history[account]",
        f"Raw user funding history in the configured {args.days:g}-day lookback window.",
        lambda: info.user_funding_history(account, start_ms, end_ms),
    )

    report.heading("NORMALIZED CONNECTOR DATA")
    report.call("connector.fetch_signer_role", "Normalized signer role.", connector.fetch_signer_role)
    report.call("connector.fetch_account_summary", "Normalized account summary passthrough.", connector.fetch_account_summary)
    report.call("connector.fetch_positions", "Normalized list of perpetual positions.", connector.fetch_positions)
    report.call("connector.fetch_spot_balances", "Normalized spot balances.", connector.fetch_spot_balances)
    report.call("connector.fetch_open_orders", "Normalized open orders used by strategies.", connector.fetch_open_orders)
    report.call("connector.fetch_open_order_details", "Exchange-specific open-order detail payload.", connector.fetch_open_order_details)
    report.call(
        "connector.fetch_recent_fills",
        f"Normalized fills in the configured {args.days:g}-day lookback window.",
        lambda: connector.fetch_recent_fills(start_ms),
    )
    for market in markets:
        market_u = connector.normalize_market(market)
        report.call(
            f"connector.fetch_position_size[{market_u}]",
            "Normalized current position quantity and raw matching position payload.",
            lambda market=market_u: connector.fetch_position_size(market),
        )

    report.heading("ORDER STATUS LOOKUPS")
    order_ids = unique_order_ids(
        raw_open_orders,
        raw_frontend_orders,
        raw_window_fills,
        raw_fills,
        raw_historical_orders,
        *raw_open_orders_by_dex,
        *raw_frontend_orders_by_dex,
    )
    cloids = unique_cloids(
        raw_open_orders,
        raw_frontend_orders,
        raw_window_fills,
        raw_fills,
        raw_historical_orders,
        *raw_open_orders_by_dex,
        *raw_frontend_orders_by_dex,
    )
    report.write(f"discovered_unique_oids: {len(order_ids)}")
    report.write(f"discovered_unique_cloids: {len(cloids)}")
    report.write(f"sample_limit_per_identifier_type: {args.max_order_statuses}")
    report.write()
    for oid in order_ids[: args.max_order_statuses]:
        report.call(
            f"info.query_order_by_oid[{oid}]",
            "Raw order-status lookup by exchange order ID.",
            lambda oid=oid: info.query_order_by_oid(account, oid),
        )
    for cloid in cloids[: args.max_order_statuses]:
        report.call(
            f"info.query_order_by_cloid[{cloid}]",
            "Raw order-status lookup by client order ID.",
            lambda cloid=cloid: info.query_order_by_cloid(account, Cloid.from_str(cloid)),
        )

    report.heading("TRADE API EXAMPLES GENERATED LOCALLY")
    report.write("The following payload examples are generated locally from current public prices.")
    report.write("They document live connector usage but are never signed or sent by this diagnostic.")
    report.write()
    for market in markets:
        market_u = connector.normalize_market(market)
        report.call(
            f"local.trade_api_examples[{market_u}]",
            "Generated placement, bulk amendment, cancellation, and read-only state examples. No write call is executed.",
            lambda market=market_u: trade_api_examples(connector, market),
        )

    report.heading("SIGNED WRITE CALLS")
    report.skipped("connector.place_limit_order", "Intentionally skipped: this diagnostic is read-only and must not place live orders.")
    report.skipped("connector.bulk_edit_orders", "Intentionally skipped: this diagnostic is read-only and must not amend live orders.")
    report.skipped("connector.cancel_order", "Intentionally skipped: this diagnostic is read-only and must not cancel live orders.")
    report.skipped("connector.set_market_leverage", "Intentionally skipped: this diagnostic is read-only and must not change live leverage.")
    report.skipped("transfers and withdrawals", "Intentionally skipped: this diagnostic never transfers funds.")

    report.heading("SUMMARY")
    report.write(f"account_role: {account_role}")
    report.write(f"signer_role: {signer_role}")
    report.write(f"successful_calls: {report.ok_count}")
    report.write(f"failed_calls: {report.error_count}")
    report.write(f"report_path: {report.output_path.resolve()}")
    report.write("mutating_calls_executed: 0")
    report.write()
    report.save()

    print()
    print(f"Saved report: {report.output_path.resolve()}")
    print(f"Successful calls: {report.ok_count}")
    print(f"Failed calls: {report.error_count}")
    print("Mutating calls executed: 0")
    return 0 if report.error_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
