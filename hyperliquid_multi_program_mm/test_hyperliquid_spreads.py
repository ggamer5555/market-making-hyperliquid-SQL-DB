from __future__ import annotations

import argparse
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from hyperliquid.info import Info
from hyperliquid.utils import constants


@dataclass
class SpreadStats:
    market: str
    day_notional_volume: float = 0.0
    samples: int = 0
    latest_bid: float = 0.0
    latest_ask: float = 0.0
    latest_spread: float = 0.0
    latest_spread_bps: float = 0.0
    min_spread_bps: float = float("inf")
    max_spread_bps: float = 0.0
    total_spread_bps: float = 0.0
    http_fallback_samples: int = 0
    first_timestamp_ms: Optional[int] = None
    last_timestamp_ms: Optional[int] = None

    @property
    def average_spread_bps(self) -> float:
        return self.total_spread_bps / self.samples if self.samples else 0.0

    def add(self, bid: float, ask: float, timestamp_ms: Optional[int], *, from_http_fallback: bool = False) -> None:
        if bid <= 0 or ask <= 0 or bid >= ask:
            return
        midpoint = 0.5 * (bid + ask)
        spread = ask - bid
        spread_bps = spread / midpoint * 10000.0
        self.samples += 1
        self.latest_bid = bid
        self.latest_ask = ask
        self.latest_spread = spread
        self.latest_spread_bps = spread_bps
        self.min_spread_bps = min(self.min_spread_bps, spread_bps)
        self.max_spread_bps = max(self.max_spread_bps, spread_bps)
        self.total_spread_bps += spread_bps
        if from_http_fallback:
            self.http_fallback_samples += 1
        self.first_timestamp_ms = self.first_timestamp_ms or timestamp_ms
        self.last_timestamp_ms = timestamp_ms


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def safe_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def active_market_stats(info: Info) -> Dict[str, SpreadStats]:
    meta, asset_contexts = info.meta_and_asset_ctxs()
    universe = meta.get("universe", []) if isinstance(meta, dict) else []
    stats: Dict[str, SpreadStats] = {}
    for index, item in enumerate(universe):
        if not isinstance(item, dict) or item.get("isDelisted"):
            continue
        market = str(item.get("name") or "").strip()
        if not market:
            continue
        context = asset_contexts[index] if index < len(asset_contexts) and isinstance(asset_contexts[index], dict) else {}
        stats[market] = SpreadStats(
            market=market,
            day_notional_volume=safe_float(context.get("dayNtlVlm"), 0.0),
        )
    return stats


def parse_bbo_message(message: Any) -> tuple[Optional[str], float, float, Optional[int]]:
    data = message.get("data") if isinstance(message, dict) else None
    if not isinstance(data, dict):
        return None, 0.0, 0.0, None
    market = str(data.get("coin") or "").strip()
    bbo = data.get("bbo")
    if not market or not isinstance(bbo, list) or len(bbo) < 2:
        return None, 0.0, 0.0, None
    bid_level, ask_level = bbo[0], bbo[1]
    if not isinstance(bid_level, dict) or not isinstance(ask_level, dict):
        return None, 0.0, 0.0, None
    return market, safe_float(bid_level.get("px")), safe_float(ask_level.get("px")), safe_int(data.get("time"))


def parse_l2_snapshot(snapshot: Any) -> tuple[float, float, Optional[int]]:
    if not isinstance(snapshot, dict):
        return 0.0, 0.0, None
    levels = snapshot.get("levels")
    if not isinstance(levels, list) or len(levels) < 2:
        return 0.0, 0.0, safe_int(snapshot.get("time"))
    bids, asks = levels[0], levels[1]
    if not isinstance(bids, list) or not bids or not isinstance(asks, list) or not asks:
        return 0.0, 0.0, safe_int(snapshot.get("time"))
    bid_level, ask_level = bids[0], asks[0]
    if not isinstance(bid_level, dict) or not isinstance(ask_level, dict):
        return 0.0, 0.0, safe_int(snapshot.get("time"))
    return safe_float(bid_level.get("px")), safe_float(ask_level.get("px")), safe_int(snapshot.get("time"))


def print_results(stats_by_market: Dict[str, SpreadStats], top: Optional[int]) -> None:
    ranked = sorted(
        (stats for stats in stats_by_market.values() if stats.samples),
        key=lambda stats: (stats.average_spread_bps, stats.latest_spread_bps),
        reverse=True,
    )
    no_data = sorted(stats.market for stats in stats_by_market.values() if not stats.samples)
    displayed = ranked if top is None else ranked[:top]

    print()
    print("HYPERLIQUID PERPETUAL SPREADS: HIGHEST TO LOWEST")
    print("Ranking metric: average top-of-book bid-ask spread in basis points during the sampling window.")
    print()
    header = (
        f"{'#':>4}  {'MARKET':<18} {'AVG BPS':>10} {'LAST BPS':>10} "
        f"{'MIN BPS':>10} {'MAX BPS':>10} {'LAST BID':>16} {'LAST ASK':>16} "
        f"{'SPREAD':>14} {'SAMPLES':>8} {'HTTP':>6} {'24H VOLUME USD':>18}"
    )
    print(header)
    print("-" * len(header))
    for rank, stats in enumerate(displayed, start=1):
        print(
            f"{rank:>4}  {stats.market:<18} "
            f"{stats.average_spread_bps:>10.4f} {stats.latest_spread_bps:>10.4f} "
            f"{stats.min_spread_bps:>10.4f} {stats.max_spread_bps:>10.4f} "
            f"{stats.latest_bid:>16.8g} {stats.latest_ask:>16.8g} "
            f"{stats.latest_spread:>14.8g} {stats.samples:>8} {stats.http_fallback_samples:>6} "
            f"{stats.day_notional_volume:>18,.2f}"
        )

    print()
    print(f"Markets discovered: {len(stats_by_market)}")
    print(f"Markets with valid spread samples: {len(ranked)}")
    print(f"Markets without valid BBO samples: {len(no_data)}")
    if top is not None and len(ranked) > top:
        print(f"Displayed markets: top {top} of {len(ranked)}. Omit --top to print every ranked market.")
    if no_data:
        print("No-data markets:")
        print(", ".join(no_data))


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect Hyperliquid perpetual BBO spreads over websocket and print a highest-to-lowest ranking."
    )
    parser.add_argument("--seconds", type=float, default=60.0, help="Sampling duration in seconds. Default: 60")
    parser.add_argument("--progress-seconds", type=float, default=10.0, help="Progress-print interval. Default: 10")
    parser.add_argument("--top", type=int, help="Print only the top N ranked markets. Default: print all markets")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if args.seconds <= 0:
        raise RuntimeError("--seconds must be greater than zero")
    if args.progress_seconds <= 0:
        raise RuntimeError("--progress-seconds must be greater than zero")
    if args.top is not None and args.top <= 0:
        raise RuntimeError("--top must be greater than zero")

    print("Starting read-only Hyperliquid spread scanner.")
    print("No account credentials are loaded. No orders are placed or canceled.")
    info = Info(constants.MAINNET_API_URL, skip_ws=False)
    stats_by_market = active_market_stats(info)
    lock = threading.Lock()
    subscription_ids: List[tuple[Dict[str, str], int]] = []
    callback_errors: List[str] = []

    def on_bbo(message: Any) -> None:
        try:
            market, bid, ask, timestamp_ms = parse_bbo_message(message)
            if not market:
                return
            with lock:
                stats = stats_by_market.get(market)
                if stats is not None:
                    stats.add(bid, ask, timestamp_ms)
        except Exception as exc:
            with lock:
                callback_errors.append(f"{type(exc).__name__}: {exc}")

    print(f"Discovered {len(stats_by_market)} active perpetual markets.")
    print(f"Subscribing to BBO streams and sampling for {args.seconds:g} seconds...")
    started = time.monotonic()
    next_progress = started + args.progress_seconds
    try:
        for market in stats_by_market:
            subscription: Dict[str, str] = {"type": "bbo", "coin": market}
            subscription_id = info.subscribe(subscription, on_bbo)
            subscription_ids.append((subscription, subscription_id))

        deadline = started + args.seconds
        while True:
            now = time.monotonic()
            if now >= deadline:
                break
            if now >= next_progress:
                with lock:
                    sampled_markets = sum(1 for stats in stats_by_market.values() if stats.samples)
                    total_samples = sum(stats.samples for stats in stats_by_market.values())
                elapsed = now - started
                print(f"Progress: {elapsed:.1f}s elapsed, {sampled_markets}/{len(stats_by_market)} markets sampled, {total_samples} BBO updates.")
                next_progress = now + args.progress_seconds
            time.sleep(min(0.25, max(0.0, deadline - now)))
    finally:
        for subscription, subscription_id in subscription_ids:
            try:
                info.unsubscribe(subscription, subscription_id)
            except Exception:
                pass
        info.disconnect_websocket()

    unsampled = [stats for stats in stats_by_market.values() if not stats.samples]
    if unsampled:
        print(f"Fetching one read-only HTTP order-book snapshot for {len(unsampled)} unsampled markets...")
        for stats in unsampled:
            try:
                bid, ask, timestamp_ms = parse_l2_snapshot(info.l2_snapshot(stats.market))
                stats.add(bid, ask, timestamp_ms, from_http_fallback=True)
            except Exception as exc:
                callback_errors.append(f"HTTP fallback {stats.market}: {type(exc).__name__}: {exc}")

    with lock:
        print_results(stats_by_market, args.top)
        if callback_errors:
            print()
            print(f"Callback errors: {len(callback_errors)}")
            for error in callback_errors[:10]:
                print(f"- {error}")
    return 0 if not callback_errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
