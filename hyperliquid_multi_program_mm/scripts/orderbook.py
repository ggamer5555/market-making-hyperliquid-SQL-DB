from __future__ import annotations

import statistics
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from connectors.base import OrderBookLevel


@dataclass(frozen=True)
class BookResolution:
    n_sig_figs: Optional[int]

    def __post_init__(self) -> None:
        if self.n_sig_figs not in (None, 5, 4, 3, 2):
            raise ValueError("Hyperliquid nSigFigs must be null, 5, 4, 3, or 2")

    @property
    def key(self) -> str:
        return "raw" if self.n_sig_figs is None else f"sig{self.n_sig_figs}"

    def subscription(self, coin: str) -> Dict[str, object]:
        out: Dict[str, object] = {"type": "l2Book", "coin": str(coin)}
        if self.n_sig_figs is not None:
            out["nSigFigs"] = self.n_sig_figs
        return out


@dataclass
class ResolutionBookSnapshot:
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    received_at: float
    exchange_timestamp_ms: Optional[int]


@dataclass
class SyntheticBookSnapshot:
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    received_at: float
    exchange_timestamp_ms: Optional[int]
    resolution_level_counts: Dict[str, Dict[str, int]]
    synthetic_source_counts: Dict[str, Dict[str, int]]
    bbo_age_ms: Optional[int]


class MultiResolutionOrderBook:
    """Build deeper L2 without counting coarse buckets over finer ranges twice."""

    def __init__(
        self,
        coin: str,
        resolutions: Sequence[BookResolution],
        priority: Sequence[str],
        stale_s: float,
        price_step_fallback: float,
    ) -> None:
        self.coin = str(coin)
        self.resolutions = {resolution.key: resolution for resolution in resolutions}
        self.priority = list(priority)
        missing = [key for key in self.priority if key not in self.resolutions]
        if missing:
            raise ValueError(f"unknown resolution priority keys: {missing}")
        self.stale_s = max(float(stale_s), 0.05)
        self.price_step_fallback = max(float(price_step_fallback), 1e-12)
        self.snapshots: Dict[str, ResolutionBookSnapshot] = {}
        self.bbo: Optional[ResolutionBookSnapshot] = None

    def update(
        self,
        resolution_key: str,
        bids: Sequence[OrderBookLevel],
        asks: Sequence[OrderBookLevel],
        exchange_timestamp_ms: Optional[int] = None,
        received_at: Optional[float] = None,
    ) -> None:
        if resolution_key not in self.resolutions:
            raise ValueError(f"unknown book resolution: {resolution_key}")
        self.snapshots[resolution_key] = ResolutionBookSnapshot(
            bids=list(bids),
            asks=list(asks),
            received_at=float(received_at if received_at is not None else time.time()),
            exchange_timestamp_ms=exchange_timestamp_ms,
        )

    def update_bbo(
        self,
        bid: OrderBookLevel,
        ask: OrderBookLevel,
        exchange_timestamp_ms: Optional[int] = None,
        received_at: Optional[float] = None,
    ) -> None:
        if bid.price <= 0 or ask.price <= 0 or bid.price >= ask.price:
            return
        self.bbo = ResolutionBookSnapshot(
            bids=[bid],
            asks=[ask],
            received_at=float(received_at if received_at is not None else time.time()),
            exchange_timestamp_ms=exchange_timestamp_ms,
        )

    def snapshot(self, depth: Optional[int] = None, now: Optional[float] = None) -> Optional[SyntheticBookSnapshot]:
        now = float(now if now is not None else time.time())
        fresh = {
            key: snapshot
            for key, snapshot in self.snapshots.items()
            if now - snapshot.received_at <= self.stale_s
        }
        if not fresh:
            return None
        bids, bid_sources = self._merge_side(fresh, is_bid=True)
        asks, ask_sources = self._merge_side(fresh, is_bid=False)
        bbo_age_ms: Optional[int] = None
        received_times = [snapshot.received_at for snapshot in fresh.values()]
        exchange_times = [
            snapshot.exchange_timestamp_ms
            for snapshot in fresh.values()
            if snapshot.exchange_timestamp_ms is not None
        ]
        if self.bbo is not None and now - self.bbo.received_at <= self.stale_s:
            bids = self._patch_bbo_side(bids, self.bbo.bids[0], is_bid=True)
            asks = self._patch_bbo_side(asks, self.bbo.asks[0], is_bid=False)
            received_times.append(self.bbo.received_at)
            if self.bbo.exchange_timestamp_ms is not None:
                exchange_times.append(self.bbo.exchange_timestamp_ms)
            bbo_age_ms = max(0, int((now - self.bbo.received_at) * 1000))
        if not bids or not asks or bids[0].price >= asks[0].price:
            return None
        if depth is not None:
            bids = bids[:depth]
            asks = asks[:depth]
        return SyntheticBookSnapshot(
            bids=bids,
            asks=asks,
            received_at=max(received_times),
            exchange_timestamp_ms=max(exchange_times) if exchange_times else None,
            resolution_level_counts={
                key: {"bids": len(snapshot.bids), "asks": len(snapshot.asks)}
                for key, snapshot in fresh.items()
            },
            synthetic_source_counts={"bids": bid_sources, "asks": ask_sources},
            bbo_age_ms=bbo_age_ms,
        )

    def _merge_side(
        self,
        snapshots: Dict[str, ResolutionBookSnapshot],
        *,
        is_bid: bool,
    ) -> Tuple[List[OrderBookLevel], Dict[str, int]]:
        merged: List[OrderBookLevel] = []
        seen_prices = set()
        source_counts: Dict[str, int] = {}
        for key in self.priority:
            snapshot = snapshots.get(key)
            if snapshot is None:
                continue
            levels = snapshot.bids if is_bid else snapshot.asks
            levels = sorted(
                (level for level in levels if level.price > 0 and level.size >= 0),
                key=lambda level: level.price,
                reverse=is_bid,
            )
            step = self._observed_step(levels)
            added = 0
            for level in levels:
                if level.price in seen_prices:
                    continue
                if merged:
                    outer_price = merged[-1].price
                    if is_bid and level.price + step > outer_price:
                        continue
                    if not is_bid and level.price - step < outer_price:
                        continue
                merged.append(self._tagged_level(level, key))
                seen_prices.add(level.price)
                added += 1
            merged.sort(key=lambda level: level.price, reverse=is_bid)
            if added:
                source_counts[key] = added
        return merged, source_counts

    def _observed_step(self, levels: Sequence[OrderBookLevel]) -> float:
        prices = sorted({level.price for level in levels})
        steps = [next_price - price for price, next_price in zip(prices, prices[1:]) if next_price > price]
        return float(statistics.median(steps)) if steps else self.price_step_fallback

    @staticmethod
    def _tagged_level(level: OrderBookLevel, source: str) -> OrderBookLevel:
        return OrderBookLevel(
            price=level.price,
            size=level.size,
            order_count=level.order_count,
            raw={**level.raw, "syntheticBookSource": source},
        )

    @classmethod
    def _patch_bbo_side(
        cls,
        levels: Sequence[OrderBookLevel],
        bbo: OrderBookLevel,
        *,
        is_bid: bool,
    ) -> List[OrderBookLevel]:
        out = [
            level
            for level in levels
            if (level.price <= bbo.price if is_bid else level.price >= bbo.price)
        ]
        out = [level for level in out if level.price != bbo.price]
        out.append(cls._tagged_level(bbo, "bbo"))
        return sorted(out, key=lambda level: level.price, reverse=is_bid)


@dataclass
class LobGuard:
    bid_price: Optional[float]
    ask_price: Optional[float]
    bid_percentile_price: Optional[float]
    ask_percentile_price: Optional[float]
    price_step: float


def levels_within_mid_band(
    levels: Sequence[OrderBookLevel],
    mid: float,
    within_mid_pct: float,
    max_levels: int,
) -> List[OrderBookLevel]:
    low = mid * (1.0 - within_mid_pct)
    high = mid * (1.0 + within_mid_pct)
    return [level for level in levels if low <= level.price <= high][:max_levels]


def percentile_price(levels: Sequence[OrderBookLevel], percentile: float, *, is_bid: bool) -> Optional[float]:
    """Return the first outward price where cumulative size reaches a depth percentile."""
    if not levels or percentile <= 0:
        return None
    ordered = sorted(levels, key=lambda level: level.price, reverse=is_bid)
    total_size = sum(max(level.size, 0.0) for level in ordered)
    if total_size <= 0:
        return None
    target = percentile * total_size
    cumulative = 0.0
    for level in ordered:
        cumulative += max(level.size, 0.0)
        if cumulative >= target:
            return level.price
    return ordered[-1].price


def median_price_step(
    bids: Sequence[OrderBookLevel],
    asks: Sequence[OrderBookLevel],
    fallback: float,
) -> float:
    steps: List[float] = []
    for levels in (bids, asks):
        prices = sorted({level.price for level in levels})
        steps.extend(b - a for a, b in zip(prices, prices[1:]) if b > a)
    return float(statistics.median(steps)) if steps else fallback


def calculate_lob_guard(
    bids: Sequence[OrderBookLevel],
    asks: Sequence[OrderBookLevel],
    mid: float,
    percentile: float,
    within_mid_pct: float,
    max_levels: int,
    fallback_step: float,
) -> LobGuard:
    filtered_bids = levels_within_mid_band(bids, mid, within_mid_pct, max_levels)
    filtered_asks = levels_within_mid_band(asks, mid, within_mid_pct, max_levels)
    step = median_price_step(filtered_bids, filtered_asks, fallback_step)
    bid_percentile = percentile_price(filtered_bids, percentile, is_bid=True)
    ask_percentile = percentile_price(filtered_asks, percentile, is_bid=False)
    return LobGuard(
        bid_price=bid_percentile + step if bid_percentile is not None else None,
        ask_price=ask_percentile - step if ask_percentile is not None else None,
        bid_percentile_price=bid_percentile,
        ask_percentile_price=ask_percentile,
        price_step=step,
    )


def outward_level_prices(
    side: str,
    closest_price: float,
    count: int,
    spacing_bps: float,
    price_step: float,
) -> List[float]:
    prices: List[float] = []
    previous: Optional[float] = None
    for level in range(max(count, 0)):
        multiplier = 1.0 + (-1.0 if side == "buy" else 1.0) * spacing_bps * level / 10000.0
        price = closest_price * multiplier
        if previous is not None:
            if side == "buy" and price >= previous:
                price = previous - price_step
            elif side == "sell" and price <= previous:
                price = previous + price_step
        prices.append(price)
        previous = price
    return prices
