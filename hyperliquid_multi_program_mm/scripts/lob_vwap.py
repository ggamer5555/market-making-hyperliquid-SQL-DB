from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional, Sequence, Tuple

from connectors.base import OrderBookLevel
from scripts.orderbook import levels_within_mid_band


@dataclass
class LobVwapSample:
    ts: float
    fair_price: float
    total_base_size: float


@dataclass
class LobVwapWindow:
    window_s: float
    samples: Deque[LobVwapSample] = field(default_factory=deque)

    def add_from_book(
        self,
        bids: Sequence[OrderBookLevel],
        asks: Sequence[OrderBookLevel],
        mid: float,
        within_mid_pct: float,
        max_levels: int,
        ts: Optional[float] = None,
    ) -> Optional[float]:
        ts = float(ts if ts is not None else time.time())
        filtered_bids = levels_within_mid_band(bids, mid, within_mid_pct, max_levels)
        filtered_asks = levels_within_mid_band(asks, mid, within_mid_pct, max_levels)
        bid_vwap, bid_base = side_vwap(filtered_bids)
        ask_vwap, ask_base = side_vwap(filtered_asks)

        if bid_vwap is not None and ask_vwap is not None:
            fair_price = 0.5 * (bid_vwap + ask_vwap)
            total_base = bid_base + ask_base
            if fair_price > 0 and total_base > 0:
                self.samples.append(LobVwapSample(ts, fair_price, total_base))

        self.trim(ts)
        return self.value()

    def value(self) -> Optional[float]:
        if not self.samples:
            return None
        total_weight = sum(max(sample.total_base_size, 0.0) for sample in self.samples)
        if total_weight <= 0:
            return sum(sample.fair_price for sample in self.samples) / len(self.samples)
        return sum(
            sample.fair_price * max(sample.total_base_size, 0.0)
            for sample in self.samples
        ) / total_weight

    def trim(self, now: float) -> None:
        cutoff = now - max(float(self.window_s), 0.0)
        while self.samples and self.samples[0].ts < cutoff:
            self.samples.popleft()


def side_vwap(levels: Sequence[OrderBookLevel]) -> Tuple[Optional[float], float]:
    weighted_price_base = 0.0
    total_base = 0.0
    for level in levels:
        price = float(level.price)
        size = float(level.size)
        if price <= 0 or size <= 0:
            continue
        weighted_price_base += price * size
        total_base += size
    if total_base <= 0:
        return None, 0.0
    return weighted_price_base / total_base, total_base
