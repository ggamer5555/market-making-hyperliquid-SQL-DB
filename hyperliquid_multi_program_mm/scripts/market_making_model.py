from __future__ import annotations

import math
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

import settings
from common import coin_to_symbol, normalize_market_name
from scripts.ev_spread import EvSpreadResult


@dataclass
class Quote:
    coin: str
    symbol: str
    mid: float
    best_bid: float
    best_ask: float
    bid: Optional[float]
    ask: Optional[float]
    reservation_price: float
    half_spread: float
    half_spread_bps: float
    sigma_per_s: float
    q_norm: float
    inventory_base: float
    gamma: float
    k: float
    horizon_seconds: float
    fair_price: float
    spread_source: str
    ev_spread_result: Optional[Dict[str, float]]


@dataclass
class PriceWindow:
    maxlen: int
    mids: Deque[Tuple[float, float]] = field(default_factory=deque)

    def add(self, mid: float, ts: Optional[float] = None) -> None:
        self.mids.append((ts or time.time(), mid))
        while len(self.mids) > self.maxlen:
            self.mids.popleft()

    def sigma_per_s(self, fallback: float) -> float:
        if len(self.mids) < 5:
            return fallback
        rets: List[float] = []
        dts: List[float] = []
        prev_t, prev_m = self.mids[0]
        for t, m in list(self.mids)[1:]:
            if prev_m > 0 and m > 0 and t > prev_t:
                rets.append(math.log(m / prev_m))
                dts.append(t - prev_t)
            prev_t, prev_m = t, m
        if len(rets) < 4:
            return fallback
        stdev = statistics.pstdev(rets)
        avg_dt = max(statistics.mean(dts), 1e-9)
        sigma = stdev / math.sqrt(avg_dt)
        if not math.isfinite(sigma) or sigma <= 0:
            return fallback
        return max(sigma, fallback)


class AvellanedaStoikov:
    def __init__(self):
        self.windows: Dict[str, PriceWindow] = defaultdict(lambda: PriceWindow(int(settings.VOL_WINDOW)))

    def quote(
        self,
        coin: str,
        best_bid: float,
        best_ask: float,
        inventory_base: float,
        symbol: Optional[str] = None,
        fair_price: Optional[float] = None,
        ev_choice: Optional[EvSpreadResult] = None,
    ) -> Quote:
        coin = normalize_market_name(coin)
        symbol = symbol or coin_to_symbol(coin)
        mid = 0.5 * (best_bid + best_ask)
        fair = float(fair_price) if fair_price is not None and fair_price > 0 else mid
        self.windows[coin].add(fair)

        sigma = self.windows[coin].sigma_per_s(float(settings.FALLBACK_SIGMA_PER_S))
        gamma = float(settings.GAMMA)
        k = max(float(settings.K), 1e-9)
        horizon_s = max(float(settings.HORIZON_SECONDS), 1e-9)

        max_pos_notional = max(
            float(settings.MAX_LONG_INVENTORY_NOTIONAL_USD),
            float(settings.MAX_SHORT_INVENTORY_NOTIONAL_USD),
            1.0,
        )
        inventory_notional = inventory_base * fair
        q_norm = max(-3.0, min(3.0, inventory_notional / max_pos_notional))
        q_norm *= float(settings.INVENTORY_SKEW_SCALE)

        sigma2_t = sigma * sigma * horizon_s
        reservation = fair * (1.0 - q_norm * gamma * sigma2_t)

        min_half = float(settings.MIN_HALF_SPREAD_BPS) / 10000.0
        max_half = float(settings.MAX_HALF_SPREAD_BPS) / 10000.0
        spread_source = "avellaneda"
        if ev_choice is not None and ev_choice.estimated_fills_per_hour > 0:
            half_spread_pct = float(ev_choice.half_spread_bps) / 10000.0
            spread_source = "ev"
        else:
            half_spread_pct = 0.5 * gamma * sigma2_t + (1.0 / gamma) * math.log(1.0 + gamma / k)
        half_spread_pct = max(min_half, min(max_half, half_spread_pct))
        half_spread = fair * half_spread_pct

        bid = min(reservation - half_spread, best_bid)
        ask = max(reservation + half_spread, best_ask)
        if bid <= 0 or ask <= 0 or bid >= ask:
            bid = None
            ask = None

        return Quote(
            coin=coin,
            symbol=symbol,
            mid=mid,
            best_bid=best_bid,
            best_ask=best_ask,
            bid=bid,
            ask=ask,
            reservation_price=reservation,
            half_spread=half_spread,
            half_spread_bps=half_spread_pct * 10000.0,
            sigma_per_s=sigma,
            q_norm=q_norm,
            inventory_base=inventory_base,
            gamma=gamma,
            k=k,
            horizon_seconds=horizon_s,
            fair_price=fair,
            spread_source=spread_source,
            ev_spread_result=ev_choice.as_dict() if ev_choice is not None else None,
        )
