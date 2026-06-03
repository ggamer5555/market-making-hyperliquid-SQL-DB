from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class EvSpreadResult:
    half_spread_bps: float
    estimated_fills_per_hour: float
    clamped_fills_per_hour: float
    maker_fee_bps_per_side: float
    markout_bps: float
    net_edge_bps: float
    ev_per_hour: float

    def as_dict(self) -> Dict[str, float]:
        return asdict(self)


def clamp(value: float, low: float, high: float) -> float:
    return max(float(low), min(float(high), float(value)))


def spread_grid(min_bps: float, max_bps: float, step_bps: float) -> List[float]:
    if step_bps <= 0:
        raise ValueError("step_bps must be positive")
    out: List[float] = []
    value = float(min_bps)
    max_value = float(max_bps)
    while value <= max_value + 1e-12:
        out.append(round(value, 10))
        value += float(step_bps)
    return out


def trades_per_hour_grid(min_trades: float, max_trades: float, step: float) -> List[float]:
    if step <= 0:
        raise ValueError("step must be positive")
    out: List[float] = []
    value = float(min_trades)
    max_value = float(max_trades)
    while value <= max_value + 1e-12:
        out.append(round(value, 10))
        value += float(step)
    return out


def ev_per_hour(
    fills_per_hour: float,
    order_notional_usd: float,
    half_spread_bps: float,
    maker_fee_bps_per_side: float,
    markout_bps: float = 0.0,
) -> float:
    net_edge_bps = float(half_spread_bps) - float(maker_fee_bps_per_side) + float(markout_bps)
    return float(fills_per_hour) * float(order_notional_usd) * net_edge_bps / 10000.0


def build_ev_spread_curve(
    estimated_fills_by_spread: Dict[float, float],
    order_notional_usd: float,
    maker_fee_bps_per_side: float,
    min_half_spread_bps: float,
    max_half_spread_bps: float,
    step_bps: float,
    min_trades_per_hour: float = 0.0,
    max_trades_per_hour: float = 200.0,
    markout_by_spread: Optional[Dict[float, float]] = None,
) -> List[EvSpreadResult]:
    results: List[EvSpreadResult] = []
    for half_spread_bps in spread_grid(min_half_spread_bps, max_half_spread_bps, step_bps):
        estimated_fills = float(estimated_fills_by_spread.get(half_spread_bps, 0.0))
        clamped_fills = clamp(estimated_fills, min_trades_per_hour, max_trades_per_hour)
        markout_bps = float((markout_by_spread or {}).get(half_spread_bps, 0.0))
        net_edge_bps = half_spread_bps - float(maker_fee_bps_per_side) + markout_bps
        results.append(
            EvSpreadResult(
                half_spread_bps=half_spread_bps,
                estimated_fills_per_hour=estimated_fills,
                clamped_fills_per_hour=clamped_fills,
                maker_fee_bps_per_side=float(maker_fee_bps_per_side),
                markout_bps=markout_bps,
                net_edge_bps=net_edge_bps,
                ev_per_hour=ev_per_hour(
                    fills_per_hour=clamped_fills,
                    order_notional_usd=order_notional_usd,
                    half_spread_bps=half_spread_bps,
                    maker_fee_bps_per_side=maker_fee_bps_per_side,
                    markout_bps=markout_bps,
                ),
            )
        )
    return results


def choose_ev_half_spread_bps(curve: List[EvSpreadResult]) -> Optional[EvSpreadResult]:
    if not curve:
        return None
    return max(curve, key=lambda row: row.ev_per_hour)


def build_ev_surface(
    order_notional_usd: float,
    maker_fee_bps_per_side: float,
    min_half_spread_bps: float,
    max_half_spread_bps: float,
    half_spread_step_bps: float,
    min_trades_per_hour: float,
    max_trades_per_hour: float,
    trades_per_hour_step: float,
    markout_bps: float = 0.0,
) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    for half_spread_bps in spread_grid(min_half_spread_bps, max_half_spread_bps, half_spread_step_bps):
        for fills_per_hour in trades_per_hour_grid(
            min_trades_per_hour,
            max_trades_per_hour,
            trades_per_hour_step,
        ):
            net_edge_bps = half_spread_bps - maker_fee_bps_per_side + markout_bps
            rows.append(
                {
                    "half_spread_bps": half_spread_bps,
                    "fills_per_hour": fills_per_hour,
                    "maker_fee_bps_per_side": maker_fee_bps_per_side,
                    "markout_bps": markout_bps,
                    "net_edge_bps": net_edge_bps,
                    "ev_per_hour": ev_per_hour(
                        fills_per_hour,
                        order_notional_usd,
                        half_spread_bps,
                        maker_fee_bps_per_side,
                        markout_bps,
                    ),
                }
            )
    return rows
