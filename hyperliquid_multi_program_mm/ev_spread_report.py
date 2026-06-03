from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any, Optional

import settings
from scripts.ev_spread import build_ev_spread_curve, build_ev_surface, choose_ev_half_spread_bps
from scripts.trade_expectancy import (
    estimate_fills_curve,
    estimate_fills_curve_from_trade_fairs,
    estimate_fills_curve_from_sweeps,
    load_market_trades_from_sqlite,
    load_sweep_samples_from_sqlite,
    load_trade_fair_samples_from_sqlite,
)


ROOT = Path(__file__).resolve().parent


def latest_fair_price(db_path: Path, coin: str) -> tuple[float, Optional[int], str]:
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT captured_at_ms, mid_price, diagnostics_json
            FROM market_snapshots
            WHERE coin = ?
            ORDER BY captured_at_ms DESC
            LIMIT 1
            """,
            (coin.upper(),),
        ).fetchone()
    if row is None:
        raise RuntimeError(f"no snapshots found for coin={coin}")
    captured_at_ms, mid, diagnostics_raw = row
    try:
        diagnostics: dict[str, Any] = json.loads(diagnostics_raw or "{}")
    except Exception:
        diagnostics = {}
    for source, value in (
        ("diagnostics.ev_fair_price", diagnostics.get("ev_fair_price")),
        ("diagnostics.fair_price", diagnostics.get("fair_price")),
        ("diagnostics.lob_vwap_fair_price", diagnostics.get("lob_vwap_fair_price")),
        ("mid_price", mid),
    ):
        if value is not None and float(value) > 0:
            return float(value), int(captured_at_ms), source
    raise RuntimeError(f"latest snapshot has no usable fair price coin={coin}")


def write_report(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Estimate EV spread curve from stored market_trades.")
    parser.add_argument("--coin", default=settings.MARKETS[0])
    parser.add_argument("--minutes", type=float, default=float(settings.EV_TRADE_LOOKBACK_S) / 60.0)
    parser.add_argument("--db", default=settings.MARKET_DATA_DB_PATH)
    parser.add_argument("--out", default="ev_spread_report.txt")
    args = parser.parse_args()

    coin = str(args.coin).upper()
    db_path = (ROOT / str(args.db)).resolve()
    out_path = (ROOT / str(args.out)).resolve()
    fair_price, latest_snapshot_ms, fair_source = latest_fair_price(db_path, coin)
    lookback_s = max(float(args.minutes) * 60.0, 1.0)
    order_notional = max(float(settings.TARGET_ORDER_NOTIONAL_USD), float(settings.MIN_OPEN_ORDER_NOTIONAL_USD))
    sweeps = load_sweep_samples_from_sqlite(
        db_path,
        coin,
        lookback_s,
        float(settings.EV_SWEEP_GROUP_MS),
        float(settings.EV_DEPTH_WITHIN_MID_PCT),
        now_ms=latest_snapshot_ms,
    )
    trade_fair_samples = load_trade_fair_samples_from_sqlite(db_path, coin, lookback_s, now_ms=latest_snapshot_ms)
    trades = load_market_trades_from_sqlite(db_path, coin, lookback_s, now_ms=latest_snapshot_ms)
    observed_hours = lookback_s / 3600.0
    if sweeps:
        estimated_fills = estimate_fills_curve_from_sweeps(
            sweeps,
            order_notional,
            float(settings.EV_MIN_HALF_SPREAD_BPS),
            float(settings.EV_MAX_HALF_SPREAD_BPS),
            float(settings.EV_HALF_SPREAD_STEP_BPS),
            observed_hours=observed_hours,
        )
        expectancy_source = "grouped taker sweeps vs recorded LOB notional depth"
    elif trade_fair_samples:
        estimated_fills = estimate_fills_curve_from_trade_fairs(
            trade_fair_samples,
            float(settings.EV_MIN_HALF_SPREAD_BPS),
            float(settings.EV_MAX_HALF_SPREAD_BPS),
            float(settings.EV_HALF_SPREAD_STEP_BPS),
            observed_hours=observed_hours,
        )
        expectancy_source = "trade prices matched to recorded fair-price samples fallback"
    else:
        estimated_fills = estimate_fills_curve(
            trades,
            fair_price,
            float(settings.EV_MIN_HALF_SPREAD_BPS),
            float(settings.EV_MAX_HALF_SPREAD_BPS),
            float(settings.EV_HALF_SPREAD_STEP_BPS),
            observed_hours=observed_hours,
        )
        expectancy_source = "single latest fair price fallback"
    curve = build_ev_spread_curve(
        estimated_fills,
        order_notional_usd=order_notional,
        maker_fee_bps_per_side=float(settings.MAKER_FEE_BPS_PER_SIDE),
        min_half_spread_bps=float(settings.EV_MIN_HALF_SPREAD_BPS),
        max_half_spread_bps=float(settings.EV_MAX_HALF_SPREAD_BPS),
        step_bps=float(settings.EV_HALF_SPREAD_STEP_BPS),
        min_trades_per_hour=0.0,
        max_trades_per_hour=float(settings.EV_MAX_FILLS_PER_HOUR),
        markout_by_spread={
            spread: float(settings.EV_MARKOUT_BPS)
            for spread in estimated_fills
        },
    )
    choice = choose_ev_half_spread_bps(curve)
    surface_rows = build_ev_surface(
        order_notional_usd=order_notional,
        maker_fee_bps_per_side=float(settings.MAKER_FEE_BPS_PER_SIDE),
        min_half_spread_bps=float(settings.EV_MIN_HALF_SPREAD_BPS),
        max_half_spread_bps=float(settings.EV_MAX_HALF_SPREAD_BPS),
        half_spread_step_bps=float(settings.EV_HALF_SPREAD_STEP_BPS),
        min_trades_per_hour=1.0,
        max_trades_per_hour=float(settings.EV_MAX_FILLS_PER_HOUR),
        trades_per_hour_step=1.0,
        markout_bps=float(settings.EV_MARKOUT_BPS),
    )

    lines = [
        f"coin={coin}",
        f"db={db_path}",
        f"fair_price={fair_price:.10g} source={fair_source}",
        f"lookback_minutes={float(args.minutes):.6g}",
        f"public_trade_samples={len(trades)}",
        f"taker_sweeps={len(sweeps)} sweep_group_ms={float(settings.EV_SWEEP_GROUP_MS):.6g}",
        f"ev_depth_within_mid_pct={float(settings.EV_DEPTH_WITHIN_MID_PCT):.6g}",
        f"trade_fair_samples={len(trade_fair_samples)}",
        f"expectancy_source={expectancy_source}",
        f"observed_hours={observed_hours:.6g}",
        f"ev_surface_debug_rows={len(surface_rows)} spread_grid=1..50 trades_per_hour_grid=1..200",
        "",
    ]
    if choice is None:
        lines.append("choice=None")
    else:
        lines.extend(
            [
                "choice:",
                f"  half_spread_bps={choice.half_spread_bps}",
                f"  estimated_fills_per_hour={choice.estimated_fills_per_hour:.6g}",
                f"  clamped_fills_per_hour={choice.clamped_fills_per_hour:.6g}",
                f"  net_edge_bps={choice.net_edge_bps:.6g}",
                f"  ev_per_hour={choice.ev_per_hour:.10g}",
                "",
            ]
        )
    lines.append("curve:")
    lines.append("half_spread_bps estimated_fills_per_hour net_edge_bps ev_per_hour")
    for row in curve:
        lines.append(
            f"{row.half_spread_bps:>15.1f} "
            f"{row.estimated_fills_per_hour:>24.6f} "
            f"{row.net_edge_bps:>12.6f} "
            f"{row.ev_per_hour:>11.8f}"
        )
    write_report(out_path, lines)
    print("\n".join(lines[:16]))
    print(f"\nSaved full curve to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
