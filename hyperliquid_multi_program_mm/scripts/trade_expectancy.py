from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Sequence

from common import normalize_market_name
from scripts.ev_spread import spread_grid


@dataclass(frozen=True)
class TradeSample:
    ts_ms: int
    side: str
    price: float
    size: float

    @property
    def notional(self) -> float:
        return self.price * self.size


@dataclass(frozen=True)
class TradeFairSample:
    trade: TradeSample
    fair_price: float


@dataclass(frozen=True)
class BookLevelSample:
    price: float
    size: float

    @property
    def notional(self) -> float:
        return self.price * self.size


@dataclass(frozen=True)
class BookSnapshotSample:
    ts_ms: int
    fair_price: float
    bids: tuple[BookLevelSample, ...]
    asks: tuple[BookLevelSample, ...]


@dataclass(frozen=True)
class SweepSample:
    ts_ms: int
    side: str
    notional: float
    min_price: float
    max_price: float
    trade_count: int
    book: BookSnapshotSample


def sample_from_trade(trade: object) -> Optional[TradeSample]:
    timestamp = getattr(trade, "timestamp_ms", None)
    if timestamp is None:
        timestamp = getattr(trade, "ts_ms", None)
    side = str(getattr(trade, "side", "")).lower()
    price = getattr(trade, "price", None)
    size = getattr(trade, "size", None)
    try:
        ts_ms = int(timestamp)
        price_f = float(price)
        size_f = float(size)
    except (TypeError, ValueError):
        return None
    if side not in ("buy", "sell") or price_f <= 0 or size_f <= 0:
        return None
    return TradeSample(ts_ms=ts_ms, side=side, price=price_f, size=size_f)


def book_level_from_object(level: object) -> Optional[BookLevelSample]:
    if isinstance(level, dict):
        price = level.get("price")
        size = level.get("size")
    else:
        price = getattr(level, "price", None)
        size = getattr(level, "size", None)
    try:
        price_f = float(price)
        size_f = float(size)
    except (TypeError, ValueError):
        return None
    if price_f <= 0 or size_f <= 0:
        return None
    return BookLevelSample(price=price_f, size=size_f)


def book_snapshot_from_objects(
    ts_ms: int,
    fair_price: float,
    bids: Iterable[object],
    asks: Iterable[object],
    within_mid_pct: Optional[float] = None,
) -> Optional[BookSnapshotSample]:
    low = fair_price * (1.0 - float(within_mid_pct)) if within_mid_pct is not None else None
    high = fair_price * (1.0 + float(within_mid_pct)) if within_mid_pct is not None else None
    bid_levels = tuple(
        level
        for raw in bids
        if (level := book_level_from_object(raw)) is not None
        and (low is None or level.price >= low)
        and (high is None or level.price <= high)
    )
    ask_levels = tuple(
        level
        for raw in asks
        if (level := book_level_from_object(raw)) is not None
        and (low is None or level.price >= low)
        and (high is None or level.price <= high)
    )
    if fair_price <= 0 or not bid_levels or not ask_levels:
        return None
    return BookSnapshotSample(
        ts_ms=int(ts_ms),
        fair_price=float(fair_price),
        bids=tuple(sorted(bid_levels, key=lambda level: level.price, reverse=True)),
        asks=tuple(sorted(ask_levels, key=lambda level: level.price)),
    )


def simulated_fill_count(samples: Sequence[TradeFairSample], half_spread_bps: float) -> int:
    simulated_fills = 0
    for sample in samples:
        fair_price = float(sample.fair_price)
        if fair_price <= 0:
            continue
        ask_price = fair_price * (1.0 + float(half_spread_bps) / 10000.0)
        bid_price = fair_price * (1.0 - float(half_spread_bps) / 10000.0)
        trade = sample.trade
        if trade.side == "buy" and trade.price >= ask_price:
            simulated_fills += 1
        elif trade.side == "sell" and trade.price <= bid_price:
            simulated_fills += 1
    return simulated_fills


def diagnostic_fair_price(raw: object, mid_price: object) -> float:
    try:
        diagnostics = json.loads(str(raw or "{}"))
    except Exception:
        diagnostics = {}
    if isinstance(diagnostics, dict):
        for key in ("ev_fair_price", "fair_price", "lob_vwap_fair_price"):
            value = diagnostics.get(key)
            try:
                if value is not None and float(value) > 0:
                    return float(value)
            except (TypeError, ValueError):
                continue
    try:
        return float(mid_price)
    except (TypeError, ValueError):
        return 0.0


def grouped_sweeps_from_trade_books(
    trade_books: Sequence[tuple[TradeSample, BookSnapshotSample]],
    sweep_group_ms: float,
) -> list[SweepSample]:
    if not trade_books:
        return []
    sorted_rows = sorted(trade_books, key=lambda row: (row[0].ts_ms, row[0].side))
    group_ms = max(int(float(sweep_group_ms)), 1)
    groups: list[SweepSample] = []
    current_key: Optional[tuple[str, int]] = None
    current_book: Optional[BookSnapshotSample] = None
    notional = 0.0
    min_price = 0.0
    max_price = 0.0
    trade_count = 0

    def flush() -> None:
        nonlocal current_key, current_book, notional, min_price, max_price, trade_count
        if current_key is None or current_book is None or trade_count <= 0:
            return
        side, bucket = current_key
        groups.append(
            SweepSample(
                ts_ms=bucket * group_ms,
                side=side,
                notional=notional,
                min_price=min_price,
                max_price=max_price,
                trade_count=trade_count,
                book=current_book,
            )
        )

    for trade, book in sorted_rows:
        bucket = trade.ts_ms // group_ms
        key = (trade.side, bucket)
        if key != current_key:
            flush()
            current_key = key
            current_book = book
            notional = 0.0
            min_price = trade.price
            max_price = trade.price
            trade_count = 0
        notional += trade.notional
        min_price = min(min_price, trade.price)
        max_price = max(max_price, trade.price)
        trade_count += 1
    flush()
    return groups


def grouped_sweeps_from_trades_with_book(
    trades: Sequence[object],
    book: BookSnapshotSample,
    sweep_group_ms: float,
) -> list[SweepSample]:
    trade_books = [
        (sample, book)
        for trade in trades
        if (sample := sample_from_trade(trade)) is not None
    ]
    return grouped_sweeps_from_trade_books(trade_books, sweep_group_ms)


def notional_ahead_for_candidate(sweep: SweepSample, half_spread_bps: float) -> tuple[float, float]:
    fair_price = sweep.book.fair_price
    if sweep.side == "buy":
        candidate_price = fair_price * (1.0 + float(half_spread_bps) / 10000.0)
        levels = sweep.book.asks
        notional_ahead = sum(level.notional for level in levels if level.price < candidate_price)
    elif sweep.side == "sell":
        candidate_price = fair_price * (1.0 - float(half_spread_bps) / 10000.0)
        levels = sweep.book.bids
        notional_ahead = sum(level.notional for level in levels if level.price > candidate_price)
    else:
        return 0.0, 0.0
    return candidate_price, notional_ahead


def sweep_fill_capacity(
    sweep: SweepSample,
    half_spread_bps: float,
    order_notional_usd: float,
    current_depth: bool = False,
) -> float:
    candidate_price, notional_ahead = notional_ahead_for_candidate(sweep, half_spread_bps)
    if candidate_price <= 0:
        return 0.0
    if not current_depth:
        if sweep.side == "buy" and sweep.max_price < candidate_price:
            return 0.0
        if sweep.side == "sell" and sweep.min_price > candidate_price:
            return 0.0
    capacity = (sweep.notional - notional_ahead) / max(float(order_notional_usd), 1e-9)
    return max(0.0, capacity)


def sweep_would_fill(
    sweep: SweepSample,
    half_spread_bps: float,
    order_notional_usd: float,
) -> bool:
    return sweep_fill_capacity(sweep, half_spread_bps, order_notional_usd) >= 1.0


def current_depth_sweep_would_fill(
    sweep: SweepSample,
    half_spread_bps: float,
    order_notional_usd: float,
) -> bool:
    return sweep_fill_capacity(sweep, half_spread_bps, order_notional_usd, current_depth=True) >= 1.0


def simulated_sweep_fill_count(
    sweeps: Sequence[SweepSample],
    half_spread_bps: float,
    order_notional_usd: float,
) -> float:
    return sum(
        sweep_fill_capacity(sweep, half_spread_bps, order_notional_usd)
        for sweep in sweeps
    )


def simulated_current_depth_sweep_fill_count(
    sweeps: Sequence[SweepSample],
    half_spread_bps: float,
    order_notional_usd: float,
) -> float:
    return sum(
        sweep_fill_capacity(sweep, half_spread_bps, order_notional_usd, current_depth=True)
        for sweep in sweeps
    )


def estimate_fills_per_hour_from_trades(
    trades: Sequence[object],
    fair_price: float,
    half_spread_bps: float,
    observed_hours: Optional[float] = None,
) -> float:
    samples = [sample for trade in trades if (sample := sample_from_trade(trade)) is not None]
    if not samples or fair_price <= 0:
        return 0.0
    if observed_hours is None:
        first_ts = min(sample.ts_ms for sample in samples)
        last_ts = max(sample.ts_ms for sample in samples)
        observed_hours = (last_ts - first_ts) / 3_600_000.0
    observed_hours = max(float(observed_hours), 1e-9)

    simulated_fills = simulated_fill_count(
        [TradeFairSample(sample, fair_price) for sample in samples],
        half_spread_bps,
    )
    return simulated_fills / observed_hours


def estimate_fills_per_hour_from_trade_fairs(
    samples: Sequence[TradeFairSample],
    half_spread_bps: float,
    observed_hours: Optional[float] = None,
) -> float:
    usable = [sample for sample in samples if sample.fair_price > 0]
    if not usable:
        return 0.0
    if observed_hours is None:
        first_ts = min(sample.trade.ts_ms for sample in usable)
        last_ts = max(sample.trade.ts_ms for sample in usable)
        observed_hours = (last_ts - first_ts) / 3_600_000.0
    observed_hours = max(float(observed_hours), 1e-9)
    return simulated_fill_count(usable, half_spread_bps) / observed_hours


def estimate_fills_curve(
    trades: Sequence[object],
    fair_price: float,
    min_half_spread_bps: float,
    max_half_spread_bps: float,
    step_bps: float,
    observed_hours: Optional[float] = None,
) -> Dict[float, float]:
    return {
        half_spread_bps: estimate_fills_per_hour_from_trades(
            trades,
            fair_price,
            half_spread_bps,
            observed_hours=observed_hours,
        )
        for half_spread_bps in spread_grid(min_half_spread_bps, max_half_spread_bps, step_bps)
    }


def estimate_fills_curve_from_trade_fairs(
    samples: Sequence[TradeFairSample],
    min_half_spread_bps: float,
    max_half_spread_bps: float,
    step_bps: float,
    observed_hours: Optional[float] = None,
) -> Dict[float, float]:
    return {
        half_spread_bps: estimate_fills_per_hour_from_trade_fairs(
            samples,
            half_spread_bps,
            observed_hours=observed_hours,
        )
        for half_spread_bps in spread_grid(min_half_spread_bps, max_half_spread_bps, step_bps)
    }


def estimate_fills_per_hour_from_sweeps(
    sweeps: Sequence[SweepSample],
    half_spread_bps: float,
    order_notional_usd: float,
    observed_hours: Optional[float] = None,
) -> float:
    if not sweeps:
        return 0.0
    if observed_hours is None:
        first_ts = min(sweep.ts_ms for sweep in sweeps)
        last_ts = max(sweep.ts_ms for sweep in sweeps)
        observed_hours = (last_ts - first_ts) / 3_600_000.0
    observed_hours = max(float(observed_hours), 1e-9)
    return simulated_sweep_fill_count(sweeps, half_spread_bps, order_notional_usd) / observed_hours


def estimate_fills_per_hour_from_current_depth_sweeps(
    sweeps: Sequence[SweepSample],
    half_spread_bps: float,
    order_notional_usd: float,
    observed_hours: Optional[float] = None,
) -> float:
    if not sweeps:
        return 0.0
    if observed_hours is None:
        first_ts = min(sweep.ts_ms for sweep in sweeps)
        last_ts = max(sweep.ts_ms for sweep in sweeps)
        observed_hours = (last_ts - first_ts) / 3_600_000.0
    observed_hours = max(float(observed_hours), 1e-9)
    return simulated_current_depth_sweep_fill_count(sweeps, half_spread_bps, order_notional_usd) / observed_hours


def estimate_fills_curve_from_sweeps(
    sweeps: Sequence[SweepSample],
    order_notional_usd: float,
    min_half_spread_bps: float,
    max_half_spread_bps: float,
    step_bps: float,
    observed_hours: Optional[float] = None,
) -> Dict[float, float]:
    return {
        half_spread_bps: estimate_fills_per_hour_from_sweeps(
            sweeps,
            half_spread_bps,
            order_notional_usd,
            observed_hours=observed_hours,
        )
        for half_spread_bps in spread_grid(min_half_spread_bps, max_half_spread_bps, step_bps)
    }


def estimate_fills_curve_from_current_depth_sweeps(
    sweeps: Sequence[SweepSample],
    order_notional_usd: float,
    min_half_spread_bps: float,
    max_half_spread_bps: float,
    step_bps: float,
    observed_hours: Optional[float] = None,
) -> Dict[float, float]:
    return {
        half_spread_bps: estimate_fills_per_hour_from_current_depth_sweeps(
            sweeps,
            half_spread_bps,
            order_notional_usd,
            observed_hours=observed_hours,
        )
        for half_spread_bps in spread_grid(min_half_spread_bps, max_half_spread_bps, step_bps)
    }


def estimate_fills_curve_by_side_from_sweeps(
    sweeps: Sequence[SweepSample],
    order_notional_usd: float,
    min_half_spread_bps: float,
    max_half_spread_bps: float,
    step_bps: float,
    observed_hours: Optional[float] = None,
) -> dict[str, Dict[float, float]]:
    return {
        "ask": {
            half_spread_bps: sum(
                sweep_fill_capacity(sweep, half_spread_bps, order_notional_usd)
                for sweep in sweeps
                if sweep.side == "buy"
            ) / max(float(observed_hours or 1.0), 1e-9)
            for half_spread_bps in spread_grid(min_half_spread_bps, max_half_spread_bps, step_bps)
        },
        "bid": {
            half_spread_bps: sum(
                sweep_fill_capacity(sweep, half_spread_bps, order_notional_usd)
                for sweep in sweeps
                if sweep.side == "sell"
            ) / max(float(observed_hours or 1.0), 1e-9)
            for half_spread_bps in spread_grid(min_half_spread_bps, max_half_spread_bps, step_bps)
        },
    }


def estimate_fills_curve_by_side_from_current_depth_sweeps(
    sweeps: Sequence[SweepSample],
    order_notional_usd: float,
    min_half_spread_bps: float,
    max_half_spread_bps: float,
    step_bps: float,
    observed_hours: Optional[float] = None,
) -> dict[str, Dict[float, float]]:
    return {
        "ask": {
            half_spread_bps: sum(
                sweep_fill_capacity(sweep, half_spread_bps, order_notional_usd, current_depth=True)
                for sweep in sweeps
                if sweep.side == "buy"
            ) / max(float(observed_hours or 1.0), 1e-9)
            for half_spread_bps in spread_grid(min_half_spread_bps, max_half_spread_bps, step_bps)
        },
        "bid": {
            half_spread_bps: sum(
                sweep_fill_capacity(sweep, half_spread_bps, order_notional_usd, current_depth=True)
                for sweep in sweeps
                if sweep.side == "sell"
            ) / max(float(observed_hours or 1.0), 1e-9)
            for half_spread_bps in spread_grid(min_half_spread_bps, max_half_spread_bps, step_bps)
        },
    }


def load_market_trades_from_sqlite(
    db_path: str | Path,
    coin: str,
    lookback_s: float,
    now_ms: Optional[int] = None,
) -> list[TradeSample]:
    now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    cutoff_ms = now_ms - int(float(lookback_s) * 1000)
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT side, price, size, timestamp_ms
            FROM market_trades
            WHERE coin = ? AND timestamp_ms >= ?
            ORDER BY timestamp_ms
            """,
            (normalize_market_name(coin), cutoff_ms),
        ).fetchall()
    return [
        TradeSample(ts_ms=int(timestamp_ms), side=str(side).lower(), price=float(price), size=float(size))
        for side, price, size, timestamp_ms in rows
        if timestamp_ms is not None
    ]


def load_trade_fair_samples_from_sqlite(
    db_path: str | Path,
    coin: str,
    lookback_s: float,
    now_ms: Optional[int] = None,
) -> list[TradeFairSample]:
    now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    cutoff_ms = now_ms - int(float(lookback_s) * 1000)
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT t.side, t.price, t.size, t.timestamp_ms,
                   s.mid_price, s.diagnostics_json
            FROM market_trades AS t
            LEFT JOIN market_snapshots AS s
              ON s.snapshot_id = (
                SELECT snapshot_id
                FROM market_snapshots
                WHERE coin = t.coin AND captured_at_ms <= t.timestamp_ms
                ORDER BY captured_at_ms DESC
                LIMIT 1
              )
            WHERE t.coin = ? AND t.timestamp_ms >= ?
            ORDER BY t.timestamp_ms
            """,
            (normalize_market_name(coin), cutoff_ms),
        ).fetchall()
    out: list[TradeFairSample] = []
    for side, price, size, timestamp_ms, mid_price, diagnostics_json in rows:
        if timestamp_ms is None:
            continue
        trade = TradeSample(ts_ms=int(timestamp_ms), side=str(side).lower(), price=float(price), size=float(size))
        fair_price = diagnostic_fair_price(diagnostics_json, mid_price)
        if fair_price > 0:
            out.append(TradeFairSample(trade, fair_price))
    return out


def load_sweep_samples_from_sqlite(
    db_path: str | Path,
    coin: str,
    lookback_s: float,
    sweep_group_ms: float,
    depth_within_mid_pct: float,
    now_ms: Optional[int] = None,
) -> list[SweepSample]:
    now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    cutoff_ms = now_ms - int(float(lookback_s) * 1000)
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT t.side, t.price, t.size, t.timestamp_ms,
                   s.captured_at_ms, s.mid_price, s.diagnostics_json,
                   s.bids_json, s.asks_json
            FROM market_trades AS t
            LEFT JOIN market_snapshots AS s
              ON s.snapshot_id = (
                SELECT snapshot_id
                FROM market_snapshots
                WHERE coin = t.coin AND captured_at_ms <= t.timestamp_ms
                ORDER BY captured_at_ms DESC
                LIMIT 1
              )
            WHERE t.coin = ? AND t.timestamp_ms >= ?
            ORDER BY t.timestamp_ms
            """,
            (normalize_market_name(coin), cutoff_ms),
        ).fetchall()
    trade_books: list[tuple[TradeSample, BookSnapshotSample]] = []
    for side, price, size, timestamp_ms, captured_at_ms, mid_price, diagnostics_json, bids_json, asks_json in rows:
        if timestamp_ms is None or captured_at_ms is None:
            continue
        trade = TradeSample(ts_ms=int(timestamp_ms), side=str(side).lower(), price=float(price), size=float(size))
        fair_price = diagnostic_fair_price(diagnostics_json, mid_price)
        try:
            bids = json.loads(bids_json or "[]")
            asks = json.loads(asks_json or "[]")
        except Exception:
            continue
        book = book_snapshot_from_objects(
            int(captured_at_ms),
            fair_price,
            bids,
            asks,
            within_mid_pct=float(depth_within_mid_pct),
        )
        if book is not None:
            trade_books.append((trade, book))
    return grouped_sweeps_from_trade_books(trade_books, sweep_group_ms)
