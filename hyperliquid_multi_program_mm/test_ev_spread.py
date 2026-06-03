from __future__ import annotations

import math
import unittest

from connectors.base import OrderBookLevel
from scripts.ev_spread import (
    build_ev_spread_curve,
    build_ev_surface,
    choose_ev_half_spread_bps,
    ev_per_hour,
    spread_grid,
)
from scripts.lob_vwap import LobVwapWindow, side_vwap
from scripts.trade_expectancy import (
    BookLevelSample,
    BookSnapshotSample,
    SweepSample,
    TradeFairSample,
    TradeSample,
    current_depth_sweep_would_fill,
    estimate_fills_curve,
    estimate_fills_curve_by_side_from_current_depth_sweeps,
    estimate_fills_curve_from_current_depth_sweeps,
    estimate_fills_curve_by_side_from_sweeps,
    estimate_fills_curve_from_trade_fairs,
    estimate_fills_curve_from_sweeps,
    estimate_fills_per_hour_from_trades,
    sweep_would_fill,
)


class EvSpreadTest(unittest.TestCase):
    def test_spread_grid_and_ev_surface_are_granular(self) -> None:
        self.assertEqual(spread_grid(1.0, 5.0, 1.0), [1.0, 2.0, 3.0, 4.0, 5.0])
        surface = build_ev_surface(
            order_notional_usd=10.0,
            maker_fee_bps_per_side=3.0,
            min_half_spread_bps=1.0,
            max_half_spread_bps=50.0,
            half_spread_step_bps=1.0,
            min_trades_per_hour=1.0,
            max_trades_per_hour=200.0,
            trades_per_hour_step=1.0,
        )
        self.assertEqual(len(surface), 50 * 200)
        self.assertAlmostEqual(ev_per_hour(80.0, 10.0, 5.0, 3.0), 0.16)

    def test_trade_expectancy_uses_real_crosses_not_free_trade_count(self) -> None:
        trades = [
            TradeSample(0, "buy", 100.03, 1.0),
            TradeSample(900_000, "sell", 99.98, 1.0),
            TradeSample(1_800_000, "buy", 100.20, 1.0),
            TradeSample(2_700_000, "sell", 99.70, 1.0),
            TradeSample(3_600_000, "buy", 100.01, 1.0),
        ]
        one_bps = estimate_fills_per_hour_from_trades(trades, 100.0, 1.0)
        ten_bps = estimate_fills_per_hour_from_trades(trades, 100.0, 10.0)

        self.assertGreater(one_bps, ten_bps)
        self.assertAlmostEqual(one_bps, 5.0)
        self.assertAlmostEqual(ten_bps, 2.0)

    def test_choose_ev_spread_uses_estimated_fill_curve(self) -> None:
        estimated = estimate_fills_curve(
            [
                TradeSample(0, "buy", 100.03, 1.0),
                TradeSample(900_000, "sell", 99.98, 1.0),
                TradeSample(1_800_000, "buy", 100.20, 1.0),
                TradeSample(2_700_000, "sell", 99.70, 1.0),
                TradeSample(3_600_000, "buy", 100.01, 1.0),
            ],
            fair_price=100.0,
            min_half_spread_bps=1.0,
            max_half_spread_bps=20.0,
            step_bps=1.0,
        )
        curve = build_ev_spread_curve(
            estimated,
            order_notional_usd=10.0,
            maker_fee_bps_per_side=1.5,
            min_half_spread_bps=1.0,
            max_half_spread_bps=20.0,
            step_bps=1.0,
            max_trades_per_hour=200.0,
        )
        choice = choose_ev_half_spread_bps(curve)

        self.assertIsNotNone(choice)
        assert choice is not None
        self.assertEqual(choice.half_spread_bps, 20.0)
        self.assertLess(choice.estimated_fills_per_hour, estimated[1.0])
        self.assertGreater(choice.ev_per_hour, 0.0)

    def test_dynamic_fair_samples_prevent_trend_from_becoming_fake_fills(self) -> None:
        samples = [
            TradeFairSample(TradeSample(0, "buy", 100.03, 1.0), 100.0),
            TradeFairSample(TradeSample(1_800_000, "buy", 105.03, 1.0), 105.0),
            TradeFairSample(TradeSample(3_600_000, "sell", 109.97, 1.0), 110.0),
        ]

        curve = estimate_fills_curve_from_trade_fairs(samples, 1.0, 50.0, 1.0)

        self.assertEqual(curve[1.0], 3.0)
        self.assertEqual(curve[50.0], 0.0)

    def test_sweep_depth_requires_enough_notional_to_clear_book_ahead(self) -> None:
        book = BookSnapshotSample(
            ts_ms=0,
            fair_price=100.0,
            bids=(
                BookLevelSample(99.95, 20.0),
                BookLevelSample(99.90, 30.0),
            ),
            asks=(
                BookLevelSample(100.05, 20.0),
                BookLevelSample(100.10, 30.0),
            ),
        )
        small_sweep = SweepSample(0, "buy", 2500.0, 100.05, 100.12, 1, book)
        large_sweep = SweepSample(0, "buy", 5100.0, 100.05, 100.12, 1, book)

        self.assertFalse(sweep_would_fill(small_sweep, 11.0, order_notional_usd=10.0))
        self.assertTrue(sweep_would_fill(large_sweep, 11.0, order_notional_usd=10.0))
        self.assertFalse(sweep_would_fill(large_sweep, 20.0, order_notional_usd=10.0))

    def test_current_depth_ev_uses_sweep_notional_not_historical_trade_price(self) -> None:
        book = BookSnapshotSample(
            ts_ms=0,
            fair_price=100.0,
            bids=(BookLevelSample(99.95, 20.0), BookLevelSample(99.90, 30.0)),
            asks=(BookLevelSample(100.05, 20.0), BookLevelSample(100.10, 30.0), BookLevelSample(100.15, 30.0)),
        )
        old_price_did_not_reach_candidate = SweepSample(0, "buy", 5100.0, 100.01, 100.01, 1, book)

        self.assertFalse(sweep_would_fill(old_price_did_not_reach_candidate, 11.0, order_notional_usd=10.0))
        self.assertTrue(current_depth_sweep_would_fill(old_price_did_not_reach_candidate, 11.0, order_notional_usd=10.0))

        curve = estimate_fills_curve_from_current_depth_sweeps(
            [old_price_did_not_reach_candidate],
            order_notional_usd=10.0,
            min_half_spread_bps=1.0,
            max_half_spread_bps=20.0,
            step_bps=1.0,
            observed_hours=1.0,
        )

        self.assertGreater(curve[11.0], 0.0)
        self.assertEqual(curve[20.0], 0.0)

    def test_ev_book_snapshot_keeps_only_levels_within_point_one_percent(self) -> None:
        from scripts.trade_expectancy import book_snapshot_from_objects

        book = book_snapshot_from_objects(
            ts_ms=0,
            fair_price=100.0,
            bids=[
                {"price": 99.95, "size": 2.0},
                {"price": 99.0, "size": 9999.0},
            ],
            asks=[
                {"price": 100.05, "size": 3.0},
                {"price": 101.0, "size": 9999.0},
            ],
            within_mid_pct=0.001,
        )

        self.assertIsNotNone(book)
        assert book is not None
        self.assertEqual([level.price for level in book.bids], [99.95])
        self.assertEqual([level.price for level in book.asks], [100.05])

    def test_sweep_curve_counts_sweeps_not_each_public_fill_print(self) -> None:
        book = BookSnapshotSample(
            ts_ms=0,
            fair_price=100.0,
            bids=(BookLevelSample(99.95, 20.0),),
            asks=(BookLevelSample(100.05, 20.0),),
        )
        sweeps = [
            SweepSample(0, "buy", 3000.0, 100.05, 100.10, 5, book),
            SweepSample(3_600_000, "sell", 3000.0, 99.90, 99.95, 4, book),
        ]

        curve = estimate_fills_curve_from_sweeps(sweeps, 10.0, 1.0, 20.0, 1.0)

        self.assertGreater(curve[5.0], 2.0)
        self.assertEqual(curve[20.0], 0.0)

    def test_lob_vwap_only_uses_levels_inside_mid_band(self) -> None:
        bids = [
            OrderBookLevel(99.95, 2.0, 1, {}),
            OrderBookLevel(98.0, 1000.0, 1, {}),
        ]
        asks = [
            OrderBookLevel(100.05, 4.0, 1, {}),
            OrderBookLevel(102.0, 1000.0, 1, {}),
        ]
        bid_vwap, bid_size = side_vwap(bids[:1])
        self.assertAlmostEqual(bid_vwap or 0.0, 99.95)
        self.assertAlmostEqual(bid_size, 2.0)

        window = LobVwapWindow(window_s=2.0)
        fair = window.add_from_book(bids, asks, 100.0, within_mid_pct=0.001, max_levels=50, ts=1.0)

        self.assertTrue(math.isclose(fair or 0.0, 100.0))

    def test_estimate_fills_curve_by_side_from_sweeps_uses_all_trade_volume_for_capacity(self) -> None:
        book = BookSnapshotSample(
            ts_ms=0,
            fair_price=100.0,
            bids=(BookLevelSample(99.95, 20.0), BookLevelSample(99.90, 30.0)),
            asks=(BookLevelSample(100.05, 20.0), BookLevelSample(100.10, 30.0), BookLevelSample(100.15, 30.0)),
        )
        buy_sweep = SweepSample(0, "buy", 15000.0, 100.05, 100.15, 10, book)

        curves = estimate_fills_curve_by_side_from_sweeps(
            [buy_sweep],
            order_notional_usd=10.0,
            min_half_spread_bps=10.0,
            max_half_spread_bps=20.0,
            step_bps=1.0,
            observed_hours=1.0,
        )

        self.assertGreater(curves["ask"][11.0], 1.0)
        self.assertEqual(curves["bid"][11.0], 0.0)


if __name__ == "__main__":
    unittest.main()
