from __future__ import annotations

import unittest

from connectors.base import OrderBookLevel
from scripts.orderbook import (
    BookResolution,
    MultiResolutionOrderBook,
    calculate_lob_guard,
    outward_level_prices,
    percentile_price,
)


def level(price: float, size: float) -> OrderBookLevel:
    return OrderBookLevel(price=price, size=size, order_count=1, raw={})


class OrderBookTest(unittest.TestCase):
    def test_percentile_guard_sits_one_step_in_front_of_depth(self) -> None:
        bids = [level(99.90, 0.5), level(99.80, 99.5)]
        asks = [level(100.10, 0.5), level(100.20, 99.5)]
        guard = calculate_lob_guard(bids, asks, 100.0, 0.01, 0.01, 50, 0.01)
        self.assertAlmostEqual(guard.bid_price or 0.0, 99.90)
        self.assertAlmostEqual(guard.ask_price or 0.0, 100.10)

    def test_percentile_uses_cumulative_visible_size(self) -> None:
        bids = [level(99.90, 2.0), level(99.80, 98.0)]
        self.assertEqual(percentile_price(bids, 0.01, is_bid=True), 99.90)

    def test_outward_levels_move_away_from_mid(self) -> None:
        self.assertEqual(outward_level_prices("buy", 100.0, 2, 2.0, 0.01), [100.0, 99.98])
        self.assertEqual(outward_level_prices("sell", 100.0, 2, 2.0, 0.01), [100.0, 100.02])

    def test_hyperliquid_rejects_unsupported_sig1_resolution(self) -> None:
        with self.assertRaisesRegex(ValueError, "nSigFigs"):
            BookResolution(1)

    def test_multi_resolution_book_extends_depth_without_overlapping_fine_boundary(self) -> None:
        book = MultiResolutionOrderBook(
            coin="BTC",
            resolutions=[BookResolution(None), BookResolution(4), BookResolution(3)],
            priority=["raw", "sig4", "sig3"],
            stale_s=3.0,
            price_step_fallback=1.0,
        )
        book.update("raw", [level(100.0, 1.0), level(99.0, 1.0)], [level(101.0, 1.0), level(102.0, 1.0)])
        book.update("sig4", [level(100.0, 10.0), level(90.0, 10.0), level(80.0, 10.0)], [level(110.0, 10.0), level(120.0, 10.0), level(130.0, 10.0)])
        book.update("sig3", [level(100.0, 100.0), level(0.0, 100.0)], [level(200.0, 100.0), level(300.0, 100.0)])

        snapshot = book.snapshot()

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual([item.price for item in snapshot.bids], [100.0, 99.0, 80.0])
        self.assertEqual([item.price for item in snapshot.asks], [101.0, 102.0, 120.0, 130.0, 300.0])
        self.assertEqual(snapshot.synthetic_source_counts["bids"], {"raw": 2, "sig4": 1})
        self.assertEqual(snapshot.synthetic_source_counts["asks"], {"raw": 2, "sig4": 2, "sig3": 1})

    def test_multi_resolution_book_uses_faster_bbo_for_top_level(self) -> None:
        book = MultiResolutionOrderBook(
            coin="BTC",
            resolutions=[BookResolution(None)],
            priority=["raw"],
            stale_s=3.0,
            price_step_fallback=1.0,
        )
        book.update("raw", [level(100.0, 1.0), level(99.0, 1.0)], [level(101.0, 1.0), level(102.0, 1.0)])
        book.update_bbo(level(100.5, 2.0), level(100.8, 3.0))

        snapshot = book.snapshot()

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.bids[0].price, 100.5)
        self.assertEqual(snapshot.asks[0].price, 100.8)
        self.assertEqual(snapshot.bids[0].raw["syntheticBookSource"], "bbo")
        self.assertEqual(snapshot.asks[0].raw["syntheticBookSource"], "bbo")


if __name__ == "__main__":
    unittest.main()
