"""Compatibility imports for the old Hyperliquid module path."""

from connectors.base import OpenOrder as NormalizedOrder
from connectors.base import OrderResult, TradeFill
from connectors.hyperliquid_connector import HyperliquidConnector as HyperliquidMain

__all__ = ["HyperliquidMain", "NormalizedOrder", "OrderResult", "TradeFill"]
