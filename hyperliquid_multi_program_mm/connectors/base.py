from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class OpenOrder:
    exchange: str
    market: str
    symbol: str
    exchange_order_id: Optional[str]
    client_order_id: Optional[str]
    side: str
    price: float
    size: float
    timestamp_ms: Optional[int]
    raw: Dict[str, Any]

    @property
    def coin(self) -> str:
        return self.market

    @property
    def oid(self) -> Optional[str]:
        return self.exchange_order_id

    @property
    def cloid(self) -> Optional[str]:
        return self.client_order_id


@dataclass
class OrderResult:
    exchange: str
    market: str
    symbol: str
    side: str
    price: float
    size: float
    client_order_id: str
    exchange_order_id: Optional[str]
    status: str
    raw: Dict[str, Any]

    @property
    def coin(self) -> str:
        return self.market

    @property
    def oid(self) -> Optional[str]:
        return self.exchange_order_id

    @property
    def cloid(self) -> str:
        return self.client_order_id


@dataclass
class TradeFill:
    exchange: str
    market: str
    symbol: str
    exchange_order_id: Optional[str]
    client_order_id: Optional[str]
    trade_id: str
    side: str
    price: float
    size: float
    fee: float
    fee_currency: Optional[str]
    timestamp_ms: Optional[int]
    raw: Dict[str, Any]


@dataclass
class MarketTrade:
    exchange: str
    market: str
    trade_id: str
    side: str
    price: float
    size: float
    timestamp_ms: Optional[int]
    raw: Dict[str, Any]


@dataclass
class OrderBookLevel:
    price: float
    size: float
    order_count: Optional[int]
    raw: Dict[str, Any]


@dataclass
class OrderEdit:
    order: OpenOrder
    price: float
    size: float
    reduce_only: bool = False
    post_only: bool = True


class ExchangeConnector(ABC):
    """Stable strategy-facing API implemented once per exchange."""

    name: str
    account_address: str

    @property
    @abstractmethod
    def signer_address(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def post_only_time_in_force(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def symbol_for_market(self, market: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def make_client_order_id(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def fetch_bbo(self, market: str) -> Tuple[float, float, float]:
        raise NotImplementedError

    @abstractmethod
    def fetch_order_book(
        self, market: str, depth: Optional[int] = None
    ) -> Tuple[List[OrderBookLevel], List[OrderBookLevel]]:
        raise NotImplementedError

    def fetch_cached_order_book(
        self, market: str, depth: Optional[int] = None
    ) -> Tuple[List[OrderBookLevel], List[OrderBookLevel]]:
        return self.fetch_order_book(market, depth)

    def fetch_cached_market_trades(self, market: str, since_timestamp_ms: int = 0) -> List[MarketTrade]:
        return []

    def fetch_recent_market_trades(self, market: str, limit: int = 2000) -> List[MarketTrade]:
        return []

    def fetch_last_cached_market_trade(self, market: str) -> Optional[MarketTrade]:
        trades = self.fetch_cached_market_trades(market)
        return trades[-1] if trades else None

    def fetch_order_book_cache_metadata(self, market: str) -> Dict[str, Any]:
        return {}

    @abstractmethod
    def fetch_open_orders(self, market: Optional[str] = None, *, force: bool = False) -> List[OpenOrder]:
        raise NotImplementedError

    @abstractmethod
    def fetch_position_size(self, market: str) -> Tuple[float, Dict[str, Any]]:
        raise NotImplementedError

    def fetch_inventory_balance(self, market: str) -> Tuple[float, Dict[str, Any]]:
        """Return exchange-reported inventory used by strategies for risk decisions."""
        return self.fetch_position_size(market)

    def fetch_inventory_balances(self, markets: List[str]) -> Dict[str, Tuple[float, Dict[str, Any]]]:
        return {market.upper(): self.fetch_inventory_balance(market) for market in markets}

    @abstractmethod
    def fetch_positions(self) -> List[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def fetch_recent_fills(
        self,
        start_ms: int,
        end_ms: Optional[int] = None,
        aggregate_by_time: bool = False,
    ) -> List[TradeFill]:
        raise NotImplementedError

    @abstractmethod
    def fetch_account_summary(self) -> Dict[str, Any]:
        raise NotImplementedError

    def fetch_spot_balances(self) -> List[Dict[str, Any]]:
        return []

    def fetch_open_order_details(self) -> List[Dict[str, Any]]:
        return []

    def fetch_signer_role(self) -> Dict[str, Any]:
        return {}

    def fetch_user_rate_limit(self) -> Dict[str, Any]:
        return {}

    def start_background_streams(self, markets: List[str]) -> None:
        return None

    def stop_background_streams(self) -> None:
        return None

    def wait_for_market_data_update(self, previous_sequence: int, timeout_s: float) -> int:
        time.sleep(max(float(timeout_s), 0.0))
        return previous_sequence

    def fetch_cached_open_orders(self, market: Optional[str] = None) -> List[OpenOrder]:
        return self.fetch_open_orders(market)

    def refresh_market_metadata(self) -> None:
        return None

    @abstractmethod
    def fetch_market_max_leverage(self, market: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def set_market_leverage(self, market: str, leverage: int, *, is_cross: bool = True) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def round_size(self, market: str, size: float) -> float:
        raise NotImplementedError

    @abstractmethod
    def round_size_up(self, market: str, size: float) -> float:
        raise NotImplementedError

    @abstractmethod
    def round_price(self, market: str, side: str, price: float) -> float:
        raise NotImplementedError

    @abstractmethod
    def price_step(self, market: str, price: float) -> float:
        raise NotImplementedError

    @abstractmethod
    def place_limit_order(
        self,
        market: str,
        side: str,
        size: float,
        price: float,
        *,
        reduce_only: bool = False,
        post_only: bool = True,
        client_order_id: Optional[str] = None,
    ) -> OrderResult:
        raise NotImplementedError

    @abstractmethod
    def cancel_order(
        self,
        market: str,
        *,
        client_order_id: Optional[str] = None,
        exchange_order_id: Optional[str] = None,
    ) -> Tuple[bool, Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def bulk_edit_orders(self, edits: List[OrderEdit]) -> List[OrderResult]:
        raise NotImplementedError

    def cancel_all_open_orders_for_market(self, market: str) -> None:
        for order in self.fetch_open_orders(market):
            self.cancel_order(
                market,
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id,
            )

    def wait_until_order_gone(
        self,
        market: str,
        client_order_id: Optional[str],
        exchange_order_id: Optional[str],
        timeout_s: float,
        poll_s: float,
    ) -> bool:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if not self.order_is_open(market, client_order_id, exchange_order_id):
                return True
            time.sleep(poll_s)
        return not self.order_is_open(market, client_order_id, exchange_order_id)

    def order_is_open(
        self,
        market: str,
        client_order_id: Optional[str],
        exchange_order_id: Optional[str],
    ) -> bool:
        for order in self.fetch_open_orders(market):
            if (
                exchange_order_id is not None
                and order.exchange_order_id is not None
                and str(order.exchange_order_id) == str(exchange_order_id)
            ):
                return True
            if (
                client_order_id
                and order.client_order_id
                and order.client_order_id.lower() == client_order_id.lower()
            ):
                return True
        return False
