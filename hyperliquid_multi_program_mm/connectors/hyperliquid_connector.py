from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import websocket
from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants
from hyperliquid.utils.types import Cloid

import settings
from common import (
    ceil_to_step,
    coin_to_symbol,
    floor_to_step,
    is_benign_cancel_error,
    json_dumps,
    make_cloid,
    normalize_market_name,
    price_to_step_by_side,
    safe_float,
    safe_int,
)
from connectors.base import ExchangeConnector, MarketTrade, OpenOrder, OrderBookLevel, OrderEdit, OrderResult, TradeFill
from scripts.orderbook import BookResolution, MultiResolutionOrderBook


class HyperliquidConnector(ExchangeConnector):
    """Hyperliquid implementation of the strategy-facing exchange API."""

    name = "hyperliquid"

    def __init__(self) -> None:
        self.log = logging.getLogger("HyperliquidConnector")
        self.base_url = constants.MAINNET_API_URL
        self.account_address = settings.WALLET_ADDRESS
        if not self.account_address.startswith("0x") or "PASTE" in self.account_address:
            raise RuntimeError("Edit settings.py and paste the Hyperliquid main-account WALLET_ADDRESS.")
        if not settings.PRIVATE_KEY.startswith("0x") or "PASTE" in settings.PRIVATE_KEY:
            raise RuntimeError("Edit settings.py and paste the approved Hyperliquid API-wallet PRIVATE_KEY.")
        self.wallet = Account.from_key(settings.PRIVATE_KEY)
        self.info = Info(self.base_url, skip_ws=True)
        self.perp_dex_names = self._load_perp_dex_names(self.info)
        if any(self.perp_dex_names):
            self.info = Info(self.base_url, skip_ws=True, perp_dexs=self.perp_dex_names)
        self._validate_account_configuration()
        # account_address lets an approved API wallet trade for the main account.
        self.sdk_exchange = Exchange(
            self.wallet,
            self.base_url,
            account_address=self.account_address,
            perp_dexs=self.perp_dex_names,
        )
        self.meta = self.info.meta()
        self.market_to_sz_decimals = self._build_market_to_sz_decimals()
        self.market_to_max_leverage = self._build_market_to_max_leverage()
        self.market_aliases = self._build_market_aliases()
        self._open_orders_cache: List[OpenOrder] = []
        self._open_orders_last_fetch = 0.0
        self._open_orders_lock = threading.RLock()
        self._order_book_cache: Dict[str, Tuple[List[OrderBookLevel], List[OrderBookLevel], float]] = {}
        self._order_book_exchange_time_ms: Dict[str, Optional[int]] = {}
        self._order_book_metadata: Dict[str, Dict[str, Any]] = {}
        self._multi_resolution_books: Dict[str, MultiResolutionOrderBook] = {}
        self._order_book_lock = threading.RLock()
        self._market_trades_cache: Dict[str, List[MarketTrade]] = {}
        self._market_trades_seen: Dict[str, set[str]] = {}
        self._market_trades_lock = threading.RLock()
        self._stream_markets: List[str] = []
        self._streams_stop = threading.Event()
        self._streams_thread: Optional[threading.Thread] = None
        self._streams_ws: Optional[websocket.WebSocketApp] = None
        self._resolution_streams_threads: Dict[str, threading.Thread] = {}
        self._resolution_streams_ws: Dict[str, websocket.WebSocketApp] = {}
        self._market_data_condition = threading.Condition()
        self._market_data_sequence = 0
        self._exchange_action_lock = threading.Lock()

    @property
    def signer_address(self) -> str:
        return self.wallet.address

    @property
    def post_only_time_in_force(self) -> str:
        return settings.POST_ONLY_TIF

    def symbol_for_market(self, market: str) -> str:
        return coin_to_symbol(self.normalize_market(market))

    def make_client_order_id(self) -> str:
        return make_cloid()

    def _validate_account_configuration(self) -> None:
        configured_role = self.info.user_role(self.account_address)
        if isinstance(configured_role, dict) and configured_role.get("role") == "agent":
            main_address = ((configured_role.get("data") or {}).get("user") or "").strip()
            raise RuntimeError(
                "WALLET_ADDRESS is an API agent wallet address. "
                f"Set WALLET_ADDRESS to the main Hyperliquid account address: {main_address}"
            )

        signer_role = self.info.user_role(self.wallet.address)
        if isinstance(signer_role, dict) and signer_role.get("role") == "agent":
            main_address = ((signer_role.get("data") or {}).get("user") or "").strip()
            if not main_address or main_address.lower() != self.account_address.lower():
                raise RuntimeError(
                    "PRIVATE_KEY belongs to an API agent wallet approved for a different main account. "
                    f"Set WALLET_ADDRESS to: {main_address}"
                )

    @staticmethod
    def _load_perp_dex_names(info: Info) -> List[str]:
        raw = info.perp_dexs()
        out: List[str] = []
        if isinstance(raw, list):
            for item in raw:
                if item is None:
                    out.append("")
                elif isinstance(item, dict):
                    out.append(str(item.get("name") or ""))
        if "" not in out:
            out.insert(0, "")
        return list(dict.fromkeys(out))

    def _perp_dex_names(self) -> List[str]:
        names = getattr(self, "perp_dex_names", None)
        if names:
            return list(names)
        self.perp_dex_names = self._load_perp_dex_names(self.info)
        return list(self.perp_dex_names)

    @staticmethod
    def _dex_for_market_name(market: str) -> str:
        market = normalize_market_name(market)
        if ":" not in market:
            return ""
        return market.split(":", 1)[0]

    @staticmethod
    def _meta_market_name(perp_dex: str, item: Dict[str, Any]) -> str:
        name = str(item.get("name") or "").strip()
        if not name:
            return ""
        if perp_dex and ":" not in name:
            name = f"{perp_dex}:{name}"
        return normalize_market_name(name)

    def _iter_perp_universe_items(self) -> List[Tuple[str, Dict[str, Any]]]:
        items: List[Tuple[str, Dict[str, Any]]] = []
        for perp_dex in self._perp_dex_names():
            try:
                meta = self.info.meta(dex=perp_dex) if perp_dex else self.info.meta()
            except Exception:
                continue
            if not isinstance(meta, dict):
                continue
            universe = meta.get("universe")
            if not isinstance(universe, list):
                continue
            for item in universe:
                if isinstance(item, dict):
                    items.append((perp_dex, item))
        return items

    def _build_market_aliases(self) -> Dict[str, str]:
        aliases: Dict[str, str] = {}
        for perp_dex, item in self._iter_perp_universe_items():
            name = self._meta_market_name(perp_dex, item)
            if not name or item.get("isDelisted"):
                continue
            aliases[name] = name
            aliases[normalize_market_name(name)] = name
            if ":" in name:
                dex, coin = name.split(":", 1)
                aliases[normalize_market_name(f"{dex}:{coin}-USDC")] = name
                aliases[f"{dex}:{coin}"] = name
                aliases[normalize_market_name(f"{dex}:{coin.upper()}")] = name
                aliases[normalize_market_name(f"{dex}:{coin.upper()}-USDC")] = name
            else:
                aliases[normalize_market_name(f"{name}-USDC")] = name
                aliases[name.upper()] = name
                aliases[normalize_market_name(f"{name.upper()}-USDC")] = name
        return aliases

    def normalize_market(self, market: str) -> str:
        candidate = normalize_market_name(market)
        aliases = getattr(self, "market_aliases", {})
        return aliases.get(candidate, candidate)

    def _dexs_for_market_filter(self, market: Optional[str] = None) -> List[str]:
        if market:
            return [self._dex_for_market_name(self.normalize_market(market))]
        configured = getattr(settings, "MARKETS", getattr(settings, "COINS", []))
        out = {""}
        for configured_market in configured:
            out.add(self._dex_for_market_name(self.normalize_market(str(configured_market))))
        return [""] + sorted(dex for dex in out if dex)

    def _build_market_to_sz_decimals(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for perp_dex, item in self._iter_perp_universe_items():
            name = self._meta_market_name(perp_dex, item)
            if not name or item.get("isDelisted"):
                continue
            out[name] = int(item.get("szDecimals", 5))
        return out

    def _build_market_to_max_leverage(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for perp_dex, item in self._iter_perp_universe_items():
            name = self._meta_market_name(perp_dex, item)
            max_leverage = safe_int(item.get("maxLeverage"), 0)
            if not name or item.get("isDelisted") or max_leverage <= 0:
                continue
            out[name] = max_leverage
        return out

    def refresh_market_metadata(self) -> None:
        self.meta = self.info.meta()
        self.market_to_sz_decimals = self._build_market_to_sz_decimals()
        self.market_to_max_leverage = self._build_market_to_max_leverage()
        self.market_aliases = self._build_market_aliases()

    def fetch_market_max_leverage(self, market: str) -> int:
        market = self.normalize_market(market)
        leverage = self.market_to_max_leverage.get(market, 0)
        if leverage <= 0:
            self.refresh_market_metadata()
            market = self.normalize_market(market)
            leverage = self.market_to_max_leverage.get(market, 0)
        if leverage <= 0:
            raise RuntimeError(f"missing max leverage metadata market={market}")
        return leverage

    def set_market_leverage(self, market: str, leverage: int, *, is_cross: bool = True) -> Dict[str, Any]:
        market = self.normalize_market(market)
        max_leverage = self.fetch_market_max_leverage(market)
        leverage = int(leverage)
        if leverage <= 0 or leverage > max_leverage:
            raise RuntimeError(f"invalid leverage market={market} leverage={leverage} max={max_leverage}")
        with self._exchange_action_lock:
            raw = self.sdk_exchange.update_leverage(leverage, market, is_cross=bool(is_cross))
        errors = self.action_response_errors(raw)
        if errors or not isinstance(raw, dict) or raw.get("status") != "ok":
            raise RuntimeError(f"set leverage rejected market={market} errors={errors} raw={raw}")
        return raw if isinstance(raw, dict) else {"raw": raw}

    def size_step(self, market: str) -> float:
        market = self.normalize_market(market)
        decimals = self.market_to_sz_decimals.get(market, 5)
        return 10.0 ** (-decimals)

    def round_size(self, market: str, size: float) -> float:
        return floor_to_step(float(size), self.size_step(market))

    def round_size_up(self, market: str, size: float) -> float:
        return ceil_to_step(float(size), self.size_step(market))

    def round_price(self, market: str, side: str, price: float) -> float:
        rounded = float(f"{float(price):.5g}")
        return price_to_step_by_side(side, rounded, 1e-6)

    def price_step(self, market: str, price: float) -> float:
        market = self.normalize_market(market)
        decimals = max(0, 6 - self.market_to_sz_decimals.get(market, 5))
        return 10.0 ** (-decimals)

    def fetch_l2_book(self, market: str) -> Dict[str, Any]:
        market = self.normalize_market(market)
        raw = self.info.post("/info", {"type": "l2Book", "coin": market})
        return raw if isinstance(raw, dict) else {}

    @staticmethod
    def _configured_book_resolutions() -> List[BookResolution]:
        resolutions = [BookResolution(None)]
        if bool(getattr(settings, "WS_MULTI_RESOLUTION_BOOK_ENABLED", False)):
            resolutions.extend(
                BookResolution(int(n_sig_figs))
                for n_sig_figs in getattr(settings, "WS_BOOK_N_SIG_FIGS", (5, 4, 3, 2))
            )
        return resolutions

    @staticmethod
    def _configured_book_priority() -> List[str]:
        configured = getattr(settings, "WS_BOOK_RESOLUTION_PRIORITY", ("raw", "sig5", "sig4", "sig3", "sig2"))
        enabled = {resolution.key for resolution in HyperliquidConnector._configured_book_resolutions()}
        return [str(key) for key in configured if str(key) in enabled]

    def _ensure_stream_state(self) -> None:
        if not hasattr(self, "_order_book_metadata"):
            self._order_book_metadata = {}
        if not hasattr(self, "_multi_resolution_books"):
            self._multi_resolution_books = {}
        if not hasattr(self, "_resolution_streams_threads"):
            self._resolution_streams_threads = {}
        if not hasattr(self, "_resolution_streams_ws"):
            self._resolution_streams_ws = {}
        if not hasattr(self, "_market_data_condition"):
            self._market_data_condition = threading.Condition()
        if not hasattr(self, "_market_data_sequence"):
            self._market_data_sequence = 0

    def _multi_resolution_book(self, market: str) -> MultiResolutionOrderBook:
        self._ensure_stream_state()
        market = self.normalize_market(market)
        book = self._multi_resolution_books.get(market)
        if book is None:
            book = MultiResolutionOrderBook(
                coin=market,
                resolutions=self._configured_book_resolutions(),
                priority=self._configured_book_priority(),
                stale_s=float(settings.WS_ORDER_BOOK_STALE_S),
                price_step_fallback=self.price_step(market, 1.0),
            )
            self._multi_resolution_books[market] = book
        return book

    def _notify_market_data_update(self) -> None:
        self._ensure_stream_state()
        with self._market_data_condition:
            self._market_data_sequence += 1
            self._market_data_condition.notify_all()

    def wait_for_market_data_update(self, previous_sequence: int, timeout_s: float) -> int:
        self._ensure_stream_state()
        with self._market_data_condition:
            if self._market_data_sequence == previous_sequence:
                self._market_data_condition.wait(max(float(timeout_s), 0.0))
            return self._market_data_sequence

    def start_background_streams(self, markets: List[str]) -> None:
        if not bool(getattr(settings, "USE_WS_MARKET_DATA", False)):
            return
        self._ensure_stream_state()
        self._stream_markets = sorted({self.normalize_market(market) for market in markets})
        if self._streams_thread is not None and self._streams_thread.is_alive():
            return
        self._streams_stop.clear()
        self._streams_thread = threading.Thread(
            target=self._run_background_streams,
            name="hyperliquid_ws_cache",
            daemon=True,
        )
        self._streams_thread.start()
        for resolution in self._configured_book_resolutions():
            if resolution.n_sig_figs is None:
                continue
            thread = threading.Thread(
                target=self._run_resolution_stream,
                args=(resolution,),
                name=f"hyperliquid_ws_cache_{resolution.key}",
                daemon=True,
            )
            self._resolution_streams_threads[resolution.key] = thread
            thread.start()

    def stop_background_streams(self) -> None:
        self._streams_stop.set()
        ws = self._streams_ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
        for resolution_ws in list(self._resolution_streams_ws.values()):
            try:
                resolution_ws.close()
            except Exception:
                pass
        thread = self._streams_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=3.0)
        for resolution_thread in list(self._resolution_streams_threads.values()):
            if resolution_thread.is_alive():
                resolution_thread.join(timeout=3.0)
        self._streams_thread = None
        self._streams_ws = None
        self._resolution_streams_threads = {}
        self._resolution_streams_ws = {}

    def _run_background_streams(self) -> None:
        ws_url = "ws" + self.base_url[len("http"):] + "/ws"
        while not self._streams_stop.is_set():
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=self._on_streams_open,
                on_message=self._on_streams_message,
                on_error=self._on_streams_error,
            )
            self._streams_ws = ws
            ws.run_forever()
            self._streams_ws = None
            if not self._streams_stop.wait(float(settings.WS_RECONNECT_DELAY_S)):
                self.log.warning("websocket cache disconnected; reconnecting")

    def _run_resolution_stream(self, resolution: BookResolution) -> None:
        ws_url = "ws" + self.base_url[len("http"):] + "/ws"
        while not self._streams_stop.is_set():
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=lambda app: self._on_resolution_stream_open(app, resolution),
                on_message=lambda app, message: self._on_resolution_stream_message(app, message, resolution),
                on_error=lambda app, error: self._on_resolution_stream_error(app, error, resolution),
            )
            self._resolution_streams_ws[resolution.key] = ws
            ws.run_forever()
            self._resolution_streams_ws.pop(resolution.key, None)
            if not self._streams_stop.wait(float(settings.WS_RECONNECT_DELAY_S)):
                self.log.warning("websocket resolution cache disconnected resolution=%s; reconnecting", resolution.key)

    def _on_streams_open(self, ws: websocket.WebSocketApp) -> None:
        for market in self._stream_markets:
            ws.send(json_dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": market}}))
            if bool(getattr(settings, "WS_BBO_STREAM_ENABLED", True)):
                ws.send(json_dumps({"method": "subscribe", "subscription": {"type": "bbo", "coin": market}}))
            ws.send(json_dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": market}}))
        ws.send(
            json_dumps(
                {
                    "method": "subscribe",
                    "subscription": {"type": "orderUpdates", "user": self.account_address},
                }
            )
        )
        self.log.info(
            "websocket cache subscriptions active markets=%s book_priority=%s",
            self._stream_markets,
            self._configured_book_priority(),
        )

    def _on_resolution_stream_open(self, ws: websocket.WebSocketApp, resolution: BookResolution) -> None:
        for market in self._stream_markets:
            ws.send(json_dumps({"method": "subscribe", "subscription": resolution.subscription(market)}))
        self.log.info("websocket resolution cache subscriptions active resolution=%s markets=%s", resolution.key, self._stream_markets)

    def _on_resolution_stream_message(
        self,
        _ws: websocket.WebSocketApp,
        message: str,
        resolution: BookResolution,
    ) -> None:
        try:
            payload = json.loads(message)
            channel = payload.get("channel") if isinstance(payload, dict) else None
            data = payload.get("data") if isinstance(payload, dict) else None
            if channel != "l2Book" or not isinstance(data, dict):
                return
            self._cache_normalized_ws_book(data, resolution.key)
        except Exception as exc:
            self.log.warning("websocket resolution cache message ignored resolution=%s error=%s", resolution.key, exc)

    def _on_resolution_stream_error(
        self,
        _ws: websocket.WebSocketApp,
        error: Any,
        resolution: BookResolution,
    ) -> None:
        if not self._streams_stop.is_set():
            self.log.warning("websocket resolution cache error resolution=%s error=%s", resolution.key, error)

    def _on_streams_message(self, _ws: websocket.WebSocketApp, message: str) -> None:
        try:
            payload = json.loads(message)
            channel = payload.get("channel") if isinstance(payload, dict) else None
            data = payload.get("data") if isinstance(payload, dict) else None
            if channel == "l2Book" and isinstance(data, dict):
                self._cache_normalized_ws_book(data, "raw")
            elif channel == "bbo" and isinstance(data, dict):
                self._cache_normalized_ws_bbo(data)
            elif channel == "trades" and isinstance(data, list):
                self._cache_market_trades(self.normalize_market_trades(data))
            elif channel == "orderUpdates" and isinstance(data, list):
                for update in data:
                    self._apply_order_update(update)
        except Exception as exc:
            self.log.warning("websocket cache message ignored error=%s", exc)

    def _on_streams_error(self, _ws: websocket.WebSocketApp, error: Any) -> None:
        if not self._streams_stop.is_set():
            self.log.warning("websocket cache error=%s", error)

    def _cache_normalized_ws_book(self, data: Dict[str, Any], resolution_key: str) -> None:
        market = self.normalize_market(str(data.get("coin") or ""))
        levels = data.get("levels")
        if not market or not isinstance(levels, list) or len(levels) < 2:
            return
        bids = self.normalize_order_book_side(levels[0] or [], None)
        asks = self.normalize_order_book_side(levels[1] or [], None)
        if bids and asks:
            self._cache_order_book(
                market,
                bids,
                asks,
                safe_int(data.get("time"), 0) or None,
                resolution_key=resolution_key,
            )

    def _cache_normalized_ws_bbo(self, data: Dict[str, Any]) -> None:
        market = self.normalize_market(str(data.get("coin") or ""))
        bbo = data.get("bbo")
        if not market or not isinstance(bbo, list) or len(bbo) < 2:
            return
        bids = self.normalize_order_book_side([bbo[0]] if bbo[0] else [], None)
        asks = self.normalize_order_book_side([bbo[1]] if bbo[1] else [], None)
        if bids and asks:
            self._cache_bbo(market, bids[0], asks[0], safe_int(data.get("time"), 0) or None)

    def _apply_order_update(self, update: Any) -> None:
        if not isinstance(update, dict):
            return
        raw_order = update.get("order")
        if not isinstance(raw_order, dict):
            return
        orders = self.normalize_open_orders([raw_order])
        if not orders:
            return
        order = orders[0]
        status = str(update.get("status") or "").lower()
        order.raw = {**raw_order, "wsStatus": status, "statusTimestamp": update.get("statusTimestamp")}
        if status == "open":
            self._upsert_cached_order(order)
        else:
            self._remove_cached_order(order.market, order.client_order_id, order.exchange_order_id)
        self._notify_market_data_update()

    def _cache_order_book(
        self,
        market: str,
        bids: List[OrderBookLevel],
        asks: List[OrderBookLevel],
        exchange_timestamp_ms: Optional[int] = None,
        *,
        resolution_key: str = "raw",
    ) -> None:
        with self._order_book_lock:
            book = self._multi_resolution_book(market)
            book.update(resolution_key, bids, asks, exchange_timestamp_ms)
            self._refresh_synthetic_order_book_cache(market)
        self._notify_market_data_update()

    def _cache_bbo(
        self,
        market: str,
        bid: OrderBookLevel,
        ask: OrderBookLevel,
        exchange_timestamp_ms: Optional[int] = None,
    ) -> None:
        with self._order_book_lock:
            book = self._multi_resolution_book(market)
            book.update_bbo(bid, ask, exchange_timestamp_ms)
            self._refresh_synthetic_order_book_cache(market)
        self._notify_market_data_update()

    def _refresh_synthetic_order_book_cache(self, market: str) -> None:
        self._ensure_stream_state()
        market = self.normalize_market(market)
        book = self._multi_resolution_books.get(market)
        if book is None:
            return
        snapshot = book.snapshot()
        if snapshot is None:
            self._order_book_cache.pop(market, None)
            self._order_book_exchange_time_ms.pop(market, None)
            self._order_book_metadata.pop(market, None)
            return
        self._order_book_cache[market] = (snapshot.bids, snapshot.asks, snapshot.received_at)
        self._order_book_exchange_time_ms[market] = snapshot.exchange_timestamp_ms
        self._order_book_metadata[market] = {
            "synthetic": True,
            "resolution_level_counts": snapshot.resolution_level_counts,
            "synthetic_source_counts": snapshot.synthetic_source_counts,
            "bbo_age_ms": snapshot.bbo_age_ms,
        }

    def _cached_order_book(
        self,
        market: str,
        depth: Optional[int],
    ) -> Optional[Tuple[List[OrderBookLevel], List[OrderBookLevel]]]:
        with self._order_book_lock:
            self._refresh_synthetic_order_book_cache(market)
            cached = self._order_book_cache.get(self.normalize_market(market))
        if cached is None:
            return None
        bids, asks, received_at = cached
        if time.time() - received_at > float(settings.WS_ORDER_BOOK_STALE_S):
            return None
        return self._limit_order_book_depth(bids, asks, depth)

    @staticmethod
    def _limit_order_book_depth(
        bids: List[OrderBookLevel],
        asks: List[OrderBookLevel],
        depth: Optional[int],
    ) -> Tuple[List[OrderBookLevel], List[OrderBookLevel]]:
        if depth is None:
            return list(bids), list(asks)
        return list(bids[:depth]), list(asks[:depth])

    def fetch_order_book(
        self, market: str, depth: Optional[int] = None
    ) -> Tuple[List[OrderBookLevel], List[OrderBookLevel]]:
        market = self.normalize_market(market)
        cached = self._cached_order_book(market, depth)
        if cached is not None:
            return cached
        data = self.fetch_l2_book(market)
        levels = data.get("levels") if isinstance(data, dict) else None
        if not isinstance(levels, list) or len(levels) < 2:
            raise RuntimeError(f"bad l2_snapshot for {market}: {data}")
        limit = int(depth) if depth is not None else None
        bids = self.normalize_order_book_side(levels[0] or [], limit)
        asks = self.normalize_order_book_side(levels[1] or [], limit)
        if not bids or not asks:
            raise RuntimeError(f"empty orderbook for {market}")
        self._cache_order_book(market, bids, asks)
        return self._cached_order_book(market, depth) or (bids, asks)

    def fetch_cached_order_book(
        self, market: str, depth: Optional[int] = None
    ) -> Tuple[List[OrderBookLevel], List[OrderBookLevel]]:
        market = self.normalize_market(market)
        with self._order_book_lock:
            self._refresh_synthetic_order_book_cache(market)
            cached = self._order_book_cache.get(market)
        if cached is None:
            return [], []
        bids, asks, _received_at = cached
        return self._limit_order_book_depth(bids, asks, depth)

    def fetch_order_book_cache_metadata(self, market: str) -> Dict[str, Any]:
        market_u = self.normalize_market(market)
        with self._order_book_lock:
            self._refresh_synthetic_order_book_cache(market_u)
            cached = self._order_book_cache.get(market_u)
            exchange_timestamp_ms = self._order_book_exchange_time_ms.get(market_u)
            metadata = dict(self._order_book_metadata.get(market_u, {}))
        if cached is None:
            return {}
        _bids, _asks, received_at = cached
        return {
            "exchange_timestamp_ms": exchange_timestamp_ms,
            "received_at_ms": int(received_at * 1000),
            "age_ms": max(0, int((time.time() - received_at) * 1000)),
            **metadata,
        }

    def _cache_market_trades(self, trades: List[MarketTrade]) -> None:
        max_items = max(int(getattr(settings, "WS_PUBLIC_TRADE_CACHE_ITEMS", 5000)), 1)
        retention_s = max(
            float(getattr(settings, "WS_PUBLIC_TRADE_CACHE_RETENTION_S", 0.0)),
            float(getattr(settings, "EV_TRADE_LOOKBACK_S", 0.0)),
        )
        cutoff_ms = int((time.time() - retention_s) * 1000) if retention_s > 0 else None
        changed = False
        with self._market_trades_lock:
            for trade in trades:
                market = self.normalize_market(trade.market)
                items = self._market_trades_cache.setdefault(market, [])
                seen = self._market_trades_seen.setdefault(market, set())
                key = self.market_trade_key(trade)
                if key in seen:
                    continue
                items.append(trade)
                seen.add(key)
                changed = True
                while len(items) > max_items:
                    removed = items.pop(0)
                    seen.discard(self.market_trade_key(removed))
                newest_timestamp_ms = max(
                    (item.timestamp_ms for item in items if item.timestamp_ms is not None),
                    default=None,
                )
                while (
                    cutoff_ms is not None
                    and newest_timestamp_ms is not None
                    and newest_timestamp_ms >= cutoff_ms
                    and items
                    and items[0].timestamp_ms is not None
                    and items[0].timestamp_ms < cutoff_ms
                ):
                    removed = items.pop(0)
                    seen.discard(self.market_trade_key(removed))
        if changed:
            self._notify_market_data_update()

    def fetch_cached_market_trades(self, market: str, since_timestamp_ms: int = 0) -> List[MarketTrade]:
        market = self.normalize_market(market)
        with self._market_trades_lock:
            trades = list(self._market_trades_cache.get(market, []))
        return [
            trade
            for trade in trades
            if trade.timestamp_ms is None or trade.timestamp_ms >= since_timestamp_ms
        ]

    def fetch_last_cached_market_trade(self, market: str) -> Optional[MarketTrade]:
        market = self.normalize_market(market)
        with self._market_trades_lock:
            trades = self._market_trades_cache.get(market, [])
            return trades[-1] if trades else None

    def fetch_bbo(self, market: str) -> Tuple[float, float, float]:
        bids, asks = self.fetch_order_book(market, depth=1)
        bid = bids[0].price
        ask = asks[0].price
        if bid <= 0 or ask <= 0 or bid >= ask:
            raise RuntimeError(f"invalid bbo market={market} bid={bid} ask={ask}")
        return bid, ask, 0.5 * (bid + ask)

    def fetch_open_orders(self, market: Optional[str] = None, *, force: bool = False) -> List[OpenOrder]:
        max_age_s = float(getattr(settings, "OPEN_ORDERS_RECONCILE_S", 0.0))
        if force or time.time() - self._open_orders_last_fetch >= max_age_s:
            orders: List[OpenOrder] = []
            for dex in self._dexs_for_market_filter(market):
                dex_orders = self.normalize_open_orders(self.info.open_orders(self.account_address, dex=dex))
                for order in dex_orders:
                    order.market = self.normalize_market(order.market)
                    order.symbol = coin_to_symbol(order.market)
                orders.extend(dex_orders)
            with self._open_orders_lock:
                cached_by_oid = {
                    order.exchange_order_id: order for order in self._open_orders_cache if order.exchange_order_id is not None
                }
                for order in orders:
                    previous = cached_by_oid.get(order.exchange_order_id)
                    if previous is None:
                        continue
                    if not order.client_order_id:
                        order.client_order_id = previous.client_order_id
                    order.raw = {**previous.raw, **order.raw}
                self._open_orders_cache = orders
                self._open_orders_last_fetch = time.time()
            self.log.debug(
                "open_orders remote_refresh count=%s ids=%s",
                len(orders),
                [order.exchange_order_id for order in orders],
            )
        return self.fetch_cached_open_orders(market)

    def fetch_cached_open_orders(self, market: Optional[str] = None) -> List[OpenOrder]:
        with self._open_orders_lock:
            orders = list(self._open_orders_cache)
        if market:
            market_u = self.normalize_market(market)
            orders = [order for order in orders if order.market == market_u]
        return orders

    def _fetch_positions_for_dexs(self, dexs: List[str]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for dex in list(dict.fromkeys(dexs)):
            state = self.info.user_state(self.account_address, dex=dex)
            positions = state.get("assetPositions") if isinstance(state, dict) else []
            if isinstance(positions, list):
                out.extend(positions)
        return out

    def fetch_positions(self) -> List[Dict[str, Any]]:
        return self._fetch_positions_for_dexs(self._dexs_for_market_filter())

    def fetch_position_size(self, market: str) -> Tuple[float, Dict[str, Any]]:
        market = self.normalize_market(market)
        for asset_position in self._fetch_positions_for_dexs(self._dexs_for_market_filter(market)):
            position = asset_position.get("position") if isinstance(asset_position, dict) else None
            if not isinstance(position, dict):
                continue
            if self.normalize_market(str(position.get("coin") or "")) == market:
                return safe_float(position.get("szi"), 0.0), position
        return 0.0, {}

    def fetch_inventory_balances(self, markets: List[str]) -> Dict[str, Tuple[float, Dict[str, Any]]]:
        wanted = {self.normalize_market(market) for market in markets}
        balances: Dict[str, Tuple[float, Dict[str, Any]]] = {market: (0.0, {}) for market in wanted}
        dexs = [""] + sorted(
            dex for dex in {self._dex_for_market_name(market) for market in wanted} if dex
        )
        for asset_position in self._fetch_positions_for_dexs(dexs):
            position = asset_position.get("position") if isinstance(asset_position, dict) else None
            if not isinstance(position, dict):
                continue
            market = self.normalize_market(str(position.get("coin") or ""))
            if market in wanted:
                balances[market] = (safe_float(position.get("szi"), 0.0), position)
        return balances

    def fetch_recent_market_trades(self, market: str, limit: int = 2000) -> List[MarketTrade]:
        market = self.normalize_market(market)
        raw = self.info.post("/info", {"type": "recentTrades", "coin": market})
        if isinstance(raw, dict) and isinstance(raw.get("trades"), list):
            raw_trades = raw["trades"]
        elif isinstance(raw, list):
            raw_trades = raw
        else:
            raw_trades = []
        normalized_raw = []
        for item in raw_trades:
            if isinstance(item, dict):
                normalized_raw.append({"coin": market, **item})
        trades = self.normalize_market_trades(normalized_raw)
        trades.sort(key=lambda trade: trade.timestamp_ms or 0)
        if limit > 0:
            trades = trades[-int(limit):]
        self._cache_market_trades(trades)
        self.log.info("recent market trades backfilled market=%s count=%s", market, len(trades))
        return trades

    def fetch_recent_fills(
        self,
        start_ms: int,
        end_ms: Optional[int] = None,
        aggregate_by_time: bool = False,
    ) -> List[TradeFill]:
        end_ms = int(end_ms if end_ms is not None else time.time() * 1000)
        try:
            raw = self.info.user_fills_by_time(
                self.account_address,
                int(start_ms),
                end_ms,
                bool(aggregate_by_time),
            )
        except TypeError:
            raw = self.info.user_fills_by_time(self.account_address, int(start_ms), end_ms)
        return self.normalize_fills(raw)

    def fetch_account_summary(self) -> Dict[str, Any]:
        state = self.info.user_state(self.account_address)
        return state if isinstance(state, dict) else {}

    def fetch_open_order_details(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for dex in self._dexs_for_market_filter():
            raw = self.info.frontend_open_orders(self.account_address, dex=dex)
            if isinstance(raw, list):
                out.extend(raw)
        return out

    fetch_frontend_open_orders = fetch_open_order_details

    def fetch_spot_balances(self) -> List[Dict[str, Any]]:
        raw = self.info.spot_user_state(self.account_address)
        balances = raw.get("balances") if isinstance(raw, dict) else []
        return balances if isinstance(balances, list) else []

    def fetch_signer_role(self) -> Dict[str, Any]:
        raw = self.info.user_role(self.wallet.address)
        return raw if isinstance(raw, dict) else {}

    def fetch_user_rate_limit(self) -> Dict[str, Any]:
        raw = self.info.user_rate_limit(self.account_address)
        return raw if isinstance(raw, dict) else {"raw": raw}

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
        market = self.normalize_market(market)
        side = side.lower()
        if side not in ("buy", "sell"):
            raise RuntimeError(f"invalid side: {side}")
        size = self.round_size(market, size)
        price = self.round_price(market, side, price)
        if size <= 0:
            raise RuntimeError(f"rounded size <= 0 market={market} size={size}")
        client_order_id = client_order_id or make_cloid()
        with self._exchange_action_lock:
            raw = self.sdk_exchange.order(
                market,
                side == "buy",
                size,
                price,
                order_type={"limit": {"tif": settings.POST_ONLY_TIF if post_only else "Gtc"}},
                reduce_only=bool(reduce_only),
                cloid=Cloid.from_str(client_order_id),
            )
        status, exchange_order_id, error = self.extract_order_status(raw)
        if error:
            raise RuntimeError(f"order rejected market={market} side={side} error={error} raw={raw}")
        result = OrderResult(
            exchange=self.name,
            market=market,
            symbol=coin_to_symbol(market),
            side=side,
            price=price,
            size=size,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            status=status,
            raw=raw if isinstance(raw, dict) else {"raw": raw},
        )
        if result.status == "open":
            self._upsert_cached_order(
                OpenOrder(
                    exchange=self.name,
                    market=market,
                    symbol=coin_to_symbol(market),
                    exchange_order_id=result.exchange_order_id,
                    client_order_id=client_order_id,
                    side=side,
                    price=price,
                    size=size,
                    timestamp_ms=int(time.time() * 1000),
                    raw={"reduceOnly": bool(reduce_only), **result.raw},
                )
            )
        return result

    def cancel_order(
        self,
        market: str,
        *,
        client_order_id: Optional[str] = None,
        exchange_order_id: Optional[str] = None,
        cloid: Optional[str] = None,
        oid: Optional[int] = None,
    ) -> Tuple[bool, Dict[str, Any]]:
        market = self.normalize_market(market)
        client_order_id = client_order_id or cloid
        exchange_order_id = exchange_order_id or (str(oid) if oid is not None else None)
        errors: List[str] = []

        if client_order_id and self.valid_client_order_id(client_order_id):
            try:
                with self._exchange_action_lock:
                    raw = self.sdk_exchange.cancel_by_cloid(market, Cloid.from_str(client_order_id))
                ok, message = self.cancel_response_ok(raw)
                if ok or is_benign_cancel_error(message):
                    self._remove_cached_order(market, client_order_id, None)
                    return True, raw if isinstance(raw, dict) else {"raw": raw}
                errors.append(message)
            except Exception as exc:
                message = str(exc)
                errors.append(message)
                if is_benign_cancel_error(message):
                    return True, {"exception": message}

        if exchange_order_id is not None:
            try:
                with self._exchange_action_lock:
                    raw = self.sdk_exchange.cancel(market, int(exchange_order_id))
                ok, message = self.cancel_response_ok(raw)
                if ok or is_benign_cancel_error(message):
                    self._remove_cached_order(market, None, exchange_order_id)
                    return True, raw if isinstance(raw, dict) else {"raw": raw}
                errors.append(message)
            except Exception as exc:
                message = str(exc)
                errors.append(message)
                if is_benign_cancel_error(message):
                    return True, {"exception": message}

        return False, {
            "errors": errors,
            "market": market,
            "client_order_id": client_order_id,
            "exchange_order_id": exchange_order_id,
        }

    def bulk_edit_orders(self, edits: List[OrderEdit]) -> List[OrderResult]:
        if not edits:
            return []
        requests: List[Dict[str, Any]] = []
        normalized: List[Tuple[OrderEdit, float, float]] = []
        for edit in edits:
            market = self.normalize_market(edit.order.market)
            side = edit.order.side.lower()
            size = self.round_size(market, edit.size)
            price = self.round_price(market, side, edit.price)
            if size <= 0:
                raise RuntimeError(f"rounded edit size <= 0 market={market} size={size}")
            oid: Any
            if edit.order.exchange_order_id is not None:
                oid = int(edit.order.exchange_order_id)
            elif edit.order.client_order_id and self.valid_client_order_id(edit.order.client_order_id):
                oid = Cloid.from_str(edit.order.client_order_id)
            else:
                raise RuntimeError(f"cannot edit order without oid or cloid: {edit.order}")
            request: Dict[str, Any] = {
                "oid": oid,
                "order": {
                    "coin": market,
                    "is_buy": side == "buy",
                    "sz": size,
                    "limit_px": price,
                    "order_type": {"limit": {"tif": settings.POST_ONLY_TIF if edit.post_only else "Gtc"}},
                    "reduce_only": bool(edit.reduce_only),
                },
            }
            if edit.order.client_order_id and self.valid_client_order_id(edit.order.client_order_id):
                request["order"]["cloid"] = Cloid.from_str(edit.order.client_order_id)
            requests.append(request)
            normalized.append((edit, price, size))

        with self._exchange_action_lock:
            raw = self.sdk_exchange.bulk_modify_orders_new(requests)
        errors = self.action_response_errors(raw)
        if errors:
            self._open_orders_last_fetch = 0.0
            raise RuntimeError(f"bulk edit rejected errors={errors} raw={raw}")

        results: List[OrderResult] = []
        for index, (edit, price, size) in enumerate(normalized):
            order = edit.order
            status, response_order_id, error = self.extract_order_status(raw, index=index)
            if error:
                raise RuntimeError(f"bulk edit rejected index={index} error={error} raw={raw}")
            exchange_order_id = response_order_id or order.exchange_order_id
            if status == "filled":
                self._remove_cached_order(order.market, None, order.exchange_order_id)
            else:
                cached = OpenOrder(
                    exchange=self.name,
                    market=self.normalize_market(order.market),
                    symbol=coin_to_symbol(self.normalize_market(order.market)),
                    exchange_order_id=exchange_order_id,
                    client_order_id=order.client_order_id,
                    side=order.side.lower(),
                    price=price,
                    size=size,
                    timestamp_ms=int(time.time() * 1000),
                    raw={"reduceOnly": bool(edit.reduce_only), "modify_response": raw},
                )
                self._upsert_cached_order(cached)
            results.append(
                OrderResult(
                    exchange=self.name,
                    market=self.normalize_market(order.market),
                    symbol=coin_to_symbol(self.normalize_market(order.market)),
                    side=order.side.lower(),
                    price=price,
                    size=size,
                    client_order_id=order.client_order_id or "",
                    exchange_order_id=exchange_order_id,
                    status=status if status != "unknown" else "open",
                    raw=raw if isinstance(raw, dict) else {"raw": raw},
                )
            )
        return results

    def _upsert_cached_order(self, updated: OpenOrder) -> None:
        with self._open_orders_lock:
            self._open_orders_cache = [
                order
                for order in self._open_orders_cache
                if not (
                    updated.exchange_order_id is not None
                    and order.exchange_order_id is not None
                    and str(order.exchange_order_id) == str(updated.exchange_order_id)
                )
                and not (
                    updated.client_order_id
                    and order.client_order_id
                    and order.client_order_id.lower() == updated.client_order_id.lower()
                )
            ]
            self._open_orders_cache.append(updated)

    def _remove_cached_order(
        self,
        market: str,
        client_order_id: Optional[str],
        exchange_order_id: Optional[str],
    ) -> None:
        market = self.normalize_market(market)
        # A modify can reuse the cloid while replacing the oid. A delayed close
        # update for the old oid must not remove the amended order from cache.
        with self._open_orders_lock:
            self._open_orders_cache = [
                order
                for order in self._open_orders_cache
                if not (
                    order.market == market
                    and (
                        exchange_order_id is not None
                        and order.exchange_order_id is not None
                        and str(order.exchange_order_id) == str(exchange_order_id)
                        or exchange_order_id is None
                        and client_order_id
                        and order.client_order_id
                        and order.client_order_id.lower() == client_order_id.lower()
                    )
                )
            ]

    @staticmethod
    def valid_client_order_id(client_order_id: str) -> bool:
        return isinstance(client_order_id, str) and client_order_id.startswith("0x") and len(client_order_id[2:]) == 32

    valid_cloid = valid_client_order_id

    def place_post_only_limit(
        self,
        coin: str,
        side: str,
        size: float,
        price: float,
        reduce_only: bool = False,
        cloid: Optional[str] = None,
    ) -> OrderResult:
        return self.place_limit_order(
            coin,
            side,
            size,
            price,
            reduce_only=reduce_only,
            post_only=True,
            client_order_id=cloid,
        )

    def cancel_all_open_orders_for_coin(self, coin: str) -> None:
        self.cancel_all_open_orders_for_market(coin)

    @classmethod
    def normalize_open_orders(cls, raw: Any) -> List[OpenOrder]:
        out: List[OpenOrder] = []
        if isinstance(raw, dict) and isinstance(raw.get("orders"), list):
            raw = raw["orders"]
        if not isinstance(raw, list):
            return out
        for item in raw:
            if not isinstance(item, dict):
                continue
            market = normalize_market_name(str(item.get("coin") or ""))
            if not market:
                continue
            oid_raw = item.get("oid") or item.get("id") or item.get("orderId")
            oid = safe_int(oid_raw, 0) or None
            client_order_id = item.get("cloid") or item.get("clientOrderId") or item.get("c")
            side_raw = str(item.get("side") or item.get("dir") or "").lower().strip()
            if side_raw in ("b", "bid", "buy"):
                side = "buy"
            elif side_raw in ("a", "ask", "sell"):
                side = "sell"
            else:
                side = side_raw
            out.append(
                OpenOrder(
                    exchange=cls.name,
                    market=market,
                    symbol=coin_to_symbol(market),
                    exchange_order_id=str(oid) if oid is not None else None,
                    client_order_id=str(client_order_id) if client_order_id else None,
                    side=side,
                    price=safe_float(item.get("limitPx") or item.get("px") or item.get("price"), 0.0),
                    size=safe_float(item.get("sz") or item.get("size") or item.get("amount"), 0.0),
                    timestamp_ms=safe_int(item.get("timestamp") or item.get("time"), 0) or None,
                    raw=item,
                )
            )
        return out

    @staticmethod
    def normalize_order_book_side(raw: Any, depth: Optional[int]) -> List[OrderBookLevel]:
        out: List[OrderBookLevel] = []
        if not isinstance(raw, list):
            return out
        for item in raw[:depth] if depth is not None else raw:
            if not isinstance(item, dict):
                continue
            price = safe_float(item.get("px"), 0.0)
            size = safe_float(item.get("sz"), 0.0)
            if price <= 0 or size < 0:
                continue
            out.append(
                OrderBookLevel(
                    price=price,
                    size=size,
                    order_count=safe_int(item.get("n"), 0) or None,
                    raw=item,
                )
            )
        return out

    @staticmethod
    def extract_order_status(raw: Any, index: int = 0) -> Tuple[str, Optional[str], Optional[str]]:
        if not isinstance(raw, dict):
            return "unknown", None, None
        top_level_error = HyperliquidConnector.response_error_message(raw)
        if top_level_error:
            return "error", None, top_level_error
        statuses = HyperliquidConnector.response_statuses(raw)
        if not statuses or not isinstance(statuses, list):
            return "unknown", None, None
        first = statuses[index] if index < len(statuses) else None
        if not isinstance(first, dict):
            return "unknown", None, None
        if "error" in first:
            return "error", None, str(first.get("error"))
        if "resting" in first and isinstance(first["resting"], dict):
            oid = safe_int(first["resting"].get("oid"), 0) or None
            return "open", str(oid) if oid is not None else None, None
        if "filled" in first and isinstance(first["filled"], dict):
            oid = safe_int(first["filled"].get("oid"), 0) or None
            return "filled", str(oid) if oid is not None else None, None
        return "unknown", None, None

    @classmethod
    def normalize_fills(cls, raw: Any) -> List[TradeFill]:
        out: List[TradeFill] = []
        if not isinstance(raw, list):
            return out
        for item in raw:
            if not isinstance(item, dict):
                continue
            market = normalize_market_name(str(item.get("coin") or ""))
            if not market:
                continue
            oid = item.get("oid")
            timestamp_ms = safe_int(item.get("time"), 0) or None
            trade_id = item.get("tid") or item.get("hash") or f"{market}:{oid}:{timestamp_ms}"
            side_raw = str(item.get("side") or "").lower()
            side = "buy" if side_raw in ("b", "buy") else "sell" if side_raw in ("a", "sell") else side_raw
            out.append(
                TradeFill(
                    exchange=cls.name,
                    market=market,
                    symbol=coin_to_symbol(market),
                    exchange_order_id=str(oid) if oid is not None else None,
                    client_order_id=str(item.get("cloid")) if item.get("cloid") else None,
                    trade_id=str(trade_id),
                    side=side,
                    price=safe_float(item.get("px"), 0.0),
                    size=abs(safe_float(item.get("sz"), 0.0)),
                    fee=safe_float(item.get("fee"), 0.0),
                    fee_currency=str(item.get("feeToken") or "") or None,
                    timestamp_ms=timestamp_ms,
                    raw=item,
                )
            )
        return out

    @classmethod
    def normalize_market_trades(cls, raw: Any) -> List[MarketTrade]:
        out: List[MarketTrade] = []
        if isinstance(raw, dict) and isinstance(raw.get("trades"), list):
            raw = raw["trades"]
        if not isinstance(raw, list):
            return out
        for item in raw:
            if not isinstance(item, dict):
                continue
            market = normalize_market_name(str(item.get("coin") or ""))
            if not market:
                continue
            timestamp_ms = safe_int(item.get("time"), 0) or None
            trade_id = item.get("tid") or item.get("hash") or f"{market}:{timestamp_ms}:{len(out)}"
            side_raw = str(item.get("side") or "").lower()
            side = "buy" if side_raw in ("b", "buy") else "sell" if side_raw in ("a", "sell") else side_raw
            out.append(
                MarketTrade(
                    exchange=cls.name,
                    market=market,
                    trade_id=str(trade_id),
                    side=side,
                    price=safe_float(item.get("px"), 0.0),
                    size=abs(safe_float(item.get("sz"), 0.0)),
                    timestamp_ms=timestamp_ms,
                    raw=item,
                )
            )
        return out

    @staticmethod
    def market_trade_key(trade: MarketTrade) -> str:
        return f"{trade.market}:{trade.timestamp_ms}:{trade.trade_id}"

    @staticmethod
    def cancel_response_ok(raw: Any) -> Tuple[bool, str]:
        if not isinstance(raw, dict):
            return True, "non-dict response"
        statuses = HyperliquidConnector.response_statuses(raw)
        if not statuses:
            return raw.get("status") == "ok", json_dumps(raw)
        errors = [str(status.get("error")) for status in statuses if isinstance(status, dict) and status.get("error")]
        if errors:
            return False, "; ".join(errors)
        return True, json_dumps(raw)

    @staticmethod
    def response_statuses(raw: Any) -> List[Dict[str, Any]]:
        if not isinstance(raw, dict):
            return []
        response = raw.get("response")
        if not isinstance(response, dict):
            return []
        data = response.get("data")
        if not isinstance(data, dict):
            return []
        statuses = data.get("statuses")
        return statuses if isinstance(statuses, list) else []

    @staticmethod
    def response_error_message(raw: Any) -> Optional[str]:
        if not isinstance(raw, dict) or raw.get("status") != "err":
            return None
        response = raw.get("response")
        if isinstance(response, str):
            return response
        if response is not None:
            return json_dumps(response)
        return json_dumps(raw)

    @staticmethod
    def action_response_errors(raw: Any) -> List[str]:
        if not isinstance(raw, dict):
            return []
        top_level_error = HyperliquidConnector.response_error_message(raw)
        if top_level_error:
            return [top_level_error]
        statuses = HyperliquidConnector.response_statuses(raw)
        return [
            str(status.get("error"))
            for status in statuses
            if isinstance(status, dict) and status.get("error")
        ]


def create_connector() -> ExchangeConnector:
    return HyperliquidConnector()
