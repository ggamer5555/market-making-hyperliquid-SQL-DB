from __future__ import annotations

import logging
import signal
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import settings
from common import SingleInstanceLock, json_dumps, normalize_market_name, redacted_settings, safe_float, utc_now_iso
from connectors.base import ExchangeConnector, OpenOrder, OrderBookLevel, OrderEdit, TradeFill
from db_store import DualDB
from market_recorder import MarketDataRecorder
from scripts.base import StrategyScript
from scripts.ev_spread import EvSpreadResult, build_ev_spread_curve, choose_ev_half_spread_bps
from scripts.lob_vwap import LobVwapWindow
from scripts.market_making_model import AvellanedaStoikov, Quote
from scripts.orderbook import calculate_lob_guard, outward_level_prices
from scripts.trade_expectancy import (
    BookSnapshotSample,
    SweepSample,
    TradeFairSample,
    book_snapshot_from_objects,
    estimate_fills_curve_by_side_from_current_depth_sweeps,
    estimate_fills_curve_by_side_from_sweeps,
    estimate_fills_curve_from_current_depth_sweeps,
    estimate_fills_curve_from_sweeps,
    grouped_sweeps_from_trades_with_book,
    grouped_sweeps_from_trade_books,
    load_sweep_samples_from_sqlite,
    sample_from_trade,
)


@dataclass
class DesiredOrder:
    level: int
    side: str
    price: float
    size: float
    reduce_only: bool


class MarketMakingScript(StrategyScript):
    """
    Strategy runner only.

    Exchange API details are behind a connector in connectors/.
    Database details are in db_store.py.
    Avellaneda-Stoikov math is in scripts/market_making_model.py.
    """

    def __init__(self, connector: ExchangeConnector):
        self.log = logging.getLogger("MarketMakingScript")
        self.exchange = connector
        self.markets = [self.market_name(market) for market in settings.MARKETS]
        self.db = DualDB(settings.PRIMARY_SQL_URL, settings.SQLITE_URL, settings.REPLICATION_SPOOL_FILE)
        self.model = AvellanedaStoikov()
        self.stop_requested = False
        self.lock = SingleInstanceLock(settings.LOCK_FILE)
        self.last_bulk_edit: Dict[str, float] = {}
        self.last_leverage_sync = 0.0
        self.last_inventory_balance_poll = 0.0
        self.last_open_orders_reconcile = 0.0
        self.last_fills_poll = 0.0
        self.last_reconcile = 0.0
        self.market_data_sequence = 0
        self.maintenance_stop = threading.Event()
        self.maintenance_thread: Optional[threading.Thread] = None
        self.initialized = False
        self.inventory_balances: Dict[str, Tuple[float, dict]] = {}
        self.market_leverage: Dict[str, int] = {}
        self.latest_market_diagnostics: Dict[str, dict] = {}
        self.market_diagnostics_lock = threading.RLock()
        self.lob_vwap_windows = defaultdict(
            lambda: LobVwapWindow(float(settings.LOB_VWAP_WINDOW_S))
        )
        self.fair_price_history: Dict[str, deque[Tuple[int, float]]] = defaultdict(deque)
        self.ev_book_history: Dict[str, deque[BookSnapshotSample]] = defaultdict(deque)
        self.last_open_order_keys: Dict[str, set[str]] = {}
        self.last_open_order_snapshot: Dict[str, List[OpenOrder]] = {}
        self.inferred_inventory_after_fill: Dict[str, Tuple[float, float]] = {}
        self.last_remaining_order_skip_log: Dict[str, float] = {}
        self.cancel_on_fill_guard_until: Dict[str, float] = {}
        self.cancel_on_fill_guard_lock = threading.RLock()
        self.last_cancel_guard_log: Dict[str, float] = {}
        self.ev_choice_cache: Dict[str, Tuple[EvSpreadResult, dict, float]] = {}
        self.action_cooldown_until = 0.0
        self.last_action_cooldown_log = 0.0
        self.last_action_rate_limit_log = 0.0
        self.ev_startup_ready: Dict[str, bool] = {}
        self.ev_startup_started_at = time.time()
        self.last_ev_wait_log: Dict[str, float] = {}
        self.seen_fill_ids: set[str] = set()
        self.market_recorder = MarketDataRecorder(
            self.exchange,
            self.market_diagnostics,
            self.inventory_balance,
        )
        # Compatibility alias for older imports and offline tests.
        self.positions = self.inventory_balances

    def market_name(self, market: str) -> str:
        normalizer = getattr(self.exchange, "normalize_market", None)
        if callable(normalizer):
            return str(normalizer(market))
        return normalize_market_name(market)

    def install_signals(self) -> None:
        def _handler(signum, frame):  # type: ignore[no-untyped-def]
            self.log.warning("signal received: %s", signum)
            self.stop_requested = True

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

    def init(self) -> None:
        self.lock.acquire()
        try:
            self.db.create_all()
            self.db.reconcile_all()
            self.db.audit("WARNING", "startup", "market-making program starting", redacted_settings(settings))
            self.exchange.start_background_streams(self.markets)
            self.sync_max_leverage(force=True)
            self.refresh_inventory_balances(force=True)
            self.reconcile_open_orders(force=True)
            self.backfill_recent_fills()
            self.backfill_startup_market_trades()
            self.load_startup_ev_choices()
            if settings.CANCEL_ALL_ON_START:
                for coin in self.markets:
                    self.cancel_all_for_coin(coin, "startup")
            self.initialized = True
        except Exception:
            self.exchange.stop_background_streams()
            self.lock.release()
            raise

    def shutdown(self) -> None:
        self.market_recorder.stop()
        self.stop_maintenance()
        try:
            self.db.audit("WARNING", "shutdown", "market-making program shutting down", {"cancel_on_shutdown": settings.CANCEL_ON_SHUTDOWN})
            if self.initialized and settings.CANCEL_ON_SHUTDOWN:
                self.reconcile_open_orders(force=True)
                for coin in self.markets:
                    self.cancel_all_for_coin(coin, "shutdown")
            self.db.reconcile_all()
        finally:
            self.exchange.stop_background_streams()
            self.lock.release()

    def run_forever(self) -> None:
        self.install_signals()
        self.init()
        self.start_maintenance()
        self.market_recorder.start()
        try:
            while not self.stop_requested:
                if self.kill_switch_enabled():
                    self.log.error("kill switch enabled; cancelling orders and sleeping")
                    self.db.audit("ERROR", "kill_switch", "kill switch enabled")
                    for coin in self.markets:
                        self.cancel_all_for_coin(coin, "kill_switch")
                    time.sleep(settings.LOOP_INTERVAL_S)
                    continue

                for coin in self.markets:
                    try:
                        self.run_coin(coin)
                    except Exception as exc:
                        self.log.exception("coin loop failed coin=%s error=%s", coin, exc)
                        self.db.audit("ERROR", "coin_loop_error", str(exc), coin=coin)

                self.market_data_sequence = self.exchange.wait_for_market_data_update(
                    self.market_data_sequence,
                    settings.LOOP_INTERVAL_S,
                )
        finally:
            self.shutdown()

    def start_maintenance(self) -> None:
        if self.maintenance_thread is not None and self.maintenance_thread.is_alive():
            return
        self.maintenance_stop.clear()
        self.maintenance_thread = threading.Thread(
            target=self._maintenance_loop,
            name="market_making_maintenance",
            daemon=True,
        )
        self.maintenance_thread.start()

    def stop_maintenance(self) -> None:
        self.maintenance_stop.set()
        thread = self.maintenance_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=3.0)
        self.maintenance_thread = None

    def _maintenance_loop(self) -> None:
        while not self.maintenance_stop.is_set():
            for name, action in (
                ("inventory balance refresh", self.refresh_inventory_balances),
                ("open-order reconciliation", self.reconcile_open_orders),
                ("leverage sync", self.sync_max_leverage),
                ("fill poll", self.poll_fills_if_due),
                ("database reconciliation", self.reconcile_db_if_due),
            ):
                try:
                    action()
                except Exception as exc:
                    self.log.exception("%s failed error=%s", name, exc)
                    self.db.audit("ERROR", "maintenance_failed", f"{name}: {exc}")
            self.maintenance_stop.wait(settings.MAINTENANCE_LOOP_INTERVAL_S)

    def kill_switch_enabled(self) -> bool:
        path = Path(settings.KILL_SWITCH_FILE)
        if not path.exists():
            return False
        try:
            import json
            data = json.loads(path.read_text(encoding="utf-8"))
            return bool(data.get("enabled", False))
        except Exception:
            return True

    def refresh_inventory_balances(self, force: bool = False) -> None:
        if not force and time.time() - self.last_inventory_balance_poll < settings.INVENTORY_BALANCE_REFRESH_S:
            return
        balances = self.exchange.fetch_inventory_balances(self.markets)
        for coin in self.markets:
            market = self.market_name(coin)
            qty, raw = balances.get(market, (0.0, {}))
            self.inventory_balances[market] = (qty, raw)
        self.last_inventory_balance_poll = time.time()

    # Backward-compatible name for older imports.
    refresh_positions = refresh_inventory_balances

    def inventory_balance(self, market: str) -> float:
        qty, _raw = self.inventory_balances.get(self.market_name(market), (0.0, {}))
        return float(qty)

    def mark_cancel_on_fill_guard(self, coin: str, reason: str) -> None:
        coin = self.market_name(coin)
        until = time.time() + max(float(getattr(settings, "CANCEL_ON_FILL_GUARD_S", 5.0)), 0.1)
        with self.cancel_on_fill_guard_lock:
            self.cancel_on_fill_guard_until[coin] = max(self.cancel_on_fill_guard_until.get(coin, 0.0), until)
        self.log.debug("cancel-on-fill guard active market=%s reason=%s until=%.3f", coin, reason, until)

    def clear_cancel_on_fill_guard_if_flat(self, coin: str) -> None:
        coin = self.market_name(coin)
        if self.exchange.fetch_cached_open_orders(coin):
            return
        with self.cancel_on_fill_guard_lock:
            self.cancel_on_fill_guard_until.pop(coin, None)

    def cancel_on_fill_guard_active(self, coin: str) -> bool:
        coin = self.market_name(coin)
        with self.cancel_on_fill_guard_lock:
            until = self.cancel_on_fill_guard_until.get(coin, 0.0)
            if time.time() <= until:
                return True
            self.cancel_on_fill_guard_until.pop(coin, None)
            return False

    def quote_blocked_by_cancel_guard(self, coin: str, context: str) -> bool:
        if not self.cancel_on_fill_guard_active(coin):
            return False
        now = time.time()
        if now - self.last_cancel_guard_log.get(coin, 0.0) >= 1.0:
            self.last_cancel_guard_log[coin] = now
            self.log.warning("quote sync blocked while cancel-on-fill is active market=%s context=%s", coin, context)
        return True

    def market_diagnostics(self, market: str) -> dict:
        with self.market_diagnostics_lock:
            return dict(self.latest_market_diagnostics.get(self.market_name(market), {}))

    def reconcile_open_orders(self, force: bool = False) -> None:
        if not force and time.time() - self.last_open_orders_reconcile < settings.OPEN_ORDERS_RECONCILE_S:
            return
        self.exchange.fetch_open_orders(force=True)
        self.last_open_orders_reconcile = time.time()

    def reconcile_db_if_due(self) -> None:
        if time.time() - self.last_reconcile < settings.RECONCILE_DB_S:
            return
        self.db.reconcile_all()
        self.last_reconcile = time.time()

    def backfill_startup_market_trades(self) -> None:
        if not bool(getattr(settings, "EV_STARTUP_BACKFILL_RECENT_MARKET_TRADES", True)):
            return
        if not bool(getattr(settings, "EV_STARTUP_USE_STORED_MARKET_DATA", True)):
            return
        limit = int(getattr(settings, "EV_STARTUP_RECENT_MARKET_TRADES_LIMIT", 2000))
        for coin in self.markets:
            try:
                inserted = self.market_recorder.backfill_recent_market_trades(coin, limit=limit)
                self.db.audit(
                    "INFO",
                    "market_trades_startup_backfilled",
                    "recent public market trades saved for EV startup calculation",
                    {"limit": limit, "inserted": inserted},
                    coin=coin,
                )
            except Exception as exc:
                self.log.warning("startup market trade backfill failed market=%s error=%s", coin, exc)
                self.db.audit("WARNING", "market_trades_startup_backfill_failed", str(exc), coin=coin)

    def backfill_recent_fills(self) -> None:
        lookback_hours = max(float(getattr(settings, "USER_FILLS_BACKFILL_LOOKBACK_HOURS", 24.0)), 0.0)
        if lookback_hours <= 0:
            return
        end_ms = int(time.time() * 1000)
        start_ms = int(end_ms - lookback_hours * 3600.0 * 1000)
        try:
            fills = self.exchange.fetch_recent_fills(
                start_ms,
                end_ms,
                bool(getattr(settings, "USER_FILLS_AGGREGATE_BY_TIME", False)),
            )
            for fill in fills:
                self.record_fill(fill)
            self.last_fills_poll = time.time()
            self.db.audit(
                "INFO",
                "user_fills_startup_backfilled",
                "recent account fills saved",
                {"lookback_hours": lookback_hours, "count": len(fills)},
            )
            self.log.info("startup user fills backfilled lookback_hours=%s count=%s", lookback_hours, len(fills))
        except Exception as exc:
            self.db.audit("WARNING", "user_fills_startup_backfill_failed", str(exc))
            self.log.warning("startup user fills backfill failed error=%s", exc)

    def sync_max_leverage(self, force: bool = False) -> None:
        if not force and time.time() - self.last_leverage_sync < settings.LEVERAGE_SYNC_INTERVAL_S:
            return
        self.exchange.refresh_market_metadata()
        rate_limit_raw: dict = {}
        rate_limit_summary: dict = {}
        defer_leverage_actions = False
        if settings.SYNC_MAX_LEVERAGE:
            defer_leverage_actions, rate_limit_raw, rate_limit_summary = self.defer_actions_if_rate_limited("sync_max_leverage")
        for coin in self.markets:
            market = self.market_name(coin)
            max_leverage = self.exchange.fetch_market_max_leverage(market)
            configured_leverage = max_leverage if settings.SYNC_MAX_LEVERAGE else 1
            if settings.SYNC_MAX_LEVERAGE and not defer_leverage_actions:
                raw, margin_mode, configured_leverage = self.set_market_leverage_with_fallback(
                    market,
                    configured_leverage,
                    preferred_is_cross=settings.LEVERAGE_IS_CROSS,
                )
            elif settings.SYNC_MAX_LEVERAGE:
                configured_leverage = 1
                raw = {
                    "skipped": "action rate limit exhausted; leverage sync deferred",
                    "rate_limit_summary": rate_limit_summary,
                    "rate_limit_raw": rate_limit_raw,
                }
                margin_mode = "rate_limit_pending"
            else:
                raw = {"skipped": "SYNC_MAX_LEVERAGE is False; conservative 1x margin estimate stored"}
                margin_mode = "cross" if settings.LEVERAGE_IS_CROSS else "isolated"
            self.market_leverage[market] = configured_leverage
            target_exposure = self.opening_exposure_notional_usd()
            self.db.upsert("market_leverage", "coin", {
                "coin": market,
                "symbol": self.exchange.symbol_for_market(market),
                "max_leverage": max_leverage,
                "configured_leverage": configured_leverage,
                "margin_mode": margin_mode,
                "target_order_exposure_usd": target_exposure,
                "target_order_margin_usd": target_exposure / configured_leverage,
                "raw_json": json_dumps(raw),
                "updated_at": utc_now_iso(),
            })
            self.db.audit(
                "INFO",
                "max_leverage_synced",
                "market leverage synchronized",
                {
                    "max_leverage": max_leverage,
                    "configured_leverage": configured_leverage,
                    "margin_mode": margin_mode,
                    "target_order_exposure_usd": target_exposure,
                    "target_order_margin_usd": target_exposure / configured_leverage,
                },
                coin=market,
            )
        self.last_leverage_sync = time.time()

    def defer_actions_if_rate_limited(self, context: str) -> tuple[bool, dict, dict]:
        try:
            raw = self.exchange.fetch_user_rate_limit()
        except Exception as exc:
            self.db.audit("WARNING", "action_rate_limit_check_failed", str(exc))
            self.log.warning("action rate-limit preflight failed context=%s error=%s", context, exc)
            return False, {}, {}
        if not isinstance(raw, dict) or not raw:
            return False, raw if isinstance(raw, dict) else {"raw": raw}, {}
        used = safe_float(raw.get("nRequestsUsed"), -1.0)
        cap = safe_float(raw.get("nRequestsCap"), -1.0)
        surplus = safe_float(raw.get("nRequestsSurplus"), 0.0)
        remaining = cap + surplus - used if used >= 0 and cap >= 0 else None
        summary = {
            "n_requests_used": used if used >= 0 else None,
            "n_requests_cap": cap if cap >= 0 else None,
            "n_requests_surplus": surplus,
            "remaining_requests": remaining,
        }
        min_remaining = float(getattr(settings, "ACTION_RATE_LIMIT_MIN_REMAINING_REQUESTS", 1.0))
        if remaining is None or remaining >= min_remaining:
            return False, raw, summary
        reason = f"{context} userRateLimit remaining_requests={remaining} raw={json_dumps(raw)}"
        self.mark_action_rate_limited(reason)
        self.db.audit(
            "WARNING",
            "action_rate_limit_deferred",
            "exchange action budget exhausted; deferring action requests",
            {"summary": summary, "raw": raw},
        )
        return True, raw, summary

    def set_market_leverage_with_fallback(
        self,
        market: str,
        leverage: int,
        *,
        preferred_is_cross: bool,
    ) -> tuple[dict, str, int]:
        if not preferred_is_cross:
            try:
                return self.exchange.set_market_leverage(market, leverage, is_cross=False), "isolated", leverage
            except RuntimeError as exc:
                message = str(exc)
                if not self.is_request_limit_error(message):
                    raise
                self.mark_action_rate_limited(message)
                self.log.warning(
                    "isolated leverage sync rate limited; continuing with conservative 1x estimate market=%s leverage=%s error=%s",
                    market,
                    leverage,
                    message,
                )
                return {"isolated_sync_deferred": message}, "isolated_pending", 1
        try:
            return self.exchange.set_market_leverage(market, leverage, is_cross=True), "cross", leverage
        except RuntimeError as exc:
            message = str(exc)
            if self.is_request_limit_error(message):
                self.mark_action_rate_limited(message)
                self.log.warning(
                    "cross leverage sync rate limited; continuing with conservative 1x estimate market=%s leverage=%s error=%s",
                    market,
                    leverage,
                    message,
                )
                return {"cross_sync_deferred": message}, "cross_pending", 1
            if "cross margin is not allowed" not in message.lower():
                raise
            self.log.warning(
                "cross margin rejected; retrying isolated margin market=%s leverage=%s error=%s",
                market,
                leverage,
                message,
            )
            try:
                raw = self.exchange.set_market_leverage(market, leverage, is_cross=False)
            except RuntimeError as isolated_exc:
                isolated_message = str(isolated_exc)
                if not self.is_request_limit_error(isolated_message):
                    raise
                self.mark_action_rate_limited(isolated_message)
                self.log.warning(
                    "isolated leverage fallback rate limited; continuing with conservative 1x estimate market=%s leverage=%s error=%s",
                    market,
                    leverage,
                    isolated_message,
                )
                return {
                    "cross_rejected": message,
                    "isolated_sync_deferred": isolated_message,
                }, "isolated_pending", 1
            return {"cross_rejected": message, "isolated_response": raw}, "isolated", leverage

    @staticmethod
    def is_request_limit_error(message: str) -> bool:
        lowered = str(message).lower()
        return (
            "too many cumulative requests" in lowered
            or "too many requests" in lowered
            or "rate limit" in lowered
            or "rate-limited" in lowered
        )

    def mark_action_rate_limited(self, reason: str) -> None:
        now = time.time()
        cooldown_s = max(1.0, float(getattr(settings, "ACTION_RATE_LIMIT_COOLDOWN_S", 30.0)))
        self.action_cooldown_until = max(self.action_cooldown_until, now + cooldown_s)
        log_s = max(1.0, float(getattr(settings, "ACTION_RATE_LIMIT_LOG_S", 10.0)))
        if now - self.last_action_rate_limit_log >= log_s:
            self.last_action_rate_limit_log = now
            self.log.warning(
                "exchange action rate limited; pausing order actions for %.1fs reason=%s",
                self.action_cooldown_until - now,
                reason,
            )

    def action_cooldown_active(self, context: str) -> bool:
        now = time.time()
        remaining_s = self.action_cooldown_until - now
        if remaining_s <= 0:
            return False
        log_s = max(1.0, float(getattr(settings, "ACTION_RATE_LIMIT_LOG_S", 10.0)))
        if now - self.last_action_cooldown_log >= log_s:
            self.last_action_cooldown_log = now
            self.log.warning(
                "exchange action cooldown active context=%s remaining_s=%.1f",
                context,
                remaining_s,
            )
        return True

    def run_coin(self, coin: str) -> None:
        coin = self.market_name(coin)
        if self.quote_blocked_by_cancel_guard(coin, "run_coin_start"):
            return
        bids, asks = self.exchange.fetch_order_book(coin, depth=settings.LOB_MAX_LEVELS)
        if not bids or not asks:
            raise RuntimeError(f"empty order book coin={coin}")
        best_bid, best_ask = bids[0].price, asks[0].price
        mid = 0.5 * (best_bid + best_ask)
        spread_bps = (best_ask - best_bid) / mid * 10000.0
        if spread_bps > settings.MAX_SPREAD_BPS:
            self.log.warning("spread too wide coin=%s spread_bps=%.2f", coin, spread_bps)
            self.db.audit("WARNING", "wide_spread_skip", f"spread too wide: {spread_bps:.2f} bps", coin=coin)
            self.cancel_all_for_coin(coin, "wide_spread")
            return

        open_order_count_shrank = self.refresh_inventory_after_open_order_shrink(coin)
        if open_order_count_shrank and bool(getattr(settings, "CANCEL_SYMBOL_ORDERS_ON_FILL", True)):
            self.cancel_symbol_orders_after_fill(coin, "fill_detected_open_order_shrink")
            return
        if self.quote_blocked_by_cancel_guard(coin, "after_fill_detection"):
            return
        inventory, raw_inventory = self.inventory_balances.get(coin, (0.0, {}))
        inventory = self.inventory_for_quoting(coin, inventory)
        fair_price, lob_vwap_fair_price = self.fair_price_from_lob_vwap(coin, bids, asks, mid)
        now_ms = int(time.time() * 1000)
        self.record_fair_price_sample(coin, now_ms, fair_price)
        self.record_ev_book_sample(coin, now_ms, fair_price, bids, asks)
        ev_choice, ev_diagnostics = self.ev_spread_choice(coin, fair_price, bids, asks, now_ms)
        if self.waiting_for_startup_ev(coin, inventory, ev_choice, ev_diagnostics, mid, best_bid, best_ask, fair_price):
            return
        quote = self.model.quote(
            coin,
            best_bid,
            best_ask,
            inventory,
            symbol=self.exchange.symbol_for_market(coin),
            fair_price=fair_price,
            ev_choice=ev_choice,
        )
        if quote.bid is None or quote.ask is None:
            self.db.audit("WARNING", "bad_quote_skip", "model returned invalid quote", coin=coin)
            self.cancel_all_for_coin(coin, "bad_quote")
            return

        min_half_spread = mid * float(settings.MIN_QUOTE_SPREAD_BPS) / 20000.0
        closest_bid = min(quote.bid, best_bid, mid - min_half_spread)
        closest_ask = max(quote.ask, best_ask, mid + min_half_spread)
        price_step = self.exchange.price_step(coin, mid)
        lob_bid_percentile_price = None
        lob_ask_percentile_price = None
        lob_guard_bid_price = None
        lob_guard_ask_price = None
        if settings.USE_LOB_PERCENTILE_GUARD:
            guard = calculate_lob_guard(
                bids,
                asks,
                mid,
                float(settings.LOB_PERCENTILE),
                float(settings.LOB_WITHIN_MID_PCT),
                int(settings.LOB_MAX_LEVELS),
                price_step,
            )
            price_step = guard.price_step
            lob_bid_percentile_price = guard.bid_percentile_price
            lob_ask_percentile_price = guard.ask_percentile_price
            lob_guard_bid_price = guard.bid_price
            lob_guard_ask_price = guard.ask_price
            if guard.bid_price is not None:
                closest_bid = min(closest_bid, guard.bid_price)
            if guard.ask_price is not None:
                closest_ask = max(closest_ask, guard.ask_price)

        level_count = max(int(settings.ORDERS_PER_SIDE), 1)
        bid_prices = self.round_level_prices(coin, "buy", closest_bid, level_count, price_step)
        ask_prices = self.round_level_prices(coin, "sell", closest_ask, level_count, price_step)
        with self.market_diagnostics_lock:
            self.latest_market_diagnostics[coin] = {
                "mid_price": quote.mid,
                "best_bid": quote.best_bid,
                "best_ask": quote.best_ask,
                "volatility": quote.sigma_per_s,
                "gamma": quote.gamma,
                "k": quote.k,
                "reservation_price": quote.reservation_price,
                "half_spread": quote.half_spread,
                "half_spread_bps": quote.half_spread_bps,
                "fair_price": quote.fair_price,
                "spread_source": quote.spread_source,
                "lob_vwap_fair_price": lob_vwap_fair_price,
                "q_norm": quote.q_norm,
                "lob_bid_percentile_price": lob_bid_percentile_price,
                "lob_ask_percentile_price": lob_ask_percentile_price,
                "lob_guard_bid_price": lob_guard_bid_price,
                "lob_guard_ask_price": lob_guard_ask_price,
                "lob_price_step": price_step,
                "desired_bid_prices": bid_prices,
                "desired_ask_prices": ask_prices,
                **ev_diagnostics,
            }
        desired: List[DesiredOrder] = []
        inv_notional = inventory * quote.mid
        open_notional = self.opening_exposure_notional_usd()
        close_size = self.exchange.round_size(coin, abs(inventory))

        if inventory < 0 and settings.REDUCE_ONLY_TO_CLOSE_INVENTORY and close_size > 0:
            desired.append(DesiredOrder(0, "buy", bid_prices[0], close_size, True))
        else:
            pending_long_notional = 0.0
            for level, price in enumerate(bid_prices):
                if inv_notional + pending_long_notional + open_notional > settings.MAX_LONG_INVENTORY_NOTIONAL_USD:
                    break
                desired.append(DesiredOrder(level, "buy", price, self.opening_size(coin, price), False))
                pending_long_notional += open_notional

        if inventory > 0 and settings.REDUCE_ONLY_TO_CLOSE_INVENTORY and close_size > 0:
            desired.append(DesiredOrder(0, "sell", ask_prices[0], close_size, True))
        else:
            pending_short_notional = 0.0
            for level, price in enumerate(ask_prices):
                if inv_notional - pending_short_notional - open_notional < -settings.MAX_SHORT_INVENTORY_NOTIONAL_USD:
                    break
                desired.append(DesiredOrder(level, "sell", price, self.opening_size(coin, price), False))
                pending_short_notional += open_notional

        self.sync_orders(coin, desired, quote, place_missing=not open_order_count_shrank)
        self.remember_open_order_state(coin)
        # Persist after exchange synchronization so DB I/O cannot delay edits.
        self.record_inventory_balance(coin, inventory, raw_inventory, mid)
        self.record_quote(quote)

    @staticmethod
    def open_order_keys(orders: List[OpenOrder]) -> set[str]:
        keys = set()
        for order in orders:
            if order.exchange_order_id:
                keys.add(f"oid:{order.exchange_order_id}")
            elif order.client_order_id:
                keys.add(f"cloid:{order.client_order_id}")
        return keys

    def remember_open_order_state(self, coin: str) -> None:
        orders = self.exchange.fetch_cached_open_orders(coin)
        self.last_open_order_keys[coin] = self.open_order_keys(orders)
        self.last_open_order_snapshot[coin] = self.copy_open_orders(orders)

    @staticmethod
    def copy_open_orders(orders: List[OpenOrder]) -> List[OpenOrder]:
        return [
            OpenOrder(
                exchange=order.exchange,
                market=order.market,
                symbol=order.symbol,
                exchange_order_id=order.exchange_order_id,
                client_order_id=order.client_order_id,
                side=order.side,
                price=order.price,
                size=order.size,
                timestamp_ms=order.timestamp_ms,
                raw=dict(order.raw),
            )
            for order in orders
        ]

    def refresh_inventory_after_open_order_shrink(self, coin: str) -> bool:
        current = self.exchange.fetch_cached_open_orders(coin)
        current_keys = self.open_order_keys(current)
        previous_keys = self.last_open_order_keys.get(coin)
        previous_orders = self.last_open_order_snapshot.get(coin, [])
        if previous_keys is None:
            self.last_open_order_keys[coin] = current_keys
            self.last_open_order_snapshot[coin] = self.copy_open_orders(current)
            return False
        if len(current_keys) >= len(previous_keys):
            return False
        if bool(getattr(settings, "CANCEL_SYMBOL_ORDERS_ON_FILL", True)):
            self.mark_cancel_on_fill_guard(coin, "open_order_count_shrink")
        self.infer_inventory_from_order_shrink(coin, previous_orders, current)
        self.db.audit(
            "INFO",
            "open_order_count_decreased",
            "open-order count decreased; refreshing orders and inventory before repricing remaining quote",
            {"previous_count": len(previous_keys), "current_count": len(current_keys)},
            coin=coin,
        )
        self.exchange.fetch_open_orders(coin, force=True)
        self.refresh_inventory_balances(force=True)
        self.last_bulk_edit[coin] = 0.0
        return True

    def infer_inventory_from_order_shrink(self, coin: str, previous: List[OpenOrder], current: List[OpenOrder]) -> None:
        previous_buy = [order for order in previous if order.side == "buy"]
        previous_sell = [order for order in previous if order.side == "sell"]
        expires_at = time.time() + 10.0
        if len(current) == 1 and previous_buy and previous_sell:
            remaining = current[0]
            if remaining.side == "sell":
                size = min(remaining.size, max(order.size for order in previous_buy))
                if size > 0:
                    self.inferred_inventory_after_fill[coin] = (size, expires_at)
                    self.log.info(
                        "inferred long inventory after bid fill market=%s size=%s remaining_order_id=%s",
                        coin,
                        size,
                        remaining.exchange_order_id,
                    )
                    self.db.audit(
                        "INFO",
                        "inventory_inferred_after_bid_fill",
                        "bid disappeared while ask remains; treating ask as close-long order until exchange inventory catches up",
                        {"size": size, "remaining_order_id": remaining.exchange_order_id},
                        coin=coin,
                    )
            elif remaining.side == "buy":
                size = min(remaining.size, max(order.size for order in previous_sell))
                if size > 0:
                    self.inferred_inventory_after_fill[coin] = (-size, expires_at)
                    self.log.info(
                        "inferred short inventory after ask fill market=%s size=%s remaining_order_id=%s",
                        coin,
                        size,
                        remaining.exchange_order_id,
                    )
                    self.db.audit(
                        "INFO",
                        "inventory_inferred_after_ask_fill",
                        "ask disappeared while bid remains; treating bid as close-short order until exchange inventory catches up",
                        {"size": size, "remaining_order_id": remaining.exchange_order_id},
                        coin=coin,
                    )
        elif not current:
            inferred = self.inferred_inventory_after_fill.get(coin)
            if inferred is None:
                return
            inferred_qty, _expires = inferred
            close_disappeared = (
                inferred_qty > 0 and previous_sell
                or inferred_qty < 0 and previous_buy
            )
            if close_disappeared:
                self.inferred_inventory_after_fill[coin] = (0.0, time.time() + 2.0)
                self.log.info("cleared inferred inventory after close order disappeared market=%s", coin)
                self.db.audit(
                    "INFO",
                    "inventory_inference_cleared_after_close",
                    "remaining close order disappeared; ignoring stale exchange inventory briefly",
                    {"previous_order_ids": [order.exchange_order_id for order in previous]},
                    coin=coin,
                )

    def inventory_for_quoting(self, coin: str, exchange_inventory: float) -> float:
        inferred = self.inferred_inventory_after_fill.get(coin)
        if inferred is None:
            return exchange_inventory
        inferred_qty, expires_at = inferred
        if time.time() > expires_at:
            self.inferred_inventory_after_fill.pop(coin, None)
            return exchange_inventory
        current = self.exchange.fetch_cached_open_orders(coin)
        if abs(inferred_qty) <= 1e-12:
            if not current:
                return 0.0
            return exchange_inventory
        close_order_live = (
            inferred_qty > 0 and any(order.side == "sell" for order in current)
            or inferred_qty < 0 and any(order.side == "buy" for order in current)
        )
        if not close_order_live:
            return exchange_inventory
        if abs(exchange_inventory) > 1e-12 and exchange_inventory * inferred_qty > 0:
            return exchange_inventory
        self.log.info(
            "using inferred inventory for quote market=%s exchange_inventory=%s inferred_inventory=%s",
            coin,
            exchange_inventory,
            inferred_qty,
        )
        return inferred_qty

    def fair_price_from_lob_vwap(
        self,
        coin: str,
        bids: List[OrderBookLevel],
        asks: List[OrderBookLevel],
        mid: float,
    ) -> Tuple[float, Optional[float]]:
        if not bool(settings.USE_LOB_VWAP_FAIR_PRICE):
            return mid, None
        try:
            vwap = self.lob_vwap_windows[coin].add_from_book(
                bids,
                asks,
                mid,
                float(settings.LOB_VWAP_WITHIN_MID_PCT),
                int(settings.LOB_VWAP_MAX_LEVELS),
            )
            if vwap is not None and vwap > 0:
                return vwap, vwap
        except Exception as exc:
            self.db.audit("WARNING", "lob_vwap_failed", str(exc), coin=coin)
        return mid, None

    def record_fair_price_sample(self, coin: str, timestamp_ms: int, fair_price: float) -> None:
        if fair_price <= 0:
            return
        history = self.fair_price_history[coin]
        history.append((int(timestamp_ms), float(fair_price)))
        keep_ms = int(max(float(settings.EV_TRADE_LOOKBACK_S) * 2.0, 60.0) * 1000)
        cutoff = int(timestamp_ms) - keep_ms
        while history and history[0][0] < cutoff:
            history.popleft()

    def record_ev_book_sample(
        self,
        coin: str,
        timestamp_ms: int,
        fair_price: float,
        bids: List[OrderBookLevel],
        asks: List[OrderBookLevel],
    ) -> None:
        book = book_snapshot_from_objects(
            timestamp_ms,
            fair_price,
            bids,
            asks,
            within_mid_pct=float(settings.EV_DEPTH_WITHIN_MID_PCT),
        )
        if book is None:
            return
        history = self.ev_book_history[coin]
        history.append(book)
        keep_ms = int(max(float(settings.EV_TRADE_LOOKBACK_S) * 2.0, 60.0) * 1000)
        cutoff = int(timestamp_ms) - keep_ms
        while history and history[0].ts_ms < cutoff:
            history.popleft()

    def trade_fair_samples(self, coin: str, trades: list, current_fair_price: float) -> List[TradeFairSample]:  # type: ignore[type-arg]
        history = list(self.fair_price_history.get(coin, ()))
        out: List[TradeFairSample] = []
        for trade in trades:
            sample = sample_from_trade(trade)
            if sample is None:
                continue
            fair_price = current_fair_price
            for timestamp_ms, historical_fair in reversed(history):
                if timestamp_ms <= sample.ts_ms:
                    fair_price = historical_fair
                    break
            out.append(TradeFairSample(sample, fair_price))
        return out

    def sweep_samples(self, coin: str, trades: list) -> List[SweepSample]:  # type: ignore[type-arg]
        history = list(self.ev_book_history.get(coin, ()))
        trade_books = []
        for trade in trades:
            sample = sample_from_trade(trade)
            if sample is None:
                continue
            matched_book = None
            for book in reversed(history):
                if book.ts_ms <= sample.ts_ms:
                    matched_book = book
                    break
            if matched_book is not None:
                trade_books.append((sample, matched_book))
        return grouped_sweeps_from_trade_books(trade_books, float(settings.EV_SWEEP_GROUP_MS))

    def current_book_sweep_samples(
        self,
        trades: list,  # type: ignore[type-arg]
        fair_price: float,
        bids: List[OrderBookLevel],
        asks: List[OrderBookLevel],
        timestamp_ms: int,
    ) -> List[SweepSample]:
        within_mid_pct = max(
            float(settings.EV_DEPTH_WITHIN_MID_PCT),
            float(settings.EV_MAX_HALF_SPREAD_BPS) / 10000.0,
        )
        book = book_snapshot_from_objects(
            timestamp_ms,
            fair_price,
            bids,
            asks,
            within_mid_pct=within_mid_pct,
        )
        if book is None:
            return []
        return grouped_sweeps_from_trades_with_book(trades, book, float(settings.EV_SWEEP_GROUP_MS))

    def base_ev_diagnostics(self, fair_price: float) -> dict:
        return {
            "ev_enabled": bool(settings.USE_EV_SPREAD_PRICING),
            "ev_applied": False,
            "ev_source": None,
            "ev_depth_mode": "current_book" if bool(getattr(settings, "EV_USE_CURRENT_BOOK_DEPTH", True)) else "historical_book",
            "ev_reused_cached_choice": False,
            "ev_startup_waiting": False,
            "ev_fair_price": fair_price,
            "ev_curve": [],
            "ev_choice": None,
            "ev_trade_count": 0,
            "ev_sweep_count": 0,
            "ev_observed_hours": 0.0,
            "ev_min_trade_samples": int(settings.EV_MIN_TRADE_SAMPLES),
        }

    def cached_ev_choice(self, coin: str, diagnostics: dict):
        if not bool(getattr(settings, "EV_REUSE_LAST_CHOICE_WHEN_NOT_READY", True)):
            return None, diagnostics
        cached = self.ev_choice_cache.get(coin)
        if cached is None:
            return None, diagnostics
        choice, cached_diagnostics, cached_at = cached
        max_age = max(float(getattr(settings, "EV_CACHED_CHOICE_MAX_AGE_S", 0.0)), 0.0)
        age = time.time() - cached_at
        if max_age > 0 and age > max_age:
            self.ev_choice_cache.pop(coin, None)
            return None, diagnostics
        reused = {**diagnostics}
        reused["ev_applied"] = True
        reused["ev_source"] = f"cached:{cached_diagnostics.get('ev_source') or 'previous'}"
        reused["ev_reused_cached_choice"] = True
        reused["ev_cached_choice_age_s"] = age
        reused["ev_curve"] = cached_diagnostics.get("ev_curve", [])
        reused["ev_choice"] = choice.as_dict()
        self.ev_startup_ready[coin] = True
        return choice, reused

    def choose_ev_from_sweeps(
        self,
        coin: str,
        fair_price: float,
        sweeps: List[SweepSample],
        *,
        observed_hours: float,
        source: str,
        trade_count: int = 0,
        current_book_depth: bool = False,
    ):
        diagnostics = self.base_ev_diagnostics(fair_price)
        diagnostics["ev_source"] = source
        diagnostics["ev_depth_mode"] = "current_book" if current_book_depth else "historical_book"
        diagnostics["ev_uses_historical_book_depth"] = not current_book_depth
        diagnostics["ev_trade_count"] = trade_count
        diagnostics["ev_sweep_count"] = len(sweeps)
        diagnostics["ev_observed_hours"] = observed_hours
        try:
            estimator = (
                estimate_fills_curve_from_current_depth_sweeps
                if current_book_depth
                else estimate_fills_curve_from_sweeps
            )
            estimated_fills = estimator(
                sweeps,
                self.opening_exposure_notional_usd(),
                float(settings.EV_MIN_HALF_SPREAD_BPS),
                float(settings.EV_MAX_HALF_SPREAD_BPS),
                float(settings.EV_HALF_SPREAD_STEP_BPS),
                observed_hours=observed_hours,
            )
            side_estimator = (
                estimate_fills_curve_by_side_from_current_depth_sweeps
                if current_book_depth
                else estimate_fills_curve_by_side_from_sweeps
            )
            estimated_fills_by_side = side_estimator(
                sweeps,
                self.opening_exposure_notional_usd(),
                float(settings.EV_MIN_HALF_SPREAD_BPS),
                float(settings.EV_MAX_HALF_SPREAD_BPS),
                float(settings.EV_HALF_SPREAD_STEP_BPS),
                observed_hours=observed_hours,
            )
            markout = {
                spread: float(settings.EV_MARKOUT_BPS)
                for spread in estimated_fills
            }
            curve = build_ev_spread_curve(
                estimated_fills,
                order_notional_usd=self.opening_exposure_notional_usd(),
                maker_fee_bps_per_side=float(settings.MAKER_FEE_BPS_PER_SIDE),
                min_half_spread_bps=float(settings.EV_MIN_HALF_SPREAD_BPS),
                max_half_spread_bps=float(settings.EV_MAX_HALF_SPREAD_BPS),
                step_bps=float(settings.EV_HALF_SPREAD_STEP_BPS),
                min_trades_per_hour=0.0,
                max_trades_per_hour=float(settings.EV_MAX_FILLS_PER_HOUR),
                markout_by_spread=markout,
            )
            ask_curve = build_ev_spread_curve(
                estimated_fills_by_side["ask"],
                order_notional_usd=self.opening_exposure_notional_usd(),
                maker_fee_bps_per_side=float(settings.MAKER_FEE_BPS_PER_SIDE),
                min_half_spread_bps=float(settings.EV_MIN_HALF_SPREAD_BPS),
                max_half_spread_bps=float(settings.EV_MAX_HALF_SPREAD_BPS),
                step_bps=float(settings.EV_HALF_SPREAD_STEP_BPS),
                min_trades_per_hour=0.0,
                max_trades_per_hour=float(settings.EV_MAX_FILLS_PER_HOUR),
                markout_by_spread=markout,
            )
            bid_curve = build_ev_spread_curve(
                estimated_fills_by_side["bid"],
                order_notional_usd=self.opening_exposure_notional_usd(),
                maker_fee_bps_per_side=float(settings.MAKER_FEE_BPS_PER_SIDE),
                min_half_spread_bps=float(settings.EV_MIN_HALF_SPREAD_BPS),
                max_half_spread_bps=float(settings.EV_MAX_HALF_SPREAD_BPS),
                step_bps=float(settings.EV_HALF_SPREAD_STEP_BPS),
                min_trades_per_hour=0.0,
                max_trades_per_hour=float(settings.EV_MAX_FILLS_PER_HOUR),
                markout_by_spread=markout,
            )
            choice = choose_ev_half_spread_bps(curve)
        except Exception as exc:
            self.db.audit("WARNING", "ev_spread_failed", str(exc), coin=coin)
            diagnostics["ev_error"] = str(exc)
            return self.cached_ev_choice(coin, diagnostics)

        diagnostics["ev_curve"] = [row.as_dict() for row in curve]
        diagnostics["ev_curve_ask"] = [row.as_dict() for row in ask_curve]
        diagnostics["ev_curve_bid"] = [row.as_dict() for row in bid_curve]
        diagnostics["ev_choice"] = choice.as_dict() if choice is not None else None
        diagnostics["ev_choice_ask"] = choose_ev_half_spread_bps(ask_curve).as_dict() if choose_ev_half_spread_bps(ask_curve) is not None else None
        diagnostics["ev_choice_bid"] = choose_ev_half_spread_bps(bid_curve).as_dict() if choose_ev_half_spread_bps(bid_curve) is not None else None
        if (
            choice is None
            or len(sweeps) < int(settings.EV_MIN_TRADE_SAMPLES)
            or choice.estimated_fills_per_hour <= 0
        ):
            return self.cached_ev_choice(coin, diagnostics)
        diagnostics["ev_applied"] = True
        self.ev_choice_cache[coin] = (choice, dict(diagnostics), time.time())
        self.ev_startup_ready[coin] = True
        return choice, diagnostics

    def load_startup_ev_choices(self) -> None:
        if not bool(settings.USE_EV_SPREAD_PRICING) or not bool(getattr(settings, "EV_STARTUP_USE_STORED_MARKET_DATA", True)):
            return
        if bool(getattr(settings, "EV_USE_CURRENT_BOOK_DEPTH", True)):
            self.log.info("startup EV historical-depth choice skipped; live EV uses current book depth")
            return
        for coin in self.markets:
            coin = self.market_name(coin)
            try:
                sweeps = load_sweep_samples_from_sqlite(
                    settings.MARKET_DATA_DB_PATH,
                    coin,
                    float(settings.EV_TRADE_LOOKBACK_S),
                    float(settings.EV_SWEEP_GROUP_MS),
                    float(settings.EV_DEPTH_WITHIN_MID_PCT),
                )
            except Exception as exc:
                self.log.info("startup EV history unavailable market=%s error=%s", coin, exc)
                continue
            fair_price = sweeps[-1].book.fair_price if sweeps else 0.0
            observed_hours = max(float(settings.EV_TRADE_LOOKBACK_S) / 3600.0, 1e-9)
            choice, diagnostics = self.choose_ev_from_sweeps(
                coin,
                fair_price,
                sweeps,
                observed_hours=observed_hours,
                source="stored_startup",
                trade_count=len(sweeps),
            )
            if choice is None:
                self.log.info(
                    "startup EV not ready market=%s sweeps=%s min_sweeps=%s",
                    coin,
                    len(sweeps),
                    settings.EV_MIN_TRADE_SAMPLES,
                )
                continue
            self.log.warning(
                "startup EV ready market=%s half_spread_bps=%s estimated_fills_per_hour=%.3f ev_per_hour=%.6f source=%s",
                coin,
                choice.half_spread_bps,
                choice.estimated_fills_per_hour,
                choice.ev_per_hour,
                diagnostics.get("ev_source"),
            )

    def ev_spread_choice(
        self,
        coin: str,
        fair_price: float,
        bids: Optional[List[OrderBookLevel]] = None,
        asks: Optional[List[OrderBookLevel]] = None,
        timestamp_ms: Optional[int] = None,
    ):  # type: ignore[no-untyped-def]
        diagnostics = self.base_ev_diagnostics(fair_price)
        if not bool(settings.USE_EV_SPREAD_PRICING) or fair_price <= 0:
            return None, diagnostics

        lookback_s = max(float(settings.EV_TRADE_LOOKBACK_S), 1.0)
        observed_hours = lookback_s / 3600.0
        since_ms = int((time.time() - lookback_s) * 1000)
        trades = self.exchange.fetch_cached_market_trades(coin, since_ms)
        use_current_book_depth = bool(getattr(settings, "EV_USE_CURRENT_BOOK_DEPTH", True))
        if use_current_book_depth:
            sweeps = self.current_book_sweep_samples(
                trades,
                fair_price,
                bids or [],
                asks or [],
                int(timestamp_ms if timestamp_ms is not None else time.time() * 1000),
            )
            source = "live_current_book"
        else:
            sweeps = self.sweep_samples(coin, trades)
            source = "live_historical_book"
        return self.choose_ev_from_sweeps(
            coin,
            fair_price,
            sweeps,
            observed_hours=observed_hours,
            source=source,
            trade_count=len(trades),
            current_book_depth=use_current_book_depth,
        )

    def waiting_for_startup_ev(
        self,
        coin: str,
        inventory: float,
        ev_choice,
        ev_diagnostics: dict,
        mid: float,
        best_bid: float,
        best_ask: float,
        fair_price: float,
    ) -> bool:  # type: ignore[no-untyped-def]
        if (
            not bool(settings.USE_EV_SPREAD_PRICING)
            or not bool(getattr(settings, "EV_REQUIRE_READY_BEFORE_OPENING", False))
            or self.ev_startup_ready.get(coin)
            or abs(inventory) > 1e-12
        ):
            return False
        if ev_choice is not None:
            self.ev_startup_ready[coin] = True
            return False
        wait_timeout = max(float(getattr(settings, "EV_STARTUP_WAIT_TIMEOUT_S", 0.0)), 0.0)
        elapsed = time.time() - self.ev_startup_started_at
        if (
            wait_timeout > 0
            and elapsed >= wait_timeout
            and bool(getattr(settings, "EV_STARTUP_FALLBACK_AFTER_TIMEOUT", False))
        ):
            self.ev_startup_ready[coin] = True
            self.log.warning(
                "startup EV wait timed out; allowing fallback spread market=%s elapsed_s=%.1f",
                coin,
                elapsed,
            )
            return False

        diagnostics = dict(ev_diagnostics)
        diagnostics["ev_startup_waiting"] = True
        with self.market_diagnostics_lock:
            self.latest_market_diagnostics[coin] = {
                "mid_price": mid,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "fair_price": fair_price,
                **diagnostics,
            }
        now = time.time()
        if now - self.last_ev_wait_log.get(coin, 0.0) >= 2.0:
            self.last_ev_wait_log[coin] = now
            self.log.warning(
                "waiting for EV before opening orders market=%s sweeps=%s trades=%s min_sweeps=%s elapsed_s=%.1f",
                coin,
                diagnostics.get("ev_sweep_count", 0),
                diagnostics.get("ev_trade_count", 0),
                diagnostics.get("ev_min_trade_samples"),
                elapsed,
            )
        return True

    def record_inventory_balance(self, coin: str, qty: float, raw: dict, mid: float) -> None:
        coin = self.market_name(coin)
        self.db.upsert("positions", "coin", {
            "coin": coin,
            "symbol": self.exchange.symbol_for_market(coin),
            "base_qty": qty,
            "mark_price": mid,
            "notional": qty * mid,
            "raw_json": json_dumps(raw),
            "updated_at": utc_now_iso(),
        })

    def round_level_prices(
        self,
        coin: str,
        side: str,
        closest_price: float,
        count: int,
        price_step: float,
    ) -> List[float]:
        raw_prices = outward_level_prices(side, closest_price, count, float(settings.ORDER_LEVEL_SPACING_BPS), price_step)
        prices: List[float] = []
        for raw_price in raw_prices:
            price = self.exchange.round_price(coin, side, raw_price)
            if prices:
                for _ in range(10):
                    if side == "buy" and price < prices[-1] or side == "sell" and price > prices[-1]:
                        break
                    raw_price = prices[-1] + (-price_step if side == "buy" else price_step)
                    price = self.exchange.round_price(coin, side, raw_price)
            if price <= 0:
                raise RuntimeError(f"invalid rounded level price coin={coin} side={side} price={price}")
            if prices and (side == "buy" and price >= prices[-1] or side == "sell" and price <= prices[-1]):
                raise RuntimeError(f"could not create distinct outward levels coin={coin} side={side}")
            prices.append(price)
        return prices

    def opening_size(self, coin: str, price: float) -> float:
        target_notional = self.opening_exposure_notional_usd()
        size = self.exchange.round_size_up(coin, target_notional / price)
        if size <= 0 or size * price + 1e-9 < settings.MIN_OPEN_ORDER_NOTIONAL_USD:
            raise RuntimeError(
                f"cannot compute minimum opening size coin={coin} price={price} "
                f"size={size} min_open_notional={settings.MIN_OPEN_ORDER_NOTIONAL_USD}"
            )
        return size

    @staticmethod
    def opening_exposure_notional_usd() -> float:
        return max(settings.TARGET_ORDER_NOTIONAL_USD, settings.MIN_OPEN_ORDER_NOTIONAL_USD)

    def opening_margin_target_usd(self, coin: str) -> float:
        leverage = max(int(self.market_leverage.get(self.market_name(coin), 1)), 1)
        return self.opening_exposure_notional_usd() / leverage

    def post_only_safe_price(self, coin: str, side: str, price: float) -> float:
        safe_price = price
        try:
            bids, asks = self.exchange.fetch_cached_order_book(coin, depth=1)
        except Exception:
            bids, asks = [], []
        if bids and asks and bids[0].price > 0 and asks[0].price > bids[0].price:
            mid = 0.5 * (bids[0].price + asks[0].price)
            step = max(self.exchange.price_step(coin, mid), 1e-12)
            if side == "buy":
                safe_price = min(safe_price, max(bids[0].price, asks[0].price - step))
            elif side == "sell":
                safe_price = max(safe_price, min(asks[0].price, bids[0].price + step))
        rounded = self.exchange.round_price(coin, side, safe_price)
        if abs(rounded - price) > 1e-12:
            self.log.info(
                "post-only target clamped market=%s side=%s original_price=%s safe_price=%s",
                coin,
                side,
                price,
                rounded,
            )
        return rounded

    def post_only_safe_target(self, coin: str, target: DesiredOrder) -> DesiredOrder:
        price = self.post_only_safe_price(coin, target.side, target.price)
        if abs(price - target.price) <= 1e-12:
            return target
        return DesiredOrder(target.level, target.side, price, target.size, target.reduce_only)

    def sync_orders(self, coin: str, desired: List[DesiredOrder], quote: Quote, *, place_missing: bool = True) -> None:
        if self.quote_blocked_by_cancel_guard(coin, "sync_orders_start"):
            return
        if self.action_cooldown_active(f"{coin}:sync_orders_start"):
            return
        current = self.exchange.fetch_cached_open_orders(coin)
        desired_by_side = {
            side: sorted((order for order in desired if order.side == side), key=lambda order: order.level)
            for side in ("buy", "sell")
        }
        desired_sides = set(desired_by_side.keys())
        desired_sides = {side for side in desired_sides if desired_by_side[side]}

        # Cancellation remains necessary only when a side or extra level is no longer wanted.
        for order in list(current):
            if order.side not in desired_sides:
                self.cancel_and_wait(order, "side_not_desired")

        def match_current_orders(*, cancel_extras: bool) -> Tuple[List[OpenOrder], List[Tuple[OpenOrder, DesiredOrder]], List[DesiredOrder]]:
            current_orders = self.exchange.fetch_cached_open_orders(coin)
            matched_orders: List[Tuple[OpenOrder, DesiredOrder]] = []
            missing_orders: List[DesiredOrder] = []
            for side in ("buy", "sell"):
                same_side = [order for order in current_orders if order.side == side]
                same_side.sort(key=lambda order: order.price, reverse=side == "buy")
                side_desired = desired_by_side[side]
                if len(same_side) > len(side_desired):
                    if cancel_extras:
                        for extra in same_side[len(side_desired):]:
                            self.cancel_and_wait(extra, "extra_same_side")
                    same_side = same_side[:len(side_desired)]
                matched_orders.extend(zip(same_side, side_desired))
                missing_orders.extend(side_desired[len(same_side):])
            return current_orders, matched_orders, missing_orders

        current, matched, missing = match_current_orders(cancel_extras=True)
        matched = [(order, self.post_only_safe_target(coin, target)) for order, target in matched]
        missing = [self.post_only_safe_target(coin, target) for target in missing]

        needs_edit = any(self.should_edit(order, target) for order, target in matched)
        edit_due = time.time() - self.last_bulk_edit.get(coin, 0.0) >= settings.BULK_EDIT_INTERVAL_S
        if matched and needs_edit and edit_due:
            if self.quote_blocked_by_cancel_guard(coin, "before_bulk_edit"):
                return
            edits = [
                OrderEdit(order=order, price=target.price, size=target.size, reduce_only=target.reduce_only)
                for order, target in matched
            ]
            try:
                results = self.exchange.bulk_edit_orders(edits)
                self.last_bulk_edit[coin] = time.time()
                for result, (_, target) in zip(results, matched):
                    self.record_edited_order(result, target, quote)
                self.db.audit("INFO", "orders_bulk_edited", "resting orders amended in place", {"count": len(results)}, coin=coin)
                self.log.info(
                    "bulk edited market=%s count=%s exchange_order_ids=%s",
                    coin,
                    len(results),
                    [result.exchange_order_id for result in results],
                )
            except Exception as exc:
                self.db.audit("ERROR", "orders_bulk_edit_failed", str(exc), coin=coin)
                if self.is_request_limit_error(str(exc)):
                    self.mark_action_rate_limited(str(exc))
                    return
                self.exchange.fetch_open_orders(coin, force=True)
                return
        else:
            for order, target in matched:
                self.mark_order_seen(order, target.reduce_only)

        refreshed = self.exchange.fetch_cached_open_orders(coin)
        gross = sum((order.price or 0.0) * (order.size or 0.0) for order in refreshed)
        if len(refreshed) == 1:
            self.refresh_inventory_balances(force=True)
            live_inventory = self.inventory_for_quoting(coin, self.inventory_balance(coin))
            close_side = "sell" if live_inventory > 1e-12 else "buy" if live_inventory < -1e-12 else None
            if close_side and refreshed[0].side == close_side:
                close_size = self.exchange.round_size(coin, abs(live_inventory))
                side_targets = desired_by_side.get(close_side) or []
                target_price = side_targets[0].price if side_targets else (quote.ask if close_side == "sell" else quote.bid)
                if close_size > 0 and target_price is not None:
                    target = self.post_only_safe_target(coin, DesiredOrder(0, close_side, target_price, close_size, True))
                    order = refreshed[0]
                    if self.should_edit(order, target):
                        if self.quote_blocked_by_cancel_guard(coin, "before_remaining_reduce_only_edit"):
                            return
                        try:
                            results = self.exchange.bulk_edit_orders(
                                [OrderEdit(order=order, price=target.price, size=target.size, reduce_only=True)]
                            )
                            self.last_bulk_edit[coin] = time.time()
                            for result in results:
                                self.record_edited_order(result, target, quote)
                            self.db.audit(
                                "INFO",
                                "remaining_order_forced_reduce_only",
                                "one open order with live inventory; forced remaining order to reduce-only close",
                                {
                                    "inventory": live_inventory,
                                    "old_order_id": order.exchange_order_id,
                                    "new_order_ids": [result.exchange_order_id for result in results],
                                },
                                coin=coin,
                            )
                            self.log.info(
                                "forced remaining order reduce-only market=%s side=%s inventory=%s exchange_order_ids=%s",
                                coin,
                                close_side,
                                live_inventory,
                                [result.exchange_order_id for result in results],
                            )
                        except Exception as exc:
                            self.db.audit("ERROR", "remaining_reduce_only_edit_failed", str(exc), coin=coin)
                            if self.is_request_limit_error(str(exc)):
                                self.mark_action_rate_limited(str(exc))
                                return
                            self.exchange.fetch_open_orders(coin, force=True)
                        return
                    self.log_remaining_order_aligned(coin, order, target)
                    self.mark_order_seen(order, True)
                    return
        if missing and not place_missing:
            self.db.audit(
                "INFO",
                "missing_orders_deferred_after_fill",
                "open-order count decreased this cycle; edited remaining orders and deferred new placements",
                {"missing_count": len(missing), "open_order_count": len(refreshed)},
                coin=coin,
            )
            self.log.info(
                "deferred missing placements after fill market=%s missing_count=%s open_order_count=%s",
                coin,
                len(missing),
                len(refreshed),
            )
            return
        if missing and any(not target.reduce_only for target in missing) and (matched or self.last_open_order_keys.get(coin)):
            before_keys = self.open_order_keys(refreshed)
            self.exchange.fetch_open_orders(coin, force=True)
            self.refresh_inventory_balances(force=True)
            live_inventory = self.inventory_balance(coin)
            refreshed, _matched_after_refresh, missing = match_current_orders(cancel_extras=False)
            missing = [self.post_only_safe_target(coin, target) for target in missing]
            gross = sum((order.price or 0.0) * (order.size or 0.0) for order in refreshed)
            if abs(live_inventory) > 1e-12:
                self.last_bulk_edit[coin] = 0.0
                self.db.audit(
                    "INFO",
                    "opening_orders_deferred_live_inventory",
                    "live inventory exists before missing opening placement; deferring until next quote cycle",
                    {
                        "inventory": live_inventory,
                        "missing_count": len(missing),
                        "open_order_count": len(refreshed),
                        "before_open_order_keys": sorted(before_keys),
                        "after_open_order_keys": sorted(self.open_order_keys(refreshed)),
                    },
                    coin=coin,
                )
                self.log.info(
                    "deferred missing opening placements because live inventory changed market=%s inventory=%s missing_count=%s open_order_ids=%s",
                    coin,
                    live_inventory,
                    len(missing),
                    [order.exchange_order_id for order in refreshed],
                )
                return
        for target in missing:
            if self.quote_blocked_by_cancel_guard(coin, "before_place_missing"):
                return
            if not target.reduce_only and target.size * target.price + 1e-9 < settings.MIN_OPEN_ORDER_NOTIONAL_USD:
                self.db.audit("WARNING", "small_open_order_skip", "opening order below minimum notional", {"size": target.size, "price": target.price}, coin=coin)
                continue

            if not target.reduce_only and gross + target.size * target.price > settings.MAX_GROSS_OPEN_ORDER_NOTIONAL_USD:
                self.db.audit("WARNING", "gross_cap_skip", "open-order gross cap reached", {"gross": gross, "new": target.size * target.price}, coin=coin)
                continue

            placed = self.place_order(coin, target.side, target.size, target.price, target.reduce_only, quote)
            if not placed:
                if self.action_cooldown_active(f"{coin}:after_place_rejected"):
                    return
                if bool(getattr(settings, "FORCE_REST_REFRESH_AFTER_ORDER_REJECT", False)):
                    self.exchange.fetch_open_orders(coin, force=True)
                    self.refresh_inventory_balances(force=True)
                return
            gross += target.size * target.price

    @staticmethod
    def edit_move_bps(order: OpenOrder, target: DesiredOrder) -> float:
        if order.price <= 0:
            return float("inf")
        return abs(target.price - order.price) / order.price * 10000.0

    def should_edit(self, order: OpenOrder, target: DesiredOrder) -> bool:
        move_bps = self.edit_move_bps(order, target)
        size_changed = abs(target.size - order.size) > 1e-12
        reduce_only_changed = bool(order.raw.get("reduceOnly") or order.raw.get("reduce_only")) != target.reduce_only
        return move_bps >= settings.REPRICE_IF_PRICE_MOVES_BPS or size_changed or reduce_only_changed

    def log_remaining_order_aligned(self, coin: str, order: OpenOrder, target: DesiredOrder) -> None:
        now = time.time()
        if now - self.last_remaining_order_skip_log.get(coin, 0.0) < 1.0:
            return
        self.last_remaining_order_skip_log[coin] = now
        reduce_only_now = bool(order.raw.get("reduceOnly") or order.raw.get("reduce_only"))
        self.log.info(
            "remaining order not edited market=%s side=%s oid=%s order_price=%s target_price=%s move_bps=%.6f threshold_bps=%s order_size=%s target_size=%s order_reduce_only=%s target_reduce_only=%s",
            coin,
            order.side,
            order.exchange_order_id,
            order.price,
            target.price,
            self.edit_move_bps(order, target),
            settings.REPRICE_IF_PRICE_MOVES_BPS,
            order.size,
            target.size,
            reduce_only_now,
            target.reduce_only,
        )

    def cancel_and_wait(self, order: OpenOrder, reason: str) -> bool:
        ok, raw = self.exchange.cancel_order(
            order.market,
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id,
        )
        self.db.audit("INFO" if ok else "ERROR", "order_cancel", f"cancel {reason}", {"order": asdict(order), "response": raw}, coin=order.market, client_order_id=order.client_order_id)
        if not ok:
            raw_message = json_dumps(raw)
            if self.is_request_limit_error(raw_message):
                self.mark_action_rate_limited(raw_message)
            return False
        gone = self.exchange.wait_until_order_gone(order.market, order.client_order_id, order.exchange_order_id, settings.CANCEL_WAIT_S, settings.CANCEL_POLL_S)
        if not gone:
            self.db.audit("ERROR", "order_cancel_timeout", "order still open after cancel wait", {"order": asdict(order)}, coin=order.market, client_order_id=order.client_order_id)
        return gone

    def cancel_all_for_coin(self, coin: str, reason: str) -> None:
        submitted: List[OpenOrder] = []
        for order in list(self.exchange.fetch_cached_open_orders(coin)):
            ok, raw = self.exchange.cancel_order(
                order.market,
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id,
            )
            self.db.audit(
                "INFO" if ok else "ERROR",
                "order_cancel",
                f"cancel {reason}",
                {"order": asdict(order), "response": raw},
                coin=order.market,
                client_order_id=order.client_order_id,
            )
            if ok:
                submitted.append(order)
            elif self.is_request_limit_error(json_dumps(raw)):
                self.mark_action_rate_limited(json_dumps(raw))
        for order in submitted:
            gone = self.exchange.wait_until_order_gone(
                order.market,
                order.client_order_id,
                order.exchange_order_id,
                settings.CANCEL_WAIT_S,
                settings.CANCEL_POLL_S,
            )
            if not gone:
                self.db.audit(
                    "ERROR",
                    "order_cancel_timeout",
                    "order still open after cancel wait",
                    {"order": asdict(order)},
                    coin=order.market,
                    client_order_id=order.client_order_id,
                )

    def cancel_symbol_orders_after_fill(self, coin: str, reason: str) -> None:
        coin = self.market_name(coin)
        self.mark_cancel_on_fill_guard(coin, reason)
        force_rest_refresh = bool(getattr(settings, "CANCEL_ON_FILL_FORCE_REST_REFRESH", False))
        if force_rest_refresh:
            self.exchange.fetch_open_orders(coin, force=True)
        open_orders = self.exchange.fetch_cached_open_orders(coin)
        self.db.audit(
            "WARNING",
            "fill_detected_cancel_symbol_orders",
            "fill detected; cancelling remaining open orders for symbol",
            {"reason": reason, "open_order_count": len(open_orders), "order_ids": [order.exchange_order_id for order in open_orders]},
            coin=coin,
        )
        self.log.warning(
            "fill detected; cancelling remaining open orders market=%s reason=%s count=%s ids=%s",
            coin,
            reason,
            len(open_orders),
            [order.exchange_order_id for order in open_orders],
        )
        self.cancel_all_for_coin(coin, reason)
        if force_rest_refresh:
            self.exchange.fetch_open_orders(coin, force=True)
            self.refresh_inventory_balances(force=True)
        self.last_bulk_edit[coin] = 0.0
        orders = self.exchange.fetch_cached_open_orders(coin)
        self.last_open_order_keys[coin] = self.open_order_keys(orders)
        self.last_open_order_snapshot[coin] = self.copy_open_orders(orders)
        self.clear_cancel_on_fill_guard_if_flat(coin)

    def place_order(self, coin: str, side: str, size: float, price: float, reduce_only: bool, quote: Quote) -> bool:
        if self.quote_blocked_by_cancel_guard(coin, "place_order"):
            return False
        now = utc_now_iso()
        client_order_id = self.exchange.make_client_order_id()
        base = {
            "client_order_id": client_order_id,
            "exchange_order_id": None,
            "coin": coin,
            "symbol": self.exchange.symbol_for_market(coin),
            "side": side,
            "order_type": "limit",
            "time_in_force": self.exchange.post_only_time_in_force,
            "post_only": True,
            "reduce_only": reduce_only,
            "price": price,
            "size": size,
            "notional": size * price,
            "status": "submitting",
            "status_reason": None,
            "model": "Avellaneda-Stoikov",
            "reservation_price": quote.reservation_price,
            "half_spread": quote.half_spread,
            "mid_price": quote.mid,
            "volatility": quote.sigma_per_s,
            "inventory": quote.inventory_base,
            "gamma": quote.gamma,
            "k": quote.k,
            "horizon_seconds": quote.horizon_seconds,
            "raw_json": None,
            "created_at": now,
            "updated_at": now,
            "last_seen_at": None,
        }
        self.db.upsert("orders", "client_order_id", base)
        try:
            result = self.exchange.place_limit_order(coin, side, size, price, reduce_only=reduce_only, post_only=True, client_order_id=client_order_id)
            row = dict(base)
            row.update({
                "exchange_order_id": result.exchange_order_id,
                "status": result.status,
                "raw_json": json_dumps(result.raw),
                "updated_at": utc_now_iso(),
                "last_seen_at": utc_now_iso(),
            })
            self.db.upsert("orders", "client_order_id", row)
            self.db.audit("INFO", "order_accepted", "order accepted", {"exchange_order_id": result.exchange_order_id, "raw": result.raw}, coin=coin, client_order_id=client_order_id)
            self.log.info("placed market=%s side=%s size=%s price=%s exchange_order_id=%s client_order_id=%s", coin, side, result.size, result.price, result.exchange_order_id, client_order_id)
            return True
        except Exception as exc:
            row = dict(base)
            row.update({"status": "rejected", "status_reason": str(exc), "updated_at": utc_now_iso(), "raw_json": json_dumps({"error": str(exc)})})
            self.db.upsert("orders", "client_order_id", row)
            self.db.audit("ERROR", "order_rejected", str(exc), coin=coin, client_order_id=client_order_id)
            if self.is_request_limit_error(str(exc)):
                self.mark_action_rate_limited(str(exc))
            self.log.error("order rejected coin=%s side=%s error=%s", coin, side, exc)
            return False

    def record_edited_order(self, result, target: DesiredOrder, quote: Quote) -> None:  # type: ignore[no-untyped-def]
        self.mark_order_seen(
            OpenOrder(
                exchange=result.exchange,
                market=result.market,
                symbol=result.symbol,
                exchange_order_id=result.exchange_order_id,
                client_order_id=result.client_order_id,
                side=result.side,
                price=result.price,
                size=result.size,
                timestamp_ms=None,
                raw={"reduceOnly": target.reduce_only, "modify_response": result.raw},
            ),
            target.reduce_only,
            status_reason="bulk_edited",
        )

    def mark_order_seen(self, order: OpenOrder, reduce_only: Optional[bool] = None, status_reason: str = "seen_on_exchange") -> None:
        if not order.client_order_id:
            return
        now = utc_now_iso()
        if reduce_only is None:
            reduce_only = bool(order.raw.get("reduceOnly") or order.raw.get("reduce_only"))
        self.db.upsert("orders", "client_order_id", {
            "client_order_id": order.client_order_id,
            "exchange_order_id": order.exchange_order_id,
            "coin": order.market,
            "symbol": order.symbol,
            "side": order.side,
            "order_type": "limit",
            "time_in_force": self.exchange.post_only_time_in_force,
            "post_only": True,
            "reduce_only": reduce_only,
            "price": order.price,
            "size": order.size,
            "notional": order.price * order.size,
            "status": "open",
            "status_reason": status_reason,
            "model": "Avellaneda-Stoikov",
            "raw_json": json_dumps(order.raw),
            "created_at": now,
            "updated_at": now,
            "last_seen_at": now,
        })

    def record_quote(self, q: Quote) -> None:
        self.db.upsert("model_quotes", "quote_id", {
            "quote_id": uuid.uuid4().hex,
            "coin": q.coin,
            "symbol": q.symbol,
            "mid_price": q.mid,
            "best_bid": q.best_bid,
            "best_ask": q.best_ask,
            "bid_price": q.bid,
            "ask_price": q.ask,
            "reservation_price": q.reservation_price,
            "half_spread": q.half_spread,
            "inventory": q.inventory_base,
            "volatility": q.sigma_per_s,
            "gamma": q.gamma,
            "k": q.k,
            "horizon_seconds": q.horizon_seconds,
            "created_at": utc_now_iso(),
        })

    def poll_fills_if_due(self) -> None:
        poll_interval_s = float(getattr(settings, "FETCH_FILLS_S", 0.0))
        if poll_interval_s <= 0:
            return
        if time.time() - self.last_fills_poll < poll_interval_s:
            return
        end_ms = int(time.time() * 1000)
        lookback_hours = max(float(getattr(settings, "USER_FILLS_BACKFILL_LOOKBACK_HOURS", 24.0)), 0.0)
        start_ms = int(end_ms - lookback_hours * 3600.0 * 1000)
        try:
            fills = self.exchange.fetch_recent_fills(
                start_ms,
                end_ms,
                bool(getattr(settings, "USER_FILLS_AGGREGATE_BY_TIME", False)),
            )
            filled_markets = set()
            for f in fills:
                if self.record_fill(f):
                    market = self.market_name(f.market)
                    filled_markets.add(market)
                    if bool(getattr(settings, "CANCEL_SYMBOL_ORDERS_ON_FILL", True)):
                        self.mark_cancel_on_fill_guard(market, "fill_poll_new_fill")
            if bool(getattr(settings, "CANCEL_SYMBOL_ORDERS_ON_FILL", True)):
                for market in sorted(filled_markets):
                    self.cancel_symbol_orders_after_fill(market, "fill_detected_fill_poll")
        except Exception as exc:
            self.db.audit("WARNING", "fills_poll_failed", str(exc))
        self.last_fills_poll = time.time()

    @staticmethod
    def fill_id(fill: TradeFill) -> str:
        return f"{fill.exchange}:{fill.market}:{fill.trade_id}:{fill.exchange_order_id}:{fill.timestamp_ms}"

    def record_fill(self, fill: TradeFill) -> bool:
        fill_id = self.fill_id(fill)
        is_new = fill_id not in self.seen_fill_ids
        self.seen_fill_ids.add(fill_id)
        self.db.upsert("fills", "fill_id", {
            "fill_id": fill_id,
            "client_order_id": fill.client_order_id,
            "exchange_order_id": fill.exchange_order_id,
            "exchange_trade_id": fill.trade_id,
            "coin": fill.market,
            "symbol": fill.symbol,
            "side": fill.side,
            "price": fill.price,
            "size": fill.size,
            "notional": fill.price * fill.size,
            "fee": fill.fee,
            "fee_currency": fill.fee_currency,
            "timestamp_ms": fill.timestamp_ms,
            "raw_json": json_dumps(fill.raw),
            "created_at": utc_now_iso(),
        })
        return is_new


def create_script(connector: ExchangeConnector) -> StrategyScript:
    return MarketMakingScript(connector)
