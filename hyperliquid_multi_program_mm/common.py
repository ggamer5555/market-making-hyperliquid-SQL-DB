from __future__ import annotations

import json
import math
import os
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any, Dict, Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def utc_ms() -> int:
    return int(time.time() * 1000)


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def make_cloid() -> str:
    """Hyperliquid cloid: 0x + 32 hex chars, exactly 128 bits."""
    return "0x" + uuid.uuid4().hex


def _strip_usdc_quote_suffix(value: str) -> str:
    value = str(value).strip()
    upper = value.upper()
    for suffix in ("/USDC:USDC", "/USDC", "-USDC"):
        if upper.endswith(suffix):
            return value[: -len(suffix)]
    return value


def _normalize_coin_part(value: str) -> str:
    value = str(value).strip()
    if any(ch.islower() for ch in value) and any(ch.isupper() for ch in value):
        return value
    return value.upper()


def normalize_market_name(symbol_or_coin: str) -> str:
    """Return the Hyperliquid coin string used by /info and exchange actions.

    Examples:
    - BTC-USDC -> BTC
    - BTC/USDC:USDC -> BTC
    - xyz:AMD-USDC -> xyz:AMD

    HIP3 dex prefixes are case-sensitive on Hyperliquid, so the dex is kept
    lowercase while the common UI quote suffix is stripped from the coin part.
    """

    raw = str(symbol_or_coin or "").strip()
    if not raw:
        return ""
    raw = _strip_usdc_quote_suffix(raw)
    if ":" in raw:
        dex, coin = raw.split(":", 1)
        coin = _strip_usdc_quote_suffix(coin)
        return f"{dex.strip().lower()}:{_normalize_coin_part(coin)}"
    return _normalize_coin_part(raw)


def coin_to_symbol(coin: str) -> str:
    return f"{normalize_market_name(coin)}/USDC:USDC"


def symbol_to_coin(symbol_or_coin: str) -> str:
    return normalize_market_name(symbol_or_coin)


def step_round(x: float, step: float, rounding) -> float:
    if step <= 0:
        return float(x)
    dx = Decimal(str(x))
    ds = Decimal(str(step))
    q = (dx / ds).to_integral_value(rounding=rounding)
    return float(q * ds)


def floor_to_step(x: float, step: float) -> float:
    return step_round(x, step, ROUND_FLOOR)


def ceil_to_step(x: float, step: float) -> float:
    return step_round(x, step, ROUND_CEILING)


def price_to_step_by_side(side: str, price: float, step: float) -> float:
    side = side.lower()
    if side == "buy":
        return step_round(price, step, ROUND_FLOOR)
    return step_round(price, step, ROUND_CEILING)


def is_benign_cancel_error(msg: str) -> bool:
    s = str(msg).lower()
    return any(
        needle in s
        for needle in (
            "never placed",
            "already canceled",
            "already cancelled",
            "already filled",
            "or filled",
            "not found",
            "does not exist",
            "unknownoid",
            "order was never placed",
        )
    )


class SingleInstanceLock:
    """PID lock file that automatically clears verified stale locks."""

    def __init__(self, path: str):
        self.path = Path(path)

    @staticmethod
    def _pid_is_running(pid: int) -> bool:
        if pid <= 0:
            return False
        if os.name == "nt":
            import ctypes
            from ctypes import wintypes

            process_query_limited_information = 0x1000
            still_active = 259
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            kernel32.OpenProcess.restype = wintypes.HANDLE
            kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
            kernel32.GetExitCodeProcess.restype = wintypes.BOOL
            kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
            kernel32.CloseHandle.restype = wintypes.BOOL

            handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
            if not handle:
                # ERROR_INVALID_PARAMETER means there is no process with this PID.
                return ctypes.get_last_error() != 87
            try:
                exit_code = wintypes.DWORD()
                if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                    return True
                return exit_code.value == still_active
            finally:
                kernel32.CloseHandle(handle)

        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False
        return True

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        for _ in range(3):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                try:
                    old = self.path.read_text(encoding="utf-8", errors="ignore").strip()
                    old_stat = self.path.stat()
                except FileNotFoundError:
                    continue

                try:
                    old_pid = int(old)
                except ValueError:
                    old_pid = 0

                if self._pid_is_running(old_pid):
                    raise RuntimeError(f"Lock file belongs to running PID {old_pid}: {self.path}")

                # A newly-created empty file may still be waiting for its PID write.
                if old_pid <= 0 and time.time() - old_stat.st_mtime < 5.0:
                    raise RuntimeError(f"Lock file is being initialized: {self.path}. Old value={old!r}")

                try:
                    current_stat = self.path.stat()
                    current = self.path.read_text(encoding="utf-8", errors="ignore").strip()
                    if current != old or current_stat.st_ino != old_stat.st_ino:
                        continue
                    self.path.unlink()
                except FileNotFoundError:
                    pass
                continue

            try:
                os.write(fd, str(os.getpid()).encode("ascii"))
            finally:
                os.close(fd)
            return

        raise RuntimeError(f"Could not acquire lock after clearing a stale lock: {self.path}")

    def release(self) -> None:
        try:
            if self.path.exists() and self.path.read_text(encoding="utf-8").strip() == str(os.getpid()):
                self.path.unlink()
        except Exception:
            pass


def redacted_settings(settings_module: Any) -> Dict[str, Any]:
    return {
        "connector": getattr(settings_module, "CONNECTOR", ""),
        "strategy_script": getattr(settings_module, "STRATEGY_SCRIPT", ""),
        "wallet_address": getattr(settings_module, "WALLET_ADDRESS", ""),
        "private_key": "***REDACTED***",
        "markets": getattr(settings_module, "MARKETS", getattr(settings_module, "COINS", [])),
        "target_order_notional_usd": getattr(settings_module, "TARGET_ORDER_NOTIONAL_USD", None),
        "min_open_order_notional_usd": getattr(settings_module, "MIN_OPEN_ORDER_NOTIONAL_USD", None),
        "max_long_inventory_notional_usd": getattr(settings_module, "MAX_LONG_INVENTORY_NOTIONAL_USD", None),
        "max_short_inventory_notional_usd": getattr(settings_module, "MAX_SHORT_INVENTORY_NOTIONAL_USD", None),
        "reduce_only_to_close_inventory": getattr(settings_module, "REDUCE_ONLY_TO_CLOSE_INVENTORY", None),
        "orders_per_side": getattr(settings_module, "ORDERS_PER_SIDE", None),
        "sync_max_leverage": getattr(settings_module, "SYNC_MAX_LEVERAGE", None),
        "leverage_is_cross": getattr(settings_module, "LEVERAGE_IS_CROSS", None),
        "leverage_sync_interval_s": getattr(settings_module, "LEVERAGE_SYNC_INTERVAL_S", None),
        "bulk_edit_interval_s": getattr(settings_module, "BULK_EDIT_INTERVAL_S", None),
        "loop_interval_s": getattr(settings_module, "LOOP_INTERVAL_S", None),
        "use_ws_market_data": getattr(settings_module, "USE_WS_MARKET_DATA", None),
        "ws_order_book_stale_s": getattr(settings_module, "WS_ORDER_BOOK_STALE_S", None),
        "ws_bbo_stream_enabled": getattr(settings_module, "WS_BBO_STREAM_ENABLED", None),
        "ws_multi_resolution_book_enabled": getattr(settings_module, "WS_MULTI_RESOLUTION_BOOK_ENABLED", None),
        "ws_book_resolution_priority": getattr(settings_module, "WS_BOOK_RESOLUTION_PRIORITY", None),
        "market_data_recording_enabled": getattr(settings_module, "MARKET_DATA_RECORDING_ENABLED", None),
        "market_data_record_interval_s": getattr(settings_module, "MARKET_DATA_RECORD_INTERVAL_S", None),
        "market_data_retention_hours": getattr(settings_module, "MARKET_DATA_RETENTION_HOURS", None),
        "min_quote_spread_bps": getattr(settings_module, "MIN_QUOTE_SPREAD_BPS", None),
        "use_lob_percentile_guard": getattr(settings_module, "USE_LOB_PERCENTILE_GUARD", None),
        "lob_percentile": getattr(settings_module, "LOB_PERCENTILE", None),
        "manage_all_orders_on_coins": getattr(settings_module, "MANAGE_ALL_ORDERS_ON_COINS", None),
    }
