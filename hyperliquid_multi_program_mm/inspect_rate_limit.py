from __future__ import annotations

import json
import logging
import sys
from typing import Any, Dict, Optional

import settings
from connectors.factory import create_connector
from main import validate_settings


def _number(raw: Dict[str, Any], *names: str) -> Optional[float]:
    for name in names:
        value = raw.get(name)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _summary(raw: Dict[str, Any]) -> Dict[str, Any]:
    n_requests_used = _number(raw, "nRequestsUsed", "n_requests_used", "requestsUsed")
    n_requests_cap = _number(raw, "nRequestsCap", "n_requests_cap", "requestsCap")
    cumulative_volume = _number(raw, "cumVlm", "cumulativeVolume", "cumulative_volume")
    estimated_cap = None
    if cumulative_volume is not None:
        estimated_cap = 10000.0 + cumulative_volume
    cap = n_requests_cap if n_requests_cap is not None else estimated_cap
    remaining = cap - n_requests_used if cap is not None and n_requests_used is not None else None
    return {
        "n_requests_used": n_requests_used,
        "n_requests_cap": n_requests_cap,
        "cumulative_volume_usdc": cumulative_volume,
        "estimated_default_cap": estimated_cap,
        "estimated_remaining_requests": remaining,
        "rate_limited": remaining is not None and remaining < 0,
    }


def main() -> int:
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.StreamHandler(sys.stderr)],
    )
    validate_settings()
    connector = create_connector()
    raw = connector.fetch_user_rate_limit()
    output = {
        "account_address": connector.account_address,
        "summary": _summary(raw),
        "raw": raw,
    }
    print(json.dumps(output, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
