from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import asdict
from typing import Any, Dict

import settings
from connectors.factory import create_connector


def _nonzero_balance(balance: Dict[str, Any]) -> bool:
    return float(balance.get("total") or 0.0) != 0.0 or float(balance.get("hold") or 0.0) != 0.0


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    connector = create_connector()
    user_state = connector.fetch_account_summary()
    fills = connector.fetch_recent_fills(int((time.time() - 24 * 3600) * 1000))
    output = {
        "connector": connector.name,
        "account_address": connector.account_address,
        "signer_address": connector.signer_address,
        "signer_role": connector.fetch_signer_role(),
        "user_rate_limit": connector.fetch_user_rate_limit(),
        "account_summary": user_state,
        "positions": connector.fetch_positions(),
        "spot_balances_nonzero": [balance for balance in connector.fetch_spot_balances() if _nonzero_balance(balance)],
        "open_orders": [asdict(order) for order in connector.fetch_open_orders()],
        "open_order_details": connector.fetch_open_order_details(),
        "recent_fills_24h": [asdict(fill) for fill in fills],
    }
    print(json.dumps(output, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
