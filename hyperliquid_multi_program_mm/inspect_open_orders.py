from __future__ import annotations

import json
import logging
import sys
from dataclasses import asdict

import settings
from connectors.factory import create_connector


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    connector = create_connector()
    for market in settings.MARKETS:
        print(f"\n=== {connector.name} {market.upper()} open orders ===")
        orders = connector.fetch_open_orders(market)
        for order in orders:
            print(json.dumps(asdict(order), indent=2, default=str))
        if not orders:
            print("none")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
