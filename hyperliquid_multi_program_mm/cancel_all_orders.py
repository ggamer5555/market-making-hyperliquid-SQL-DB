from __future__ import annotations

import logging
import sys

import settings
from common import redacted_settings
from connectors.factory import create_connector
from main import validate_settings


def configure_logging() -> None:
    logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s %(message)s", handlers=[logging.StreamHandler(sys.stdout)])


def main() -> int:
    configure_logging()
    validate_settings()
    log = logging.getLogger("cancel_all_orders")
    log.warning("CANCEL ALL STARTING settings=%s", redacted_settings(settings))
    connector = create_connector()
    for market in settings.MARKETS:
        orders = connector.fetch_open_orders(market)
        log.warning("connector=%s market=%s open_order_count=%s", connector.name, market, len(orders))
        for order in orders:
            ok, raw = connector.cancel_order(
                market,
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id,
            )
            log.warning(
                "cancel market=%s exchange_order_id=%s client_order_id=%s ok=%s raw=%s",
                market,
                order.exchange_order_id,
                order.client_order_id,
                ok,
                raw,
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
