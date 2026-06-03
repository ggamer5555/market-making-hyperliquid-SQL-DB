from __future__ import annotations

import logging
import sys
from typing import List

import settings
from common import redacted_settings
from connectors.factory import create_connector
from scripts.factory import create_script


def configure_logging() -> None:
    level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if settings.LOG_FILE:
        handlers.append(logging.FileHandler(settings.LOG_FILE, encoding="utf-8"))
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s", handlers=handlers)


def validate_settings() -> None:
    if not settings.CONNECTOR:
        raise RuntimeError("settings.CONNECTOR must not be empty")
    if not settings.STRATEGY_SCRIPT:
        raise RuntimeError("settings.STRATEGY_SCRIPT must not be empty")
    if not settings.MARKETS:
        raise RuntimeError("settings.MARKETS must not be empty")
    if settings.CONNECTOR.lower() == "hyperliquid":
        if not settings.WALLET_ADDRESS.startswith("0x") or "PASTE" in settings.WALLET_ADDRESS:
            raise RuntimeError("Edit settings.py and paste the Hyperliquid main-account WALLET_ADDRESS.")
        if not settings.PRIVATE_KEY.startswith("0x") or "PASTE" in settings.PRIVATE_KEY:
            raise RuntimeError("Edit settings.py and paste the approved Hyperliquid API-wallet PRIVATE_KEY.")


def main() -> int:
    configure_logging()
    validate_settings()
    log = logging.getLogger("main")
    log.warning("LIVE STRATEGY STARTING settings=%s", redacted_settings(settings))
    connector = create_connector()
    script = create_script(connector)
    log.warning("loaded connector=%s strategy_script=%s", connector.name, settings.STRATEGY_SCRIPT)
    script.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
