from __future__ import annotations

import importlib
import re

import settings
from connectors.base import ExchangeConnector


def create_connector(name: str | None = None) -> ExchangeConnector:
    connector_name = (name or settings.CONNECTOR).lower().strip()
    if not re.fullmatch(r"[a-z][a-z0-9_]*", connector_name):
        raise RuntimeError(f"Invalid connector name: {connector_name!r}")
    try:
        module = importlib.import_module(f"connectors.{connector_name}_connector")
    except ModuleNotFoundError as exc:
        if exc.name == f"connectors.{connector_name}_connector":
            raise RuntimeError(f"Unknown connector: {connector_name}") from exc
        raise
    factory = getattr(module, "create_connector", None)
    if not callable(factory):
        raise RuntimeError(f"Connector module connectors.{connector_name}_connector has no create_connector()")
    connector = factory()
    if not isinstance(connector, ExchangeConnector):
        raise RuntimeError(f"Connector factory returned invalid object: {type(connector).__name__}")
    return connector
