from __future__ import annotations

import importlib
import re

import settings
from connectors.base import ExchangeConnector
from scripts.base import StrategyScript


def create_script(connector: ExchangeConnector, name: str | None = None) -> StrategyScript:
    script_name = (name or settings.STRATEGY_SCRIPT).lower().strip()
    if not re.fullmatch(r"[a-z][a-z0-9_]*", script_name):
        raise RuntimeError(f"Invalid strategy script name: {script_name!r}")
    try:
        module = importlib.import_module(f"scripts.{script_name}")
    except ModuleNotFoundError as exc:
        if exc.name == f"scripts.{script_name}":
            raise RuntimeError(f"Unknown strategy script: {script_name}") from exc
        raise
    factory = getattr(module, "create_script", None)
    if not callable(factory):
        raise RuntimeError(f"Strategy module scripts.{script_name} has no create_script()")
    script = factory(connector)
    if not isinstance(script, StrategyScript):
        raise RuntimeError(f"Strategy factory returned invalid object: {type(script).__name__}")
    return script
