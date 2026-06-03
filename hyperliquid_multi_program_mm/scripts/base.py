from __future__ import annotations

from abc import ABC, abstractmethod


class StrategyScript(ABC):
    """Common lifecycle used by the main launcher."""

    @abstractmethod
    def run_forever(self) -> None:
        raise NotImplementedError
