from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class HandlerResult:
    success: bool
    output: Optional[str] = None
    error_message: Optional[str] = None
    failure_reason: Optional[str] = None
    duration_ms: int = 0


class BaseProtocolHandler(ABC):
    """
    All protocol handlers implement this interface.

    connect()    — establish session, raise on failure
    execute()    — run a single command/OID, return raw string output
    disconnect() — always called in finally; must not raise
    """

    @abstractmethod
    def connect(self, device: dict) -> None:
        """Establish connection to the device. Raise on failure."""

    @abstractmethod
    def execute(self, command: str, timeout: int = 30) -> str:
        """Execute a command and return the raw output string."""

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection. Must not raise."""

    @abstractmethod
    def enter_config_mode(self) -> None:
        """Enter configuration mode (for SET operations)."""

    @abstractmethod
    def exit_config_mode(self) -> None:
        """Exit configuration mode (for SET operations)."""
