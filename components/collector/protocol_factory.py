from components.collector.handlers.base import BaseProtocolHandler
from components.collector.handlers.ssh_handler import SSHHandler
from components.collector.handlers.telnet_handler import TelnetHandler
from components.collector.handlers.snmp_handler import SNMPHandler
from components.collector.handlers.rest_handler import RESTHandler

_HANDLERS: dict[str, type[BaseProtocolHandler]] = {
    "SSH":    SSHHandler,
    "TELNET": TelnetHandler,
    "SNMP":   SNMPHandler,
    "REST":   RESTHandler,
}


def get_handler(protocol: str) -> BaseProtocolHandler:
    """
    Return a fresh handler instance for the given protocol string.
    Raises ValueError for unknown protocols.
    """
    proto = protocol.upper()
    cls = _HANDLERS.get(proto)
    if cls is None:
        raise ValueError(
            f"Unknown protocol '{protocol}'. "
            f"Supported: {list(_HANDLERS.keys())}"
        )
    return cls()
