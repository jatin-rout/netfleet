import socket

from netmiko import ConnectHandler
from netmiko.exceptions import (
    NetmikoAuthenticationException,
    NetmikoTimeoutException,
)

from components.collector.handlers.base import BaseProtocolHandler
from shared.utils.logger import get_logger

logger = get_logger("collector.telnet_handler")

_NETMIKO_TELNET_TYPE = {
    "cisco_ios":     "cisco_ios_telnet",
    "huawei_vrp":    "huawei_telnet",
    "juniper_junos": "juniper_junos_telnet",
    "bdcom":         "cisco_ios_telnet",
    "zte_zxros":     "cisco_ios_telnet",
    "utstarcom":     "cisco_ios_telnet",
}


class TelnetHandler(BaseProtocolHandler):
    """
    Telnet protocol handler using Netmiko.

    Interface is symmetric with SSHHandler — same method signatures,
    same exception propagation contract.
    """

    def __init__(self):
        self._connection = None
        self._host: str = "unknown"
        self._vendor: str = "unknown"

    def connect(self, device: dict) -> None:
        self._host = device.get("ip_address", "unknown")
        self._vendor = device.get("vendor", "cisco_ios")
        device_type = _NETMIKO_TELNET_TYPE.get(self._vendor, "cisco_ios_telnet")

        params = {
            "device_type":     device_type,
            "host":            self._host,
            "username":        device.get("username", "admin"),
            "password":        device.get("password", ""),
            "timeout":         int(device.get("connect_timeout", 30)),
            "session_timeout": int(device.get("session_timeout", 60)),
            "banner_timeout":  20,
            "conn_timeout":    15,
            "fast_cli":        False,
        }
        secret = device.get("enable_secret")
        if secret:
            params["secret"] = secret

        logger.debug(
            f"Telnet connecting — host={self._host} vendor={self._vendor} "
            f"device_type={device_type} timeout={params['timeout']}s"
        )
        # Let NetmikoAuthenticationException, NetmikoTimeoutException,
        # socket.error propagate to DeviceExecutor.
        self._connection = ConnectHandler(**params)
        logger.info(
            f"Telnet connected — host={self._host} vendor={self._vendor}"
        )

    def execute(self, command: str, timeout: int = 30) -> str:
        if self._connection is None:
            raise RuntimeError(
                f"Telnet execute called with no active session (host={self._host})"
            )
        logger.debug(
            f"Telnet execute — host={self._host} cmd='{command}' timeout={timeout}s"
        )
        output = self._connection.send_command(
            command,
            read_timeout=timeout,
            expect_string=r"[#>$]",
        )
        logger.debug(
            f"Telnet execute done — host={self._host} output_chars={len(output)}"
        )
        return output

    def enter_config_mode(self) -> None:
        if self._connection is None:
            raise RuntimeError(
                f"Telnet enter_config_mode called with no session (host={self._host})"
            )
        logger.debug(f"Telnet enter_config_mode — host={self._host}")
        self._connection.enable()
        self._connection.config_mode()
        logger.debug(f"Telnet in config mode — host={self._host}")

    def exit_config_mode(self) -> None:
        if self._connection is None:
            return
        try:
            self._connection.exit_config_mode()
            logger.debug(f"Telnet exited config mode — host={self._host}")
        except Exception as exc:
            logger.warning(
                f"Telnet exit_config_mode error (ignored) — "
                f"host={self._host}: {type(exc).__name__}: {exc}"
            )

    def send_config_set(self, commands: list, timeout: int = 60) -> str:
        if self._connection is None:
            raise RuntimeError(
                f"Telnet send_config_set called with no session (host={self._host})"
            )
        logger.debug(
            f"Telnet send_config_set — host={self._host} "
            f"lines={len(commands)} timeout={timeout}s"
        )
        output = self._connection.send_config_set(
            commands, read_timeout=timeout
        )
        logger.debug(
            f"Telnet send_config_set done — host={self._host} "
            f"output_chars={len(output)}"
        )
        return output

    def disconnect(self) -> None:
        if self._connection is not None:
            try:
                self._connection.disconnect()
                logger.debug(f"Telnet disconnected — host={self._host}")
            except Exception as exc:
                logger.warning(
                    f"Telnet disconnect error (ignored) — "
                    f"host={self._host}: {type(exc).__name__}: {exc}"
                )
            finally:
                self._connection = None
