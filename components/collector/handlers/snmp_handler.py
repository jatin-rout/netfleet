"""
SNMP handler — pysnmp >=4.4,<7 (synchronous blocking API).

The SnmpEngine is created lazily inside connect() so that importing this
module does not crash when pysnmp is not installed (e.g. in unit-test
environments). The ImportError surfaces only when a device actually
tries to use SNMP.
"""
import json
from typing import Optional

try:
    from pysnmp.hlapi import (
        CommunityData,
        ContextData,
        ObjectIdentity,
        ObjectType,
        SnmpEngine,
        UdpTransportTarget,
        UsmUserData,
        bulkCmd,
        getCmd,
        usmAesCfb128Protocol,
        usmHMACSHAAuthProtocol,
    )
    _PYSNMP_AVAILABLE = True
except ImportError:
    _PYSNMP_AVAILABLE = False

from components.collector.handlers.base import BaseProtocolHandler
from shared.utils.logger import get_logger

logger = get_logger("collector.snmp_handler")

_SNMP_TIMEOUT_INDICATOR = "no snmp response received"
_SNMP_UNREACH_INDICATOR = "no response"


class SNMPHandler(BaseProtocolHandler):
    """
    SNMP v2c / v3 handler using pysnmp.

    connect()  — stores config; creates SnmpEngine lazily
    execute()  — accepts JSON list of OID dicts, returns JSON dict of results
    disconnect()— no-op (SNMP is stateless UDP)

    SET operations over SNMP are blocked at the DeviceExecutor level before
    they reach this handler.
    """

    def __init__(self):
        self._engine = None
        self._transport = None
        self._auth = None
        self._host: str = "unknown"

    def connect(self, device: dict) -> None:
        if not _PYSNMP_AVAILABLE:
            raise ImportError(
                "pysnmp<7 is required for SNMP support. "
                "Install: pip install 'pysnmp>=4.4.12,<7'"
            )

        self._host = device.get("ip_address", "unknown")
        port = int(device.get("snmp_port", 161))
        timeout = int(device.get("connect_timeout", 10))
        retries = int(device.get("snmp_retries", 1))
        snmp_version = device.get("snmp_version", "v2c")

        logger.debug(
            f"SNMP configuring — host={self._host} port={port} "
            f"version={snmp_version} timeout={timeout}s retries={retries}"
        )

        self._engine = SnmpEngine()
        self._transport = UdpTransportTarget(
            (self._host, port),
            timeout=timeout,
            retries=retries,
        )

        if snmp_version == "v3":
            snmp_user = device.get("snmp_user", "netfleet")
            self._auth = UsmUserData(
                snmp_user,
                authKey=device.get("snmp_auth_key", ""),
                privKey=device.get("snmp_priv_key", ""),
                authProtocol=usmHMACSHAAuthProtocol,
                privProtocol=usmAesCfb128Protocol,
            )
            logger.info(
                f"SNMP v3 configured — host={self._host} user={snmp_user}"
            )
        else:
            community = device.get("snmp_community", "public")
            self._auth = CommunityData(community, mpModel=1)
            logger.info(
                f"SNMP v2c configured — host={self._host} community=***"
            )

    def execute(self, command: str, timeout: int = 30) -> str:
        """
        command: JSON list of OID dicts — [{"oid": "1.3.6.1...", "label": "..."}]
        Returns: JSON object — {"label": "value", ...}

        Each OID is queried individually via getCmd so that a single
        unreachable OID does not abort the whole list (it is recorded
        as None in the result).
        """
        if self._engine is None or self._transport is None:
            raise RuntimeError(
                f"SNMP execute called before connect (host={self._host})"
            )

        oid_list = json.loads(command)
        logger.debug(
            f"SNMP execute — host={self._host} oid_count={len(oid_list)}"
        )
        results: dict = {}

        for oid_spec in oid_list:
            oid_str = oid_spec["oid"]
            label = oid_spec.get("label", oid_str)
            try:
                value = self._get_oid(oid_str)
                results[label] = value
                logger.debug(
                    f"SNMP OID ok — host={self._host} "
                    f"oid={oid_str} label={label} value={value}"
                )
            except TimeoutError as exc:
                logger.warning(
                    f"SNMP OID timeout — host={self._host} oid={oid_str}: {exc}"
                )
                results[label] = None
                # Re-raise so DeviceExecutor can count this as a TIMEOUT failure
                raise
            except ConnectionError as exc:
                logger.warning(
                    f"SNMP OID error — host={self._host} oid={oid_str}: {exc}"
                )
                results[label] = None

        logger.debug(
            f"SNMP execute complete — host={self._host} "
            f"resolved={sum(1 for v in results.values() if v is not None)}/"
            f"{len(results)}"
        )
        return json.dumps(results)

    def bulk_walk(self, base_oid: str, max_rows: int = 100) -> dict:
        """Walk a table OID and return {oid_suffix: value} dict."""
        if self._engine is None or self._transport is None:
            raise RuntimeError(
                f"SNMP bulk_walk called before connect (host={self._host})"
            )
        logger.debug(
            f"SNMP bulk_walk — host={self._host} base_oid={base_oid} max_rows={max_rows}"
        )
        results: dict = {}
        for (error_indication, error_status, error_index, var_binds) in bulkCmd(
            self._engine,
            self._auth,
            self._transport,
            ContextData(),
            0,
            max_rows,
            ObjectType(ObjectIdentity(base_oid)),
            lexicographicMode=False,
        ):
            if error_indication:
                logger.warning(
                    f"SNMP bulk_walk error — host={self._host}: {error_indication}"
                )
                break
            if error_status:
                logger.warning(
                    f"SNMP bulk_walk PDU error — host={self._host}: "
                    f"{error_status.prettyPrint()} at {error_index}"
                )
                break
            for obj_type, value in var_binds:
                results[str(obj_type)] = value.prettyPrint()
        logger.debug(
            f"SNMP bulk_walk done — host={self._host} rows={len(results)}"
        )
        return results

    def enter_config_mode(self) -> None:
        raise NotImplementedError(
            "SNMP does not support config mode — "
            "SET operations must use SSH or Telnet"
        )

    def exit_config_mode(self) -> None:
        raise NotImplementedError(
            "SNMP does not support config mode"
        )

    def disconnect(self) -> None:
        self._engine = None
        self._transport = None
        self._auth = None
        logger.debug(f"SNMP session cleared — host={self._host}")

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    def _get_oid(self, oid: str) -> Optional[str]:
        error_indication, error_status, error_index, var_binds = next(
            getCmd(
                self._engine,
                self._auth,
                self._transport,
                ContextData(),
                ObjectType(ObjectIdentity(oid)),
            )
        )
        if error_indication:
            indication_str = str(error_indication).lower()
            if _SNMP_TIMEOUT_INDICATOR in indication_str:
                raise TimeoutError(
                    f"SNMP timeout for OID {oid} on {self._host}: {error_indication}"
                )
            raise ConnectionError(
                f"SNMP error for OID {oid} on {self._host}: {error_indication}"
            )
        if error_status:
            raise ConnectionError(
                f"SNMP PDU error for OID {oid} on {self._host} "
                f"at index {error_index}: {error_status.prettyPrint()}"
            )
        _, value = var_binds[0]
        return value.prettyPrint()
