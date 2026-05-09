"""
REST/HTTP handler using the requests library.

HTTP status code → FailureReason mapping (raised as typed exceptions):
  401, 403       → AuthenticationError (maps to FailureReason.AUTH)
  408, 504, 524  → TimeoutError        (maps to FailureReason.TIMEOUT)
  5xx (others)   → ConnectionError     (maps to FailureReason.CONNECTION)
  Other 4xx      → ConnectionError     (client error, treated as connection)

The caller (DeviceExecutor) catches ConnectionError/TimeoutError/
AuthError and assigns the correct FailureReason.
"""
import json
from typing import Optional

import requests
from requests.exceptions import (
    ConnectionError as RequestsConnectionError,
    Timeout as RequestsTimeout,
    HTTPError as RequestsHTTPError,
)
from requests.auth import HTTPBasicAuth

from components.collector.handlers.base import BaseProtocolHandler
from shared.utils.logger import get_logger

logger = get_logger("collector.rest_handler")

# Status codes that classify as authentication failure
_AUTH_STATUS_CODES = {401, 403}
# Status codes that classify as timeout
_TIMEOUT_STATUS_CODES = {408, 504, 524}


class RESTAuthenticationError(Exception):
    """HTTP 401/403 — maps to FailureReason.AUTH in DeviceExecutor."""


class RESTHandler(BaseProtocolHandler):
    """
    REST/HTTP handler.

    execute() command format:
        JSON string — {"method": "GET|POST|PUT", "path": "/api/...", "body": {...}}

    Returns response body as a string on success.
    Raises typed exceptions on failure for accurate counter classification.
    """

    def __init__(self):
        self._session: Optional[requests.Session] = None
        self._base_url: str = ""
        self._timeout: int = 30
        self._host: str = "unknown"

    def connect(self, device: dict) -> None:
        self._host = device.get("ip_address", "unknown")
        port = device.get("rest_port", 443)
        scheme = device.get("rest_scheme", "https")
        self._base_url = f"{scheme}://{self._host}:{port}"
        self._timeout = int(device.get("connect_timeout", 30))

        logger.debug(
            f"REST connecting — base_url={self._base_url} timeout={self._timeout}s"
        )

        self._session = requests.Session()
        self._session.verify = device.get("ssl_verify", False)

        username = device.get("username")
        password = device.get("password")
        if username and password:
            self._session.auth = HTTPBasicAuth(username, password)
            logger.debug(
                f"REST auth — basic auth set for user={username} "
                f"host={self._host}"
            )

        token = device.get("api_token")
        if token:
            self._session.headers.update(
                {"Authorization": "Bearer ***"}  # log masked
            )
            # Actual token set separately to avoid logging it
            self._session.headers["Authorization"] = f"Bearer {token}"
            logger.debug(f"REST auth — bearer token set host={self._host}")

        self._session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

        # Probe connectivity — raises ConnectionError or TimeoutError on failure
        probe_path = device.get("rest_health_path", "/")
        probe_url = f"{self._base_url}{probe_path}"
        logger.debug(f"REST probe — HEAD {probe_url}")
        try:
            resp = self._session.head(probe_url, timeout=self._timeout)
            logger.debug(
                f"REST probe response — host={self._host} "
                f"status={resp.status_code}"
            )
            # 401/403 on probe means bad credentials
            if resp.status_code in _AUTH_STATUS_CODES:
                raise RESTAuthenticationError(
                    f"REST auth failed during probe — "
                    f"host={self._host} status={resp.status_code}"
                )
        except RequestsConnectionError as exc:
            raise ConnectionError(
                f"REST connection refused — {self._base_url}: {exc}"
            ) from exc
        except RequestsTimeout as exc:
            raise TimeoutError(
                f"REST connection timeout — {self._base_url}"
            ) from exc

        logger.info(f"REST connected — base_url={self._base_url}")

    def execute(self, command: str, timeout: int = 30) -> str:
        """
        command: JSON string {"method": "GET|POST|PUT", "path": "/...", "body": {...}}
        Returns response body on 2xx.
        Raises typed exceptions on error.
        """
        if self._session is None:
            raise RuntimeError(
                f"REST execute called with no session (host={self._host})"
            )

        try:
            spec = json.loads(command)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"REST execute: malformed command JSON — {exc}"
            ) from exc

        method = spec.get("method", "GET").upper()
        path = spec.get("path", "/")
        body = spec.get("body")
        url = f"{self._base_url}{path}"

        logger.debug(
            f"REST execute — host={self._host} "
            f"method={method} path={path} timeout={timeout}s"
        )

        try:
            resp = self._session.request(
                method=method,
                url=url,
                json=body,
                timeout=timeout,
            )
        except RequestsTimeout as exc:
            logger.warning(
                f"REST timeout — host={self._host} method={method} path={path}"
            )
            raise TimeoutError(
                f"REST request timed out: {method} {url}"
            ) from exc
        except RequestsConnectionError as exc:
            logger.warning(
                f"REST connection error — host={self._host} "
                f"method={method} path={path}: {exc}"
            )
            raise ConnectionError(
                f"REST connection error: {method} {url}: {exc}"
            ) from exc

        logger.debug(
            f"REST response — host={self._host} "
            f"method={method} path={path} "
            f"status={resp.status_code} "
            f"body_bytes={len(resp.content)}"
        )

        # Classify HTTP errors
        if not resp.ok:
            self._raise_for_status(resp, method, path)

        return resp.text

    def _raise_for_status(
        self, resp: requests.Response, method: str, path: str
    ) -> None:
        """Convert HTTP error status into a typed exception."""
        code = resp.status_code
        if code in _AUTH_STATUS_CODES:
            logger.warning(
                f"REST auth error — host={self._host} "
                f"method={method} path={path} status={code}"
            )
            raise RESTAuthenticationError(
                f"REST authentication failed: {method} {self._base_url}{path} "
                f"status={code}"
            )
        if code in _TIMEOUT_STATUS_CODES:
            logger.warning(
                f"REST timeout status — host={self._host} "
                f"method={method} path={path} status={code}"
            )
            raise TimeoutError(
                f"REST gateway/request timeout: {method} {self._base_url}{path} "
                f"status={code}"
            )
        # 5xx and other 4xx → treat as connection error
        logger.error(
            f"REST HTTP error — host={self._host} "
            f"method={method} path={path} status={code} "
            f"body={resp.text[:200]}"
        )
        raise ConnectionError(
            f"REST HTTP error: {method} {self._base_url}{path} "
            f"status={code}"
        )

    def enter_config_mode(self) -> None:
        # REST is stateless — no config mode concept
        logger.debug(f"REST enter_config_mode (no-op) — host={self._host}")

    def exit_config_mode(self) -> None:
        logger.debug(f"REST exit_config_mode (no-op) — host={self._host}")

    def disconnect(self) -> None:
        if self._session is not None:
            try:
                self._session.close()
                logger.debug(f"REST session closed — host={self._host}")
            except Exception as exc:
                logger.warning(
                    f"REST session close error (ignored) — "
                    f"host={self._host}: {exc}"
                )
            finally:
                self._session = None
