import os
import socket
import threading
import json
from simulator.device_registry import DeviceRegistry
from shared.utils.logger import get_logger

logger = get_logger("simulator")


class MockDeviceServer:
    """
    TCP server that simulates network devices.
    Accepts connections and returns vendor specific
    CLI output based on device IP and command sent.

    Protocol:
        Client sends: {"ip": "10.1.1", "command": "show interfaces"}
        Server returns: raw CLI output string
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8888,
        device_count: int = 100
    ):
        self.host = host
        self.port = port
        self.registry = DeviceRegistry(device_count)
        self.server_socket = None
        self.running = False

    def start(self):
        self.server_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )
        self.server_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(500)
        self.running = True

        logger.info(
            f"Mock device server started on "
            f"{self.host}:{self.port}"
        )
        logger.info(
            f"Fleet stats: {self.registry.get_stats()}"
        )

        while self.running:
            try:
                client_socket, address = (
                    self.server_socket.accept()
                )
                thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address),
                    daemon=True
                )
                thread.start()
            except Exception as e:
                if self.running:
                    logger.error(
                        f"Server error: {e}"
                    )

    def _handle_client(
        self,
        client_socket: socket.socket,
        address: tuple
    ):
        try:
            data = client_socket.recv(4096).decode()
            request = json.loads(data)

            ip = request.get("ip")
            command = request.get("command", "")

            response = self.registry.get_response(
                ip, command
            )

            client_socket.send(
                response.encode()
            )

        except ValueError as e:
            error = json.dumps({"error": str(e)})
            client_socket.send(error.encode())
        except Exception as e:
            logger.error(
                f"Client handler error: {e}"
            )
        finally:
            client_socket.close()

    def stop(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        logger.info("Mock device server stopped")


def main():
    host = os.getenv("SIMULATOR_HOST", "0.0.0.0")
    port = int(os.getenv("SIMULATOR_PORT", 8888))
    device_count = int(
        os.getenv("SIMULATOR_DEVICE_COUNT", 100)
    )

    server = MockDeviceServer(
        host=host,
        port=port,
        device_count=device_count
    )

    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
