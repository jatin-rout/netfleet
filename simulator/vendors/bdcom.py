class BDCOMSimulator:
    """
    Simulates BDCOM OLT device responses.
    Common in Indian ISP edge and field segments.
    """

    vendor = "bdcom"
    supported_operations = [
        "show_interface",
        "show_pon_statistics"
    ]

    def __init__(self, device_id: str, ip: str):
        self.device_id = device_id
        self.ip = ip

    def get_response(self, command: str) -> str:
        command = command.strip().lower()

        if "show interface" in command:
            return self._show_interface()
        elif "show pon" in command:
            return self._show_pon_statistics()
        else:
            return f"Invalid input detected"

    def _show_interface(self) -> str:
        return f"""
GigabitEthernet0/1 is up
  Hardware is GigabitEthernet, address is aa:bb:cc:dd:ee:01
  Internet address is {self.ip}/24
  MTU 1500 bytes
  Full-duplex, 1000Mb/s
  Input packets: 1000, errors: 0
  Output packets: 2000, errors: 0

GigabitEthernet0/2 is up
  Hardware is GigabitEthernet, address is aa:bb:cc:dd:ee:02
  MTU 1500 bytes
  Full-duplex, 1000Mb/s
"""

    def _show_pon_statistics(self) -> str:
        return f"""
EPON Statistics for {self.device_id}:
Port    ONUs    RX-Power(dBm)    TX-Power(dBm)    Status
epon0/1    32    -18.50           2.50             Normal
epon0/2    28    -19.20           2.45             Normal
epon0/3    30    -18.80           2.48             Normal
epon0/4    25    -20.10           2.40             Warning
"""
