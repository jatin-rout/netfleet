from datetime import datetime


class CiscoIOSSimulator:
    """
    Simulates Cisco IOS device responses.
    Returns realistic CLI output for common operations.
    """

    vendor = "cisco_ios"
    supported_operations = [
        "show_interfaces",
        "show_optical_power",
        "show_version",
        "show_ip_route"
    ]

    def __init__(self, device_id: str, ip: str):
        self.device_id = device_id
        self.ip = ip

    def get_response(self, command: str) -> str:
        command = command.strip().lower()

        if "show interfaces" in command:
            return self._show_interfaces()
        elif "show version" in command:
            return self._show_version()
        elif "show optical" in command:
            return self._show_optical_power()
        elif "show ip route" in command:
            return self._show_ip_route()
        else:
            return f"% Unknown command: {command}"

    def _show_interfaces(self) -> str:
        return f"""
GigabitEthernet0/0 is up, line protocol is up
  Hardware is iGbE, address is aabb.ccdd.ee01
  Internet address is {self.ip}/24
  MTU 1500 bytes, BW 1000000 Kbit/sec
  Full-duplex, 1000Mb/s, media type is T
  Input errors 0, Output errors 0
  5 minute input rate 1000 bits/sec, 10 packets/sec
  5 minute output rate 2000 bits/sec, 15 packets/sec

GigabitEthernet0/1 is up, line protocol is up
  Hardware is iGbE, address is aabb.ccdd.ee02
  MTU 1500 bytes, BW 1000000 Kbit/sec
  Full-duplex, 1000Mb/s, media type is T
  Input errors 0, Output errors 0
  5 minute input rate 500 bits/sec, 5 packets/sec
  5 minute output rate 800 bits/sec, 8 packets/sec
"""

    def _show_version(self) -> str:
        return f"""
Cisco IOS Software, Version 15.7(3)M
Technical Support: http://www.cisco.com/techsupport
ROM: System Bootstrap, Version 15.0(1)
{self.device_id} uptime is 10 weeks, 2 days
System image file is "flash:c2900-universalk9-mz.SPA.157-3.M"
Cisco CISCO2901/K9 (revision 1.0)
Processor board ID {self.device_id}
2 Gigabit Ethernet interfaces
256K bytes of non-volatile configuration memory
Last reset from PowerOn
"""

    def _show_optical_power(self) -> str:
        return f"""
Interface       Rx Power(dBm)    Tx Power(dBm)    Status
Te0/0/0         -3.45            -2.10            Normal
Te0/0/1         -4.20            -2.30            Normal
Te0/0/2         -3.80            -2.15            Normal
Te0/0/3         -5.10            -2.45            Warning
"""

    def _show_ip_route(self) -> str:
        return f"""
Codes: C - connected, S - static, R - RIP
Gateway of last resort is 10.0.0.1 to network 0.0.0.0

C    {self.ip}/24 is directly connected, GigabitEthernet0/0
S*   0.0.0.0/0 [1/0] via 10.0.0.1
R    192.168.1.0/24 [120/1] via 10.0.0.2
"""
