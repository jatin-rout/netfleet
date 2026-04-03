class HuaweiVRPSimulator:
    """
    Simulates Huawei VRP device responses.
    Returns realistic Huawei CLI output.
    """

    vendor = "huawei_vrp"
    supported_operations = [
        "display_interface",
        "display_optical_power",
        "display_version"
    ]

    def __init__(self, device_id: str, ip: str):
        self.device_id = device_id
        self.ip = ip

    def get_response(self, command: str) -> str:
        command = command.strip().lower()

        if "display interface" in command:
            return self._display_interface()
        elif "display version" in command:
            return self._display_version()
        elif "display optical" in command:
            return self._display_optical_power()
        else:
            return f"Error: Unknown command: {command}"

    def _display_interface(self) -> str:
        return f"""
GigabitEthernet0/0/0 current state: UP
Line protocol current state: UP
Hardware address is AABB-CCDD-EE01
Internet Address is {self.ip}/24
Speed : 1000, Loopback: NONE
Duplex: FULL
Input bandwidth utilization : 0.01%
Output bandwidth utilization: 0.02%
Last 300 seconds input: 1000 bits/sec 10 packets/sec
Last 300 seconds output: 2000 bits/sec 15 packets/sec

GigabitEthernet0/0/1 current state: UP
Line protocol current state: UP
Hardware address is AABB-CCDD-EE02
Speed : 1000, Loopback: NONE
Duplex: FULL
"""

    def _display_version(self) -> str:
        return f"""
Huawei Versatile Routing Platform Software
VRP (R) software, Version 5.170 (S5700 V200R011C10)
Copyright (C) 2000-2022 Huawei Technologies Co., Ltd.
{self.device_id} uptime is 10 weeks 2 days
NVRAM Memory: 512 KB
Flash Memory: 512 MB
"""

    def _display_optical_power(self) -> str:
        return f"""
Port            RX Power(dBm)    TX Power(dBm)    Wave(nm)
XGE0/0/0        -3.45            -2.10            1310
XGE0/0/1        -4.20            -2.30            1310
XGE0/0/2        -3.80            -2.15            1310
XGE0/0/3        -5.10            -2.45            1550
"""
