from simulator.vendors.cisco_ios import CiscoIOSSimulator
from simulator.vendors.huawei_vrp import HuaweiVRPSimulator
from simulator.vendors.bdcom import BDCOMSimulator
from shared.utils.logger import get_logger

logger = get_logger("device_registry")


VENDOR_MAP = {
    "cisco_ios": CiscoIOSSimulator,
    "huawei_vrp": HuaweiVRPSimulator,
    "bdcom": BDCOMSimulator,
}


class DeviceRegistry:
    """
    Registry of simulated devices.
    Generates realistic device fleet for testing.
    """

    def __init__(self, device_count: int = 100):
        self.devices = {}
        self._generate_fleet(device_count)
        logger.info(
            f"Device registry initialized "
            f"with {len(self.devices)} devices"
        )

    def _generate_fleet(self, count: int):
        vendors = [
            ("cisco_ios", "Tier1", "10.1"),
            ("cisco_ios", "Tier2", "10.2"),
            ("huawei_vrp", "Tier2", "10.3"),
            ("huawei_vrp", "Tier3", "10.4"),
            ("bdcom", "Edge", "192.168.1"),
            ("bdcom", "Field", "192.168.2"),
        ]

        per_vendor = count // len(vendors)

        for vendor, segment, subnet in vendors:
            for i in range(1, per_vendor + 1):
                device_id = f"{vendor}_{segment}_{i:04d}"
                ip = f"{subnet}.{i}"
                simulator_class = VENDOR_MAP[vendor]
                self.devices[ip] = {
                    "device_id": device_id,
                    "ip": ip,
                    "vendor": vendor,
                    "segment": segment,
                    "simulator": simulator_class(
                        device_id, ip
                    )
                }

    def get_device(self, ip: str) -> dict:
        return self.devices.get(ip)

    def get_response(
        self,
        ip: str,
        command: str
    ) -> str:
        device = self.get_device(ip)
        if not device:
            raise ValueError(
                f"Device not found: {ip}"
            )
        return device["simulator"].get_response(command)

    def list_devices(
        self,
        segment: str = None
    ) -> list[dict]:
        devices = list(self.devices.values())
        if segment:
            devices = [
                d for d in devices
                if d["segment"] == segment
            ]
        return [{
            "device_id": d["device_id"],
            "ip": d["ip"],
            "vendor": d["vendor"],
            "segment": d["segment"]
        } for d in devices]

    def get_stats(self) -> dict:
        stats = {}
        for device in self.devices.values():
            segment = device["segment"]
            stats[segment] = stats.get(segment, 0) + 1
        return {
            "total_devices": len(self.devices),
            "by_segment": stats
        }
