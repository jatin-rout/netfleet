import hashlib
from abc import ABC, abstractmethod
from shared.config.settings import SegmentConfig
from shared.utils.logger import get_logger

logger = get_logger("primary_db_adapter")


class BasePrimaryDBAdapter(ABC):
    """
    Interface for the upstream primary database.
    Production: implement against Hive, Oracle, or any SQL source.
    Default: MockPrimaryDBAdapter for development / simulator use.
    """

    @abstractmethod
    def fetch_regions(self, segment: str) -> list[str]:
        """Return distinct regions that have devices for this segment."""
        pass

    @abstractmethod
    def fetch_devices(
        self,
        segment: str,
        region: str
    ) -> list[dict]:
        """Return raw device dicts for a (segment, region) pair."""
        pass


class MockPrimaryDBAdapter(BasePrimaryDBAdapter):
    """
    Deterministic mock for development and CI.
    Generates stable device data from region + segment seed so
    repeated runs produce the same inventory.
    """

    REGIONS = [
        "mumbai", "delhi", "bangalore",
        "hyderabad", "chennai", "pune",
        "kolkata", "ahmedabad",
    ]

    # Realistic per-city counts for Tier devices
    DEVICES_PER_REGION = {
        "Tier1": 60,
        "Tier2": 200,
        "Tier3": 300,
    }

    def fetch_regions(self, segment: str) -> list[str]:
        return self.REGIONS

    def fetch_devices(
        self,
        segment: str,
        region: str
    ) -> list[dict]:
        count = self.DEVICES_PER_REGION.get(segment, 100)
        vendors = SegmentConfig.VENDOR_MAP.get(segment, ["cisco_ios"])
        protocols = ["SSH", "SNMP"]

        devices = []
        for i in range(count):
            seed = f"{region}_{segment}_{i}"
            digest = hashlib.md5(seed.encode()).hexdigest()
            octet3 = int(digest[0:2], 16)
            octet4 = int(digest[2:4], 16) or 1

            vendor = vendors[i % len(vendors)]
            protocol = protocols[i % len(protocols)]

            devices.append({
                "device_id": f"{region}-{segment.lower()}-{i:04d}",
                "ip_address": f"10.{octet3}.{octet4}.{i % 253 + 1}",
                "serial_number": f"SN-{digest[:12].upper()}",
                "mac_address": None,
                "vendor": vendor,
                "segment": segment,
                "region": region,
                "protocol": protocol,
                "status": "ACTIVE",
            })

        return devices
