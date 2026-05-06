from abc import ABC, abstractmethod
from shared.models import DeviceDiscoveryRecord


class BaseDiscoverySource(ABC):

    @abstractmethod
    def fetch_all_regions(
        self,
        segment: str
    ) -> list[DeviceDiscoveryRecord]:
        """
        Fetch all devices for a segment across every
        known region. Returns one record per (region, segment).
        """
        pass

    @abstractmethod
    def health_check(self) -> bool:
        pass
