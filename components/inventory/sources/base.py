from abc import ABC, abstractmethod
from shared.models import DeviceInventoryRecord


class BaseInventorySource(ABC):

    @abstractmethod
    def fetch_all_regions(
        self,
        segment: str
    ) -> list[DeviceInventoryRecord]:
        """
        Fetch all devices for a segment across every
        known region. Returns one record per (region, segment).
        """
        pass

    @abstractmethod
    def health_check(self) -> bool:
        pass
