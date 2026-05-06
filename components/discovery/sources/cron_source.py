import json
import csv
from pathlib import Path
from shared.models import (
    Device,
    DeviceDiscoveryRecord,
    SegmentType,
    SegmentPriority,
    Protocol,
    DeviceStatus,
    IdentityType,
    VendorType,
)
from shared.config.settings import SegmentConfig, DiscoveryConfig
from shared.utils.logger import get_logger
from .base import BaseDiscoverySource
from .primary_db_adapter import (
    BasePrimaryDBAdapter,
    MockPrimaryDBAdapter,
)

logger = get_logger("cron_source")

HIGHER = set(SegmentConfig.HIGHER_SEGMENTS)
LOWER = set(SegmentConfig.LOWER_SEGMENTS)


def _build_device(raw: dict) -> Device:
    segment = raw["segment"]
    return Device(
        device_id=raw["device_id"],
        ip_address=raw["ip_address"],
        mac_address=raw.get("mac_address"),
        serial_number=raw.get("serial_number"),
        vendor=VendorType(raw["vendor"]),
        segment=SegmentType(segment),
        priority=SegmentPriority(
            SegmentConfig.PRIORITY_MAP.get(segment, "STANDARD")
        ),
        protocol=Protocol(raw.get("protocol", "SSH")),
        identity_type=IdentityType(
            SegmentConfig.IDENTITY_MAP.get(segment, "mac_address")
        ),
        region=raw["region"],
        status=DeviceStatus(raw.get("status", "ACTIVE")),
    )


class CronDiscoverySource(BaseDiscoverySource):
    """
    Batch discovery source for standalone (cron) mode.

    Higher segments (Tier1/2/3) — pulled from primary DB via adapter.
    Lower segments (Edge/Field)  — read from secondary files on disk.

    File naming convention for secondary files:
        {region}_{segment}.json   e.g.  mumbai_Edge.json
        {region}_{segment}.csv    e.g.  mumbai_Field.csv

    JSON format  : list of device dicts
    CSV format   : header row + device rows (same fields)
    """

    def __init__(
        self,
        db_adapter: BasePrimaryDBAdapter = None
    ):
        self.db_adapter = db_adapter or MockPrimaryDBAdapter()
        self.files_path = Path(DiscoveryConfig.SECONDARY_FILES_PATH)

    def fetch_all_regions(
        self,
        segment: str
    ) -> list[DeviceDiscoveryRecord]:
        if segment in HIGHER:
            return self._from_primary_db(segment)
        return self._from_secondary_files(segment)

    # ------------------------------------------------------------------ #
    # Higher segments — primary DB                                         #
    # ------------------------------------------------------------------ #

    def _from_primary_db(
        self,
        segment: str
    ) -> list[DeviceDiscoveryRecord]:
        records: list[DeviceDiscoveryRecord] = []

        regions = self.db_adapter.fetch_regions(segment)
        for region in regions:
            try:
                raw_devices = self.db_adapter.fetch_devices(
                    segment, region
                )
                devices = [_build_device(d) for d in raw_devices]
                records.append(DeviceDiscoveryRecord(
                    region=region,
                    segment=SegmentType(segment),
                    devices=devices,
                    total_count=len(devices),
                    source="primary_db",
                ))
                logger.info(
                    f"primary_db — {segment}/{region}: "
                    f"{len(devices)} devices"
                )
            except Exception as exc:
                logger.error(
                    f"primary_db fetch failed — "
                    f"{segment}/{region}: {exc}"
                )

        return records

    # ------------------------------------------------------------------ #
    # Lower segments — secondary files                                     #
    # ------------------------------------------------------------------ #

    def _from_secondary_files(
        self,
        segment: str
    ) -> list[DeviceDiscoveryRecord]:
        records: list[DeviceDiscoveryRecord] = []

        if not self.files_path.exists():
            logger.warning(
                f"Secondary files path not found: {self.files_path}"
            )
            return records

        files = (
            list(self.files_path.glob(f"*_{segment}.json"))
            + list(self.files_path.glob(f"*_{segment}.csv"))
        )

        if not files:
            logger.warning(
                f"No secondary files found for segment {segment} "
                f"in {self.files_path}"
            )
            return records

        for filepath in files:
            try:
                region = filepath.stem[: -(len(segment) + 1)]
                raw_devices = self._read_file(filepath)
                devices = [_build_device(d) for d in raw_devices]
                records.append(DeviceDiscoveryRecord(
                    region=region,
                    segment=SegmentType(segment),
                    devices=devices,
                    total_count=len(devices),
                    source="secondary_files",
                ))
                logger.info(
                    f"secondary_files — {segment}/{region}: "
                    f"{len(devices)} devices from {filepath.name}"
                )
            except Exception as exc:
                logger.error(f"Failed to read {filepath}: {exc}")

        return records

    def _read_file(self, filepath: Path) -> list[dict]:
        if filepath.suffix == ".json":
            with open(filepath) as fh:
                return json.load(fh)
        if filepath.suffix == ".csv":
            with open(filepath, newline="") as fh:
                return list(csv.DictReader(fh))
        raise ValueError(f"Unsupported file format: {filepath.suffix}")

    def health_check(self) -> bool:
        return True
