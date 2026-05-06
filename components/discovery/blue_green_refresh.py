from dataclasses import dataclass, field
from datetime import datetime
from shared.models import DeviceDiscoveryRecord
from shared.utils.mongo_client import MongoDBClient
from shared.utils.logger import get_logger

logger = get_logger("blue_green_refresh")

DEVICES_COLL = "devices"
BACKUP_COLL = "discovery_backup"


@dataclass
class RefreshResult:
    total_regions: int = 0
    refreshed_regions: int = 0
    skipped_regions: int = 0
    failed_regions: int = 0
    total_inserted: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.failed_regions == 0 and self.refreshed_regions > 0

    @property
    def summary(self) -> str:
        return (
            f"{self.refreshed_regions}/{self.total_regions} regions refreshed, "
            f"{self.skipped_regions} skipped, "
            f"{self.failed_regions} failed — "
            f"{self.total_inserted} devices inserted"
        )


class BlueGreenRefresh:
    """
    Atomic per-(region, segment) device inventory refresh.

    Flow for each VALID region:
        1. Backup  — copy existing devices to discovery_backup
        2. Delete  — remove existing devices for this region+segment
        3. Insert  — bulk insert new devices
        4. Verify  — count in DB must match incoming_count
        5. Rollback if verify fails — restore from backup

    Invalid regions (delta > threshold) are skipped entirely;
    existing data is never touched for them.

    Independent per region: one city failing never blocks others.
    """

    def __init__(self):
        self.mongo = MongoDBClient()

    def refresh_all(
        self,
        records: list[DeviceDiscoveryRecord],
        delta_results: dict,
    ) -> RefreshResult:
        result = RefreshResult(total_regions=len(records))

        for record in records:
            segment_val = (
                record.segment
                if isinstance(record.segment, str)
                else record.segment.value
            )
            key = f"{record.region}:{segment_val}"
            delta = delta_results.get(key)

            if delta is None or not delta.is_valid:
                logger.warning(f"Skipping invalid region: {key}")
                result.skipped_regions += 1
                continue

            try:
                inserted = self._refresh_region(
                    record, segment_val
                )
                result.refreshed_regions += 1
                result.total_inserted += inserted
            except Exception as exc:
                msg = f"Refresh failed for {key}: {exc}"
                logger.error(msg)
                result.failed_regions += 1
                result.errors.append(msg)

        logger.info(f"Blue-green refresh complete — {result.summary}")
        return result

    # ------------------------------------------------------------------ #
    # Per-region refresh                                                   #
    # ------------------------------------------------------------------ #

    def _refresh_region(
        self,
        record: DeviceDiscoveryRecord,
        segment: str,
    ) -> int:
        region = record.region
        label = f"{region}/{segment}"

        logger.info(
            f"Refreshing {label} — "
            f"{record.total_count} incoming devices"
        )

        # Step 1: Backup existing
        backed_up = self._backup(region, segment)
        logger.debug(f"Backed up {backed_up} devices for {label}")

        # Step 2: Delete existing
        deleted = self._delete(region, segment)
        logger.debug(f"Deleted {deleted} existing devices for {label}")

        try:
            # Step 3: Bulk insert new devices
            now = datetime.utcnow().isoformat()
            docs = []
            for device in record.devices:
                doc = device.model_dump()
                doc["discovered_at"] = now
                docs.append(doc)

            if not docs:
                logger.warning(f"No devices to insert for {label}")
                return 0

            inserted = self.mongo.insert_many(DEVICES_COLL, docs)

            # Step 4: Verify count
            actual = self.mongo.count_documents(
                DEVICES_COLL,
                {"region": region, "segment": segment},
            )
            if actual != record.total_count:
                raise ValueError(
                    f"Count mismatch after insert — "
                    f"expected {record.total_count}, got {actual}"
                )

            logger.info(
                f"Refreshed {label} — {inserted} devices"
            )
            self._cleanup_backup(region, segment)
            return inserted

        except Exception:
            logger.error(
                f"Insert failed for {label} — rolling back"
            )
            self._rollback(region, segment)
            raise

    # ------------------------------------------------------------------ #
    # Backup / rollback helpers                                            #
    # ------------------------------------------------------------------ #

    def _backup(self, region: str, segment: str) -> int:
        devices_col = self.mongo.get_collection(DEVICES_COLL)
        backup_col = self.mongo.get_collection(BACKUP_COLL)

        # Remove stale backup for this region+segment first
        backup_col.delete_many(
            {"region": region, "segment": segment}
        )

        existing = list(
            devices_col.find(
                {"region": region, "segment": segment},
                {"_id": 0},
            )
        )
        if existing:
            backup_col.insert_many(existing)

        return len(existing)

    def _delete(self, region: str, segment: str) -> int:
        col = self.mongo.get_collection(DEVICES_COLL)
        result = col.delete_many(
            {"region": region, "segment": segment}
        )
        return result.deleted_count

    def _rollback(self, region: str, segment: str) -> int:
        try:
            backup_col = self.mongo.get_collection(BACKUP_COLL)
            devices_col = self.mongo.get_collection(DEVICES_COLL)

            # Remove any partial inserts
            devices_col.delete_many(
                {"region": region, "segment": segment}
            )

            backup_docs = list(
                backup_col.find(
                    {"region": region, "segment": segment},
                    {"_id": 0},
                )
            )
            if backup_docs:
                devices_col.insert_many(backup_docs)

            logger.info(
                f"Rollback complete for {region}/{segment} — "
                f"restored {len(backup_docs)} devices"
            )
            return len(backup_docs)
        except Exception as exc:
            logger.error(
                f"Rollback failed for {region}/{segment}: {exc}"
            )
            return 0

    def _cleanup_backup(self, region: str, segment: str):
        try:
            backup_col = self.mongo.get_collection(BACKUP_COLL)
            backup_col.delete_many(
                {"region": region, "segment": segment}
            )
        except Exception as exc:
            logger.warning(
                f"Backup cleanup failed for {region}/{segment}: {exc}"
            )
