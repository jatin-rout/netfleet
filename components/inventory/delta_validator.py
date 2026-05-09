from shared.models import DeviceDeltaResult, DeviceInventoryRecord, SegmentType
from shared.config.settings import InventoryConfig
from shared.utils.mongo_client import MongoDBClient
from shared.utils.logger import get_logger

logger = get_logger("delta_validator")


class DeltaValidator:
    """
    Per-(region, segment) count-based delta validation.

    Logic (scales to millions — count query is O(1) with indexes):
        existing_count = MongoDB count for (region, segment)
        incoming_count = count of new batch for (region, segment)
        delta_pct      = |incoming - existing| / existing * 100

        delta_pct <= threshold  → VALID  — proceed with full replace
        delta_pct >  threshold  → INVALID — abort, keep existing data
        existing_count == 0     → first run, always VALID

    This intentionally does NOT diff individual devices.
    At 200K–2M devices per city, device-level diffing is prohibitive.
    The count check catches bulk data loss (partial file copy, DB down)
    while accepting organic fleet churn within the threshold.
    """

    def __init__(self, threshold_pct: float = None):
        self.threshold = (
            threshold_pct
            if threshold_pct is not None
            else InventoryConfig.DELTA_THRESHOLD
        )
        self.mongo = MongoDBClient()

    def validate_region(
        self,
        region: str,
        segment: str,
        incoming_count: int,
    ) -> DeviceDeltaResult:
        existing_count = self.mongo.count_documents(
            "devices",
            {"region": region, "segment": segment},
        )

        if existing_count == 0:
            logger.info(
                f"{region}/{segment}: first run — "
                f"accepting {incoming_count} devices"
            )
            return DeviceDeltaResult(
                region=region,
                segment=SegmentType(segment),
                existing_count=0,
                incoming_count=incoming_count,
                delta_count=incoming_count,
                delta_percentage=0.0,
                is_valid=True,
                reason="first_run",
            )

        delta_count = abs(incoming_count - existing_count)
        delta_pct = round(delta_count / existing_count * 100, 2)
        is_valid = delta_pct <= self.threshold

        if is_valid:
            reason = (
                f"delta {delta_pct}% within "
                f"{self.threshold}% threshold"
            )
            logger.info(
                f"{region}/{segment} VALID — "
                f"existing={existing_count} "
                f"incoming={incoming_count} "
                f"delta={delta_pct}%"
            )
        else:
            reason = (
                f"delta {delta_pct}% exceeds "
                f"{self.threshold}% threshold — "
                f"keeping existing {existing_count} devices"
            )
            logger.warning(
                f"{region}/{segment} INVALID — "
                f"existing={existing_count} "
                f"incoming={incoming_count} "
                f"delta={delta_pct}% — aborting region"
            )

        return DeviceDeltaResult(
            region=region,
            segment=SegmentType(segment),
            existing_count=existing_count,
            incoming_count=incoming_count,
            delta_count=delta_count,
            delta_percentage=delta_pct,
            is_valid=is_valid,
            reason=reason,
        )

    def validate_all(
        self,
        records: list[DeviceInventoryRecord],
    ) -> dict[str, DeviceDeltaResult]:
        """
        Validate every (region, segment) record independently.
        Returns a dict keyed by "region:segment".
        """
        results: dict[str, DeviceDeltaResult] = {}

        for record in records:
            segment_val = (
                record.segment
                if isinstance(record.segment, str)
                else record.segment.value
            )
            key = f"{record.region}:{segment_val}"
            results[key] = self.validate_region(
                record.region,
                segment_val,
                record.total_count,
            )

        return results

    def summary(
        self,
        results: dict[str, DeviceDeltaResult],
    ) -> dict:
        total = len(results)
        valid = sum(1 for r in results.values() if r.is_valid)
        invalid_regions = [
            k for k, r in results.items() if not r.is_valid
        ]

        return {
            "total_regions": total,
            "valid_regions": valid,
            "invalid_regions": len(invalid_regions),
            "all_valid": len(invalid_regions) == 0,
            "aborted_regions": invalid_regions,
        }
