import os
import time
import signal
import threading
from datetime import datetime

from croniter import croniter

from shared.config.settings import DiscoveryConfig, SegmentConfig
from shared.utils.logger import get_logger
from components.discovery.delta_validator import DeltaValidator
from components.discovery.blue_green_refresh import BlueGreenRefresh
from components.discovery.sources.factory import get_source

logger = get_logger("discovery")

DISCOVERY_CRON = os.getenv("DISCOVERY_CRON", "0 1 * * *")
ALL_SEGMENTS = (
    SegmentConfig.HIGHER_SEGMENTS + SegmentConfig.LOWER_SEGMENTS
)

# Set by signal handlers to stop all loops cleanly
_shutdown = threading.Event()


# ------------------------------------------------------------------ #
# Core discovery cycle — shared by cron and manual trigger           #
# ------------------------------------------------------------------ #

def run_discovery_cycle(
    segments: list[str] = None,
    regions: list[str] = None,
) -> dict:
    """
    Execute one full discovery cycle.

    segments — restrict to specific segments (default: all)
    regions  — restrict to specific regions   (default: all)

    Returns a summary dict suitable for logging or API response.
    """
    source = get_source()
    validator = DeltaValidator()
    refresher = BlueGreenRefresh()

    target_segments = segments or ALL_SEGMENTS
    start = datetime.utcnow()

    logger.info(
        f"Discovery cycle started — segments: {target_segments}"
    )

    all_records = []
    for segment in target_segments:
        try:
            records = source.fetch_all_regions(segment)
            # Filter to requested regions if specified
            if regions:
                records = [r for r in records if r.region in regions]
            all_records.extend(records)
            logger.info(
                f"Fetched {len(records)} region records "
                f"for segment {segment}"
            )
        except Exception as exc:
            logger.error(f"Fetch failed for {segment}: {exc}")

    if not all_records:
        logger.warning("No records fetched — skipping refresh")
        return {"status": "skipped", "reason": "no_records"}

    delta_results = validator.validate_all(all_records)
    val_summary = validator.summary(delta_results)

    logger.info(
        f"Delta validation — "
        f"{val_summary['valid_regions']}/{val_summary['total_regions']} "
        f"regions valid"
    )

    refresh_result = refresher.refresh_all(all_records, delta_results)

    duration = round(
        (datetime.utcnow() - start).total_seconds(), 2
    )

    return {
        "status": "complete" if refresh_result.success else "partial",
        "duration_seconds": duration,
        "delta": val_summary,
        "refresh": {
            "refreshed_regions": refresh_result.refreshed_regions,
            "skipped_regions": refresh_result.skipped_regions,
            "failed_regions": refresh_result.failed_regions,
            "total_inserted": refresh_result.total_inserted,
            "errors": refresh_result.errors,
        },
    }


# ------------------------------------------------------------------ #
# Cron loop — standalone mode                                         #
# ------------------------------------------------------------------ #

def run_cron_loop():
    """Run discovery on the configured cron schedule."""
    logger.info(
        f"Cron discovery loop started — schedule: {DISCOVERY_CRON}"
    )
    cron = croniter(DISCOVERY_CRON, datetime.utcnow())

    while not _shutdown.is_set():
        next_run = cron.get_next(datetime)
        wait = (next_run - datetime.utcnow()).total_seconds()

        if wait > 0:
            logger.info(
                f"Next discovery run at {next_run.isoformat()} "
                f"(in {wait:.0f}s)"
            )
            _shutdown.wait(timeout=wait)

        if _shutdown.is_set():
            break

        try:
            result = run_discovery_cycle()
            logger.info(f"Discovery cycle result: {result}")
        except Exception as exc:
            logger.error(f"Discovery cycle error: {exc}")


# ------------------------------------------------------------------ #
# Event loop — distributed mode                                       #
# ------------------------------------------------------------------ #

def run_event_loop():
    """Continuously consume device events from Kafka."""
    from components.discovery.sources.event_source import (
        EventDiscoverySource,
    )

    source = EventDiscoverySource()
    logger.info("Event-driven discovery loop started")

    while not _shutdown.is_set():
        try:
            processed = source.consume_events(batch_size=100)
            if processed:
                logger.info(
                    f"Processed {processed} discovery events"
                )
        except Exception as exc:
            logger.error(f"Event loop error: {exc}")
        time.sleep(1)


# ------------------------------------------------------------------ #
# API trigger listener — all modes                                    #
# ------------------------------------------------------------------ #

def run_trigger_listener():
    """
    Listen on queue_discovery_trigger for on-demand discovery runs
    pushed by the REST API.  Runs as a daemon thread alongside the
    cron or event loop.
    """
    from shared.utils.redis_client import RedisClient

    redis = RedisClient()
    trigger_queue = "queue_discovery_trigger"
    logger.info("Discovery trigger listener started")

    while not _shutdown.is_set():
        try:
            msg = redis.pop_from_queue(trigger_queue, timeout=2)
            if msg:
                logger.info(f"Manual discovery trigger: {msg}")
                result = run_discovery_cycle(
                    segments=msg.get("segments") or None,
                    regions=msg.get("regions") or None,
                )
                logger.info(f"Manual discovery result: {result}")
        except Exception as exc:
            logger.error(f"Trigger listener error: {exc}")


# ------------------------------------------------------------------ #
# Entry point                                                         #
# ------------------------------------------------------------------ #

def main():
    mode = DiscoveryConfig.DISCOVERY_MODE.lower()

    def _stop(sig, frame):
        logger.info(f"Shutdown signal {sig} received")
        _shutdown.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    logger.info(f"Discovery component starting — mode: {mode}")

    trigger_thread = threading.Thread(
        target=run_trigger_listener,
        daemon=True,
        name="trigger-listener",
    )
    trigger_thread.start()

    if mode == "event":
        run_event_loop()
    else:
        run_cron_loop()

    logger.info("Discovery component stopped")


if __name__ == "__main__":
    main()
