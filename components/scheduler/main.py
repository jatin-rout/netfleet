import signal
import sys
import time

from components.scheduler.job_scheduler import JobScheduler
from shared.utils.logger import get_logger

logger = get_logger("scheduler.main")


def main() -> None:
    logger.info("NetFleet Scheduler — Component 2 — starting")

    scheduler = JobScheduler()
    scheduler.start()

    def _shutdown(sig, _frame) -> None:
        logger.info(f"Signal {sig} received — shutting down")
        scheduler.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    while True:
        time.sleep(30)


if __name__ == "__main__":
    main()
