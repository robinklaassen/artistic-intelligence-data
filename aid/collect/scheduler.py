import signal
import sys

from apscheduler.schedulers.base import BaseScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from aid.collect import ALL_COLLECTORS
from aid.logger import logger


def construct_scheduler(
    scheduler_cls: type[BaseScheduler] = BlockingScheduler,
) -> BaseScheduler:
    """Construct a scheduler using the provided class. Recommended either BlockingScheduler or BackgroundScheduler."""
    scheduler = scheduler_cls()

    # Graceful shutdown happens only on next job run
    # Add an empty job every second to prevent unnecessary waiting
    scheduler.add_job(lambda: None, CronTrigger(second="*"))

    for collector_cls in ALL_COLLECTORS:
        collector = collector_cls()
        scheduler.add_job(collector.run, CronTrigger(second=f"*/{collector.interval_seconds}"))

    return scheduler


def main():
    """Run the scheduled collection standalone."""
    job_scheduler = construct_scheduler(BlockingScheduler)
    signal.signal(signal.SIGTERM, lambda sig, frame: sys.exit())  # sys.exit() raises SystemExit which is caught below

    try:
        logger.info("Starting scheduler", type=job_scheduler.__class__.__name__)
        job_scheduler.start()
    except (KeyboardInterrupt, SystemExit) as exc:
        logger.info("Received exit signal", signal=exc.__class__.__name__)
        job_scheduler.shutdown(wait=True)
        logger.info("Scheduler gracefully shut down")


if __name__ == "__main__":
    main()
