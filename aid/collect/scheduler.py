from apscheduler.schedulers.base import BaseScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from aid.collect import ALL_COLLECTORS


def construct_scheduler(
    scheduler_cls: type[BaseScheduler] = BlockingScheduler,
) -> BaseScheduler:
    """Construct a scheduler using the provided class. Recommended either BlockingScheduler or BackgroundScheduler."""
    scheduler = scheduler_cls()
    for collector_cls in ALL_COLLECTORS:
        collector = collector_cls()
        scheduler.add_job(collector.run, CronTrigger(second=f"*/{collector.interval_seconds}"))

    return scheduler


if __name__ == "__main__":
    construct_scheduler().start()
