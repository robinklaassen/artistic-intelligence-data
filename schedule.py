from apscheduler.schedulers.base import BaseScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from artistic_intelligence_data.collectors import ALL as ALL_COLLECTORS


def construct_scheduler[T: BaseScheduler](scheduler_cls: type[T] = BlockingScheduler) -> T:
    scheduler = scheduler_cls()
    for collector_cls in ALL_COLLECTORS:
        collector = collector_cls()
        scheduler.add_job(collector.run, CronTrigger(second=f"*/{collector.interval_seconds}"))

    return scheduler


if __name__ == "__main__":
    construct_scheduler().start()
