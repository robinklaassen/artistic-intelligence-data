from apscheduler.schedulers.blocking import BlockingScheduler

from artistic_intelligence_data.schedule import construct_scheduler
from artistic_intelligence_data.utils.logger import logger


def main():
    job_scheduler = construct_scheduler(BlockingScheduler)
    try:
        job_scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Received exit signal")
        pass


if __name__ == "__main__":
    main()
