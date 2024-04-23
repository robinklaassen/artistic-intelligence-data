from apscheduler.schedulers.blocking import BlockingScheduler

from aid.collect.scheduler import construct_scheduler
from aid.logger import logger


def main():
    job_scheduler = construct_scheduler(BlockingScheduler)
    try:
        job_scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Received exit signal")
        pass


if __name__ == "__main__":
    main()
