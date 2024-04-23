from apscheduler.schedulers.blocking import BlockingScheduler

from aid.schedule import construct_scheduler
from aid.utils.logger import logger


def main():
    job_scheduler = construct_scheduler(BlockingScheduler)
    try:
        job_scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Received exit signal")
        pass


if __name__ == "__main__":
    main()
