import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler

from aid.collect.scheduler import construct_scheduler
from aid.logger import logger
from aid.query.api import app


def main():
    """Run the complete application."""
    job_scheduler = construct_scheduler(BackgroundScheduler)
    try:
        logger.info("Starting scheduler", type=job_scheduler.__class__.__name__)
        job_scheduler.start()
        logger.info("Starting web server")
        uvicorn.run(app)
    except (KeyboardInterrupt, SystemExit) as exc:
        logger.info("Graceful exit", signal=exc.__class__.__name__)


if __name__ == "__main__":
    main()
