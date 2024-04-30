import logging
import os
import sys

import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

from aid.collect.scheduler import construct_scheduler
from aid.logger import logger, LOG_DATE_FORMAT

logging.basicConfig(
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)-9s] %(message)s",  # aligns with structlog
    datefmt=LOG_DATE_FORMAT,
    level=logging.WARNING,
)


def main():
    """Run the complete application."""
    job_scheduler = construct_scheduler(BackgroundScheduler)
    try:
        logger.info("Starting scheduler", type=job_scheduler.__class__.__name__)
        job_scheduler.start()

        host = os.getenv("API_HOST", "127.0.0.1")
        port = os.getenv("API_PORT", "8000")
        logger.info("Starting web server", host=host, port=port)
        uvicorn.run("aid.provide.api:app", host=host, port=int(port), workers=2, log_config=None)
    except (KeyboardInterrupt, SystemExit) as exc:
        # TODO also catch sigterm
        logger.info("Graceful exit", signal=exc.__class__.__name__)
        job_scheduler.shutdown()


if __name__ == "__main__":
    load_dotenv()
    main()
