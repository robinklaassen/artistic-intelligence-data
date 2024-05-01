import os
import signal
import sys

import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

from aid.collect.scheduler import construct_scheduler
from aid.logger import logger
from aid.provide.api import APP_MODULE


def main():
    """Run the complete application."""
    job_scheduler = construct_scheduler(BackgroundScheduler)
    signal.signal(signal.SIGTERM, lambda sig, frame: sys.exit())  # sys.exit() raises SystemExit which is caught below

    try:
        logger.info("Starting scheduler", type=job_scheduler.__class__.__name__)
        job_scheduler.start()

        host = os.getenv("API_HOST", "127.0.0.1")
        port = os.getenv("API_PORT", "8000")
        logger.info("Starting web server", host=host, port=port)
        uvicorn.run(APP_MODULE, host=host, port=int(port), workers=2, log_config=None)
    except (KeyboardInterrupt, SystemExit) as exc:  # TODO does not exit gracefully when using multiple workers
        logger.info("Received exit signal", signal=exc.__class__.__name__)
        job_scheduler.shutdown(wait=True)
        logger.info("Scheduler gracefully shut down")


if __name__ == "__main__":
    load_dotenv()
    main()
