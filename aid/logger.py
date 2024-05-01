"""
I like structlog! But I still run logging.basicConfig to align logs from other packages.
"""

import logging
import sys

import structlog

LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)-9s] %(message)s",  # aligns with structlog
    datefmt=LOG_DATE_FORMAT,
    level=logging.WARNING,
)

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt=LOG_DATE_FORMAT, utc=False),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)

logger = structlog.get_logger()
