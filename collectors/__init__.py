from .base import BaseCollector
from .trains import TrainCollector

ALL: list[type[BaseCollector]] = [
    TrainCollector
]
