from .base import BaseCollector
from .ns_trains import TrainCollector

ALL: list[type[BaseCollector]] = [TrainCollector]
