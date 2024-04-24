from .base_collector import BaseCollector
from .ns_trains import NSTrainCollector

ALL_COLLECTORS: list[type[BaseCollector]] = [NSTrainCollector]
