from .base_collector import BaseCollector
from .ns_trains import NSTrainCollector

ALL: list[type[BaseCollector]] = [NSTrainCollector]
