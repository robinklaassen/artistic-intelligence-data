from abc import ABC, abstractmethod

from dotenv import load_dotenv


class BaseCollector(ABC):

    def __init__(self):
        load_dotenv(verbose=True)

    @property
    @abstractmethod
    def interval_seconds(self) -> int:
        """The interval that this collector should run on, in seconds."""
        raise NotImplementedError

    @abstractmethod
    def run(self):
        """Run this collector once."""
        raise NotImplementedError
