from datetime import datetime, timedelta

from aid.provide.base_provider import BaseProvider


class NSTrainProvider(BaseProvider):
    def get_trains(self, start: datetime, end: datetime):
        pass


if __name__ == "__main__":
    now = datetime.now()
    prov = NSTrainProvider()
    trains = prov.get_trains(now - timedelta(weeks=1), now)
