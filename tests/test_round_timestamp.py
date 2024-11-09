from datetime import datetime

from aid.collect import BaseCollector


class TestRoundTimestamp:
    def test_round_timestamp(self):
        some_time = datetime(2024, 10, 30, 8, 30, 12)
        rounded_time = BaseCollector._round_timestamp(some_time)
        assert rounded_time == datetime(2024, 10, 30, 8, 30, 10)
