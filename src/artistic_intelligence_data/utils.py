from datetime import datetime, timedelta

from artistic_intelligence_data.constants import DEFAULT_TIMEZONE


def validate_start_end(
    start: datetime | None = None, end: datetime | None = None, minutes: int = 1
) -> tuple[datetime, datetime]:
    """
    Validate a time range usually provided as API params, providing defaults if necessary.
    """
    now = datetime.now(tz=DEFAULT_TIMEZONE)
    start = start or now - timedelta(minutes=minutes)
    end = end or now

    if start.tzinfo is None:
        start = start.replace(tzinfo=DEFAULT_TIMEZONE)

    if end.tzinfo is None:
        end = end.replace(tzinfo=DEFAULT_TIMEZONE)

    return start, end
