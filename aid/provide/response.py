from fastapi import Response


class CSVResponse(Response):
    """Custom response for returning CSV data."""

    media_type = "text/csv"
