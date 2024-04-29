import os
from datetime import datetime, timedelta

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Security, HTTPException, Depends
from fastapi.openapi.models import APIKey
from fastapi.security import APIKeyHeader
from starlette.status import HTTP_403_FORBIDDEN

from aid.provide.ns_trains import TrainRecord, NSTrainProvider

description = """
A data provider API to augment fine art with live data.

Check out the [Artistic Intelligence website](https://artisticintelligence.nl/) for more information!

This API (and data collector) is open source, 
[find it on GitHub](https://github.com/robinklaassen/artistic-intelligence-data).
"""

tags_metadata = [
    {
        "name": "trains",
        "description": "Train locations every 10 seconds from the NS VirtualTrain API.",
    }
]

app = FastAPI(
    title="AID - Artistic Intelligence Data",
    description=description,
    openapi_tags=tags_metadata,
    contact={
        "name": "Robin Klaassen",
        "url": "https://robinklaassen.com",
    },
    docs_url="/",
    redoc_url=None,
)

api_key_header = APIKeyHeader(name="x-api-key", auto_error=False)


def get_api_key(resolved_header: str = Security(api_key_header)) -> str:
    if resolved_header == os.getenv("API_KEY"):
        return resolved_header
    else:
        raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="API key missing or invalid")


@app.get("/trains/current", tags=["trains"])
def get_current_train_locations() -> list[TrainRecord]:
    """
    Get current train locations (last 10 seconds).
    """
    end = datetime.now()
    start = end - timedelta(seconds=10)
    return NSTrainProvider().get_trains(start, end)


@app.get("/trains/period", tags=["trains"])
def get_train_locations_in_period(
    start: datetime, end: datetime | None = None, api_key: APIKey = Depends(get_api_key)
) -> list[TrainRecord]:
    """
    Get the train locations for a specified period.

    - **start**: start of the requested period (timestamp, required)
    - **end**: end of the requested period (timestamp, defaults to current time)
    """
    end = end or datetime.now()
    return NSTrainProvider().get_trains(start, end)


if __name__ == "__main__":
    load_dotenv()
    uvicorn.run(app)
