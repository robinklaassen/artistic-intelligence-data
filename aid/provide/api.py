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
A data provider API to create art augmented with live data.

More information:
- [Artistic Intelligence website](https://artisticintelligence.nl/)
- [Source code of this API (including data collection)](https://github.com/robinklaassen/artistic-intelligence-data)
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


@app.get("/trains", tags=["trains"])
def get_trains(
    api_key: APIKey = Depends(get_api_key), start: datetime | None = None, end: datetime | None = None
) -> list[TrainRecord]:
    end = end or datetime.now()
    start = start or (end - timedelta(seconds=10))
    return NSTrainProvider().get_trains(start, end)


if __name__ == "__main__":
    load_dotenv()
    uvicorn.run(app)
