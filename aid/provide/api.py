import os
from datetime import datetime, timedelta

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Security, HTTPException, Depends
from fastapi.openapi.models import APIKey
from fastapi.security import APIKeyHeader
from starlette.responses import RedirectResponse
from starlette.status import HTTP_403_FORBIDDEN

from aid.provide.ns_trains import TrainRecord, NSTrainProvider

app = FastAPI()

api_key_header = APIKeyHeader(name="x-api-key", auto_error=False)


def get_api_key(api_key_header: str = Security(api_key_header)):
    if api_key_header == os.getenv("API_KEY"):
        return api_key_header
    else:
        raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="API key missing or invalid")


@app.get("/", include_in_schema=False)
def redirect_root_to_docs():
    return RedirectResponse(url="/docs")


@app.get("/trains")
def get_trains(
    api_key: APIKey = Depends(get_api_key), start: datetime | None = None, end: datetime | None = None
) -> list[TrainRecord]:
    end = end or datetime.now()
    start = start or (end - timedelta(seconds=10))
    return NSTrainProvider().get_trains(start, end)


if __name__ == "__main__":
    load_dotenv()
    uvicorn.run(app)
