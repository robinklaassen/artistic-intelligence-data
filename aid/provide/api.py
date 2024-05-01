import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware

from aid.provide.routers import trains

# Load the environment file to set API key and such
load_dotenv()

# Target for uvicorn.run, update when moving this module!
APP_MODULE = "aid.provide.api:app"

description = """
A data provider API to augment fine art with live data.

Check out the [Artistic Intelligence website](https://artisticintelligence.nl/) for more information!

This API (and data collector) is open source, 
[find it on GitHub](https://github.com/robinklaassen/artistic-intelligence-data).

Notes:
- These tend to be large datasets. To compress in transport, 
add the `Accept-Encoding: gzip` header to your HTTP request.
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

app.add_middleware(GZipMiddleware, minimum_size=10_000)  # minimum size in bytes


app.include_router(trains.router)


if __name__ == "__main__":
    uvicorn.run(APP_MODULE)
