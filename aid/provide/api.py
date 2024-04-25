from datetime import datetime, timedelta

import uvicorn
from fastapi import FastAPI

from aid.provide.ns_trains import TrainRecord, NSTrainProvider

app = FastAPI()
# TODO secure endpoints


# @app.get("/")
# def read_root():
#     return {"Hello": "World"}
#
#
# @app.get("/items/{item_id}")
# def read_item(item_id: int, q: str | None = None):
#     return {"item_id": item_id, "q": q}


@app.get("/trains")
def get_trains(start: datetime | None = None, end: datetime | None = None) -> list[TrainRecord]:
    end = end or datetime.now()
    start = start or (end - timedelta(seconds=10))

    provider = NSTrainProvider()
    return provider.get_trains(start, end)


if __name__ == "__main__":
    uvicorn.run(app)
