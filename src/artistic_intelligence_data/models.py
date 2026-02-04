from datetime import datetime

from pydantic import BaseModel


class TrainRecord(BaseModel):
    timestamp: datetime
    id: int
    x: int
    y: int
    speed: float
    direction: float
    accuracy: float
    type: str


class TrainType(BaseModel):
    id: int
    type: str
