from fastapi import APIRouter

from aid.provide.ns_trains import NSTrainProvider

router = APIRouter(prefix="/datamon", tags=["datamon"])


@router.get("/trains")
def get_current_train_record_count() -> int:
    """Get train record count for last 10 seconds."""
    return NSTrainProvider().get_current_count()
