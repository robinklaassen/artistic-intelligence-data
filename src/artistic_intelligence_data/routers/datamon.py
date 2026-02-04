from fastapi import APIRouter

from artistic_intelligence_data.trains.questdb_train_provider import QuestDBTrainProvider

router = APIRouter(prefix="/datamon", tags=["datamon"])


@router.get("/trains")
def get_current_train_record_count() -> int:
    """Get train record count for the last minute."""
    return QuestDBTrainProvider().get_current_count()
