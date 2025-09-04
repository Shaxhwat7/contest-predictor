from datetime import datetime, timedelta
from typing import Annotated, List, Optional
from fastapi import APIRouter, Request
from pydantic import BaseModel, Field, NonNegativeInt, conint
from core.db.models import Contest

router = APIRouter(
    prefix="/contests",
    tags=["contests"]
)

class ContestUserStats(BaseModel):
    slug:str
    name:str
    start_time:datetime
    users_us:Optional[int]=None
    users_cn :Optional[int]=None

@router.get("/last-ten-stats")
async def get_last_ten_contests_user_stats(request:Request)->List[ContestUserStats]:
    contests = (
        await Contest.find(
            Contest.startTime>datetime.utcnow() - timedelta(days=60),
            Contest.user_num_us>=0,
            Contest.user_num_cn>=0,
            projection_model=ContestUserStats,
            )
            .sort(-Contest.startTime)
            .limit(10)
            .to_list()
    )
    return contests

@router.get("/count")
async def get_contests_count(request:Request, include_archieved:Optional[bool]=False)->int:
    if include_archieved:
        return await Contest.count()
    return await Contest.find(Contest.predict_time > datetime(2015,1,1)).count()

@router.get("/")
async def list_contests(
    request:Request,
    include_archieved:Optional[bool]=False,
    skip:Optional[NonNegativeInt]=0,
    limit:Annotated[int, Field(ge=1, le=25)]=10
)->List[Contest]:
    query = Contest.find_all() if include_archieved else Contest.find(Contest.predict_time>datetime(2015,1,1))
    records = query.sort(-Contest.startTime).skip(skip).limit(limit).to_list()
    return records