import asyncio
from typing import Annotated, List, Optional
from beanie.operators import In
from pydantic import BaseModel, Field, NonNegativeInt, conint, conlist
from fastapi import APIRouter, Request

from api.contest_util import validate_contest
from core.db.models import PredictRecord, ArchiveRecord
from core.db.views import UserKey

router = APIRouter(
    prefix="/contest-records",
    tags=["contest_records"]
)

@router.get("/count")
async def get_records_count(
    reqeust:Request,
    contest_slug:str,
    archi:Optional[bool]=False
) -> int:
    await validate_contest(contest_slug)
    Model = PredictRecord if not archi else ArchiveRecord
    return await Model.find(
        Model.contest_name == contest_slug,
        Model.score!=0,
    ).count()

@router.get("/")
async def get_records(
    request: Request,
    contest_slug: str,
    archi: Optional[bool] = False,
    skip: Optional[NonNegativeInt] = 0,
    limit: Annotated[int, Field(ge=1, le=100)] = 25,
)->List[PredictRecord | ArchiveRecord]:
    await validate_contest(contest_slug)
    Model = PredictRecord if not archi else ArchiveRecord

    records = (
        await Model.find(
            Model.contest_name == contest_slug,
            Model.score!=0
        )
        .sort(Model.rank)
        .skip(skip)
        .limit(limit)
        .to_list()
    )
    return records

@router.get("/user")
async def get_user_records(
    request:Request,
    contest_slug:str,
    username:str,
    archi:Optional[bool] = False,
)->List[PredictRecord | ArchiveRecord]:
    await validate_contest(contest_slug)
    Model = PredictRecord if not archi else ArchiveRecord
    records = await Model.find(
        Model.contest_name == contest_slug,
        In(Model.username, [username, username.lower()]),
        Model.score!=0,
    ).to_list()
    return records

class PredictedRatingQuery(BaseModel):
    contest_slug: str
    users: Annotated[list[UserKey], Field(min_length=1, max_length=26)]

class PredictedRatingResult(BaseModel):
    old_rating:Optional[float] = None
    new_rating:Optional[float] = None
    delta_rating:Optional[float] = None

@router.post("/predicted-rating")
async def get_predicted_rating(
    request:Request,
    query:PredictedRatingQuery,
)->List[Optional[PredictedRatingResult]]:
    await validate_contest(query.contest_slug)
    tasks = (
        PredictRecord.find_one(
            PredictRecord.contest_name ==query.contest_slug,
            PredictRecord.data_region == user.data_region,
            PredictRecord.username == user.username,
            projection_model = PredictedRatingResult,
        )
        for user in query.users
    )
    return await asyncio.gather(*tasks)


class RealTimeRankQuery(BaseModel):
    contest_slug:str
    user:UserKey

class RealTimeRankResult(BaseModel):
    real_time_rank : Optional[list]

@router.post("/real-time-rank")
async def get_real_time_rank(
    request:Request,
    query:RealTimeRankQuery,
)->RealTimeRankResult:
    await validate_contest(query.contest_slug)

    return await ArchiveRecord.find_one(
        ArchiveRecord.contest_name == query.contest_slug,
        ArchiveRecord.data_region == query.user.data_region,
        ArchiveRecord.username == query.user.username,
        projection_model=RealTimeRankResult,
    )