from datetime import datetime
from typing import Counter, List, Literal, Optional, Tuple

from beanie import Document
from pydantic import Field
from pymongo import IndexModel

from .schemas import PredictionEvent, UserContestHistoryRecord

DATA_REGION = Literal["CN", "US"]


class Contest(Document):
    titleSlug: str
    title: str
    startTime: datetime
    duration: int
    endTime: datetime
    past: bool
    update_time: datetime = Field(default_factory=datetime.utcnow)
    predict_time: Optional[datetime] = None
    user_num_us: Optional[int] = None
    user_num_cn: Optional[int] = None
    convolution_array: Optional[int] = None
    prediction_progress: Optional[List[PredictionEvent]] = None

    class Settings:
        indexes = [
            IndexModel("titleSlug", unique=True),
            "title",
            "startTime",
            "endTime",
            "predict_time",
        ]


class ContestRecordBase(Document):
    contest_name: str
    contest_id: int
    username: str
    user_slug: str
    data_region: DATA_REGION
    country_code: Optional[str] = None
    country_name: Optional[str] = None
    rank: int
    score: int
    finish_time: datetime
    attendedContestsCount: Optional[int] = None
    old_rating: Optional[float] = None
    new_rating: Optional[float] = None
    delta_rating: Optional[float] = None

    class Settings:
        indexes = [
            "contest_name",
            "username",
            "user_slug",
            "rank",
            "data_region",
        ]


class PredictRecord(ContestRecordBase):
    insert_time: datetime = Field(default_factory=datetime.utcnow)
    predict_time: Optional[datetime] = None


class ArchiveRecord(ContestRecordBase):
    update_time: datetime = Field(default_factory=datetime.utcnow)
    real_time_rank: Optional[List[int]] = None


class Question(Document):
    question_id: int
    credit: int
    title: str
    title_slug: str
    update_time: datetime = Field(default_factory=datetime.utcnow)
    contest_name: str
    qi: int
    real_time_count: Optional[List[int]] = None
    user_ratings_quantiles: Optional[List[float]] = None
    user_ratings_bins: Optional[List[Tuple[int, int]]] = None
    average_fail_count: Optional[int] = None
    lang_counter: Optional[Counter] = None
    difficulty: Optional[float] = None
    first_ten_users: Optional[List[Tuple[str, datetime]]] = None
    topics: Optional[List[str]] = None

    class Settings:
        indexes = [
            "question_id",
            "title_slug",
            "contest_name",
        ]


class Submission(Document):
    contest_name: str
    username: str
    data_region: DATA_REGION
    question_id: int
    date: datetime
    fail_count: int
    credit: int
    submission_id: int
    status: int
    contest_id: int
    update_time: datetime = Field(default_factory=datetime.utcnow)
    lang: Optional[str] = None  

    class Settings:
        indexes = [
            "contest_name",
            "username",
            "data_region",
            "question_id",
            "date",
        ]


class User(Document):
    username: str
    user_slug: str
    data_region: DATA_REGION
    attendedContestsCount: int
    rating: float
    update_time: datetime = Field(default_factory=datetime.utcnow)
    contest_history: Optional[List[UserContestHistoryRecord]] = None
    avatar_url: Optional[str] = None

    class Settings:
        indexes = [
            "username",
            "user_slug",
            "data_region",
            "rating",
        ]
