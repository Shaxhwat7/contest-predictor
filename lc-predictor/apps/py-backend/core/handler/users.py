from datetime import datetime, timedelta
from math import log
from typing import List

from beanie.odm.operators.update.general import Set
from loguru import logger

from core.settings import (
    NEW_USER_CONTESTS_ATTENDED,
    NEW_USER_INITIAL_RATING,
)
from core.crawler.user import request_user_rating_and_attended_contests_count
from core.db.models import DATA_REGION, ArchiveRecord, PredictRecord, User
from core.db.database import get_async_mongodb_collection
from core.db.views import UserKey
from core.utils import log_exceptions_reraise, gather_with_limit


async def upsert_users_rating_and_attended_contests_count(
    data_region: DATA_REGION,
    username: str,
    save_new_user: bool = True,
) -> None:

    try:
        (
            rating,
            attended_contests_count,
        ) = await request_user_rating_and_attended_contests_count(data_region, username)
        if rating is None:
            logger.info(
                f"graphql data is None, new user found, {data_region=} {username=}"
            )
            if not save_new_user:
                logger.info(f"{save_new_user=} do nothing.")
                return
            rating = NEW_USER_INITIAL_RATING
            attended_contests_count = NEW_USER_CONTESTS_ATTENDED
        user = User(
            username=username,
            user_slug=username,
            data_region=data_region,
            attendedContestsCount=attended_contests_count,
            rating=rating,
        )
        await User.find_one(
            User.username == user.username,
            User.data_region == user.data_region,
        ).upsert(
            Set(
                {
                    User.update_time: user.update_time,
                    User.attendedContestsCount: user.attendedContestsCount,
                    User.rating: user.rating,
                }
            ),
            on_insert=user,
        )
    except Exception as e:
        logger.exception(f"user update error. {data_region=} {username=} Exception={e}")


@log_exceptions_reraise
async def update_all_users_in_database(
    batch_size: int = 100,
) -> None:
    """
    For all users in the User collection, update their rating and attended_contests_count.
    :param batch_size:
    :return:
    """
    total_count = await User.count()
    logger.info(f"User collection now has {total_count=}")
    for i in range(0, total_count, batch_size):
        logger.info(f"progress = {i / total_count* 100 :.2f}%")
        docs: List[UserKey] = await (
            User.find_all()
            .sort(-User.rating)
            .skip(i)
            .limit(batch_size)
            .project(UserKey)
            .to_list()
        )
        cn_tasks = []
        us_tasks = []
        for doc in docs:
            if doc.data_region == "CN":
                cn_tasks.append(
                    upsert_users_rating_and_attended_contests_count(
                        doc.data_region, doc.username, False
                    )
                )
            else:
                us_tasks.append(
                    upsert_users_rating_and_attended_contests_count(
                        doc.data_region, doc.username, False
                    )
                )
        await gather_with_limit(
            [
                gather_with_limit(cn_tasks, 1),
                gather_with_limit(us_tasks, 5),
            ],
            30,
        )


@log_exceptions_reraise
async def save_users_of_contest(
    contest_name: str,
    predict: bool,
) -> None:
    if predict:
        col = get_async_mongodb_collection(PredictRecord.__name__)
        pipeline = [
            {"$match": {"contest_name": contest_name, "score": {"$ne": 0}}},
            {
                "$lookup": {
                    "from": "User",
                    "let": {"data_region": "$data_region", "username": "$username"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$data_region", "$$data_region"]},
                                        {"$eq": ["$username", "$$username"]},
                                        {
                                            "$gte": [
                                                "$update_time",
                                                datetime.utcnow() - timedelta(hours=36),
                                            ]
                                        },
                                    ]
                                }
                            }
                        },
                    ],
                    "as": "recent_updated_user",
                }
            },
            {"$match": {"recent_updated_user": {"$eq": []}}},
            {"$project": {"_id": 0, "data_region": 1, "username": 1}},
        ]
    else:
        col = get_async_mongodb_collection(ArchiveRecord.__name__)
        pipeline = [
            {"$match": {"contest_name": contest_name}},
            {"$project": {"_id": 0, "data_region": 1, "username": 1}},
        ]
    cursor = col.aggregate(pipeline)
    docs = await cursor.to_list(length=None)
    logger.info(f"docs length = {len(docs)}")
    cn_tasks = []
    us_tasks = []
    for doc in docs:
        if doc["data_region"] == "CN":
            cn_tasks.append(
                upsert_users_rating_and_attended_contests_count(
                    doc["data_region"], doc["username"]
                )
            )
        else:
            us_tasks.append(
                upsert_users_rating_and_attended_contests_count(
                    doc["data_region"], doc["username"]
                )
            )
    await gather_with_limit(
        [
            gather_with_limit(cn_tasks, 1),
            gather_with_limit(us_tasks, 5),
        ],
        30,
    )
