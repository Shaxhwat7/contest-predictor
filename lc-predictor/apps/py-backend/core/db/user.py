from datetime import datetime, timedelta
from typing import List

from beanie.odm.operators.update.general import Set
from loguru import logger

from settings import NEW_USER_CONTESTS_ATTENDED, NEW_USER_INITIAL_RATING
from crawler.user import request_user_rating_and_attended_contests_count
from db.models import DATA_REGION, ArchiveRecord, PredictRecord, User
from db.database import get_async_mongodb_collection
from db.views import UserKey
from utils import log_exceptions_reraise, gather_with_limit


async def _upsert_user_rating_and_contests(
    data_region: DATA_REGION,
    username: str,
    save_new_user: bool = True,
) -> None:

    try:
        rating, attended_count = await request_user_rating_and_attended_contests_count(
            data_region, username
        )
        if rating is None:
            logger.info(f"New user detected or no data: {data_region=} {username=}")
            if not save_new_user:
                logger.info(f"{save_new_user=} - skipping insertion")
                return
            rating = NEW_USER_INITIAL_RATING
            attended_count = NEW_USER_CONTESTS_ATTENDED

        user = User(
            username=username,
            user_slug=username,
            data_region=data_region,
            rating=rating,
            attendedContestsCount=attended_count,
        )

        await User.find_one(
            User.username == user.username,
            User.data_region == user.data_region,
        ).upsert(
            Set({
                User.update_time: user.update_time,
                User.rating: user.rating,
                User.attendedContestsCount: user.attendedContestsCount,
            }),
            on_insert=user,
        )
    except Exception as e:
        logger.exception(f"Failed to update user {data_region=} {username=}: {e}")


@log_exceptions_reraise
async def update_all_users(batch_size: int = 100) -> None:

    total_users = await User.count()
    logger.info(f"Total users in DB: {total_users}")

    for i in range(0, total_users, batch_size):
        logger.info(f"Progress: {i / total_users * 100:.2f}%")
        docs: List[UserKey] = await (
            User.find_all()
            .sort(-User.rating)
            .skip(i)
            .limit(batch_size)
            .project(UserKey)
            .to_list()
        )

        cn_tasks, us_tasks = [], []
        for doc in docs:
            task = _upsert_user_rating_and_contests(doc.data_region, doc.username, False)
            if doc.data_region == "CN":
                cn_tasks.append(task)
            else:
                us_tasks.append(task)

        await gather_with_limit(
            [
                gather_with_limit(cn_tasks, 1),
                gather_with_limit(us_tasks, 5),
            ],
            30,
        )


@log_exceptions_reraise
async def save_users_of_contest(contest_name: str, predict: bool) -> None:

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
                                        {"$gte": ["$update_time", datetime.utcnow() - timedelta(hours=36)]},
                                    ]
                                }
                            }
                        }
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

    docs = await (await col.aggregate(pipeline)).to_list(length=None)
    logger.info(f"Users to update: {len(docs)}")

    cn_tasks, us_tasks = [], []
    for doc in docs:
        task = _upsert_user_rating_and_contests(doc["data_region"], doc["username"])
        if doc["data_region"] == "CN":
            cn_tasks.append(task)
        else:
            us_tasks.append(task)

    await gather_with_limit(
        [
            gather_with_limit(cn_tasks, 1),  
            gather_with_limit(us_tasks, 5),
        ],
        30,
    )
