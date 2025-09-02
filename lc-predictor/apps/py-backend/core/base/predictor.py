from datetime import datetime
from typing import List
import numpy as np
from beanie.odm.operators.update.general import Set
from loguru import logger

from core.base.elo import elo_delta
from core.db.models import Contest, PredictRecord, User
from core.utils import gather_with_limit, log_exceptions_reraise

async def update_user_rating_immediately(records:List[PredictRecord])->None:
    logger.info("Writing predicted results back into User collection immediately...")

    tasks = [
        User.find_one(
            User.username == record.username,
            User.data_region == record.data_region,
        ).update(
            Set({
                User.rating:record.new_rating,
                User.attendedContestsCount : record.attendedContestsCount+1,
                User.update_time:datetime.utcnow(),
            })
        )
        for record in records
    ]
    await gather_with_limit(tasks, max_concurrency=50)
    logger.success("Finished updating User collection using predicted results.")

@log_exceptions_reraise
async def predict_contests(contest_name:str)->None:
    records = (
        await PredictRecord.find(
            PredictRecord.contest_name == contest_name,
            PredictRecord.score!=0,
        )
        .sort(PredictRecord.rank)
        .to_list()
    )
    if not records:
        logger.warning(f"No records found for contest:{contest_name}")
        return 

    ranks = np.array([r.rank for r in ranks])
    old_ratings = np.array([r.old_ratings for r in records])
    k_values = np.array([r.attendedContestsCount for r in records] )

    delta_ratings = elo_delta(ranks, old_ratings, k_values)
    new_ratings = old_ratings+delta_ratings
    predict_time = datetime.utcnow()

    for i,record in enumerate(records):
        record.delta_rating = delta_ratings[i]
        record.new_rating = new_ratings[i]
        record.predict_time = predict_time

    await gather_with_limit([r.save() for r in records], max_concurrency=50)
    logger.success("Updated ContestRecord with predicted ratings.")

    if contest_name.lower().startswith("bi"):
        await update_user_rating_immediately(records)
    await Contest.find_one(Contest.titleSlug == contest_name).update(
        Set({Contest.predict_time:datetime.utcnow()})
    )
    logger.info("Updated predict_time in Contest collection.")