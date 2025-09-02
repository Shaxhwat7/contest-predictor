# app/handler/submission_data.py
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from beanie.odm.operators.update.general import Set
from loguru import logger

from core.db.models import ArchiveRecord, Submission
from core.db.database import get_async_mongodb_collection
from core.db.views import UserKey
from core.handler.questions import save_questions, save_questions_real_time_count
from core.utils import (
    log_exceptions_reraise,
    gather_with_limit,
    infer_contest_start,
)


async def _aggregate_rank_at_time_point(
    contest_name: str,
    time_point: datetime,
) -> Tuple[Dict[Tuple[str, str], int], int]:

    col = get_async_mongodb_collection(ArchiveRecord.__name__)
    rank_map: Dict[Tuple[str, str], int] = {}
    last_credit_sum = last_penalty_date = None
    tie_rank = raw_rank = 0

    pipeline = [
        {"$match": {"contest_name": contest_name, "date": {"$lte": time_point}}},
        {
            "$group": {
                "_id": {"username": "$username", "data_region": "$data_region"},
                "credit_sum": {"$sum": "$credit"},
                "fail_count_sum": {"$sum": "$fail_count"},
                "date_max": {"$max": "$date"},
            }
        },
        {
            "$addFields": {
                "penalty_date": {
                    "$dateAdd": {
                        "startDate": "$date_max",
                        "unit": "minute",
                        "amount": {"$multiply": [5, "$fail_count_sum"]},
                    }
                },
                "username": "$_id.username",
                "data_region": "$_id.data_region",
            }
        },
        {"$unset": ["_id"]},
        {"$sort": {"credit_sum": -1, "penalty_date": 1}},
        {"$project": {"_id": 0, "username": 1, "data_region": 1, "credit_sum": 1,
                      "fail_count_sum": 1, "penalty_date": 1}},
    ]

    async for doc in col.aggregate(pipeline):
        raw_rank += 1
        if doc["credit_sum"] == last_credit_sum and doc["penalty_date"] == last_penalty_date:
            rank_map[(doc["username"], doc["data_region"])] = tie_rank
        else:
            tie_rank = raw_rank
            rank_map[(doc["username"], doc["data_region"])] = raw_rank

        last_credit_sum = doc["credit_sum"]
        last_penalty_date = doc["penalty_date"]

    return rank_map, raw_rank


async def _save_real_time_rank(
    contest_name: str,
    delta_minutes: int = 1,
) -> None:
    """
    Compute and save real-time ranks for each user in the contest
    at every delta_minutes interval.
    """
    logger.info("Starting real-time rank computation")
    users = (
        await ArchiveRecord.find(
            ArchiveRecord.contest_name == contest_name,
            ArchiveRecord.score != 0,
        )
        .project(UserKey)
        .to_list()
    )

    real_time_rank_map = {(user.username, user.data_region): [] for user in users}
    start_time = infer_contest_start(contest_name)
    end_time = start_time + timedelta(minutes=90)
    step = 1

    while (start_time := start_time + timedelta(minutes=delta_minutes)) <= end_time:
        rank_map, last_rank = await _aggregate_rank_at_time_point(contest_name, start_time)
        last_rank += 1

        for key, rank in rank_map.items():
            real_time_rank_map[key].append(rank)
        for key in real_time_rank_map.keys():
            if len(real_time_rank_map[key]) != step:
                real_time_rank_map[key].append(last_rank)

        step += 1

    update_tasks = [
        ArchiveRecord.find_one(
            ArchiveRecord.contest_name == contest_name,
            ArchiveRecord.username == username,
            ArchiveRecord.data_region == data_region,
        ).update(Set({ArchiveRecord.real_time_rank: rank_list}))
        for (username, data_region), rank_list in real_time_rank_map.items()
    ]

    logger.info("Updating real_time_rank field in ContestRecordArchive collection")
    await gather_with_limit(update_tasks, max_con_num=5)
    logger.success(f"Finished updating real_time_rank for {contest_name}")


@log_exceptions_reraise
async def save_submission(
    contest_name: str,
    contest_record_list: List[Dict],
    nested_submission_list: List[Dict],
) -> None:

    timestamp_now = datetime.utcnow()
    questions = await save_questions(contest_name)
    question_credit_map = {q.question_id: q.credit for q in questions}

    submissions = []
    for record_dict, submission_dict_map in zip(contest_record_list, nested_submission_list):
        for question_id, submission_dict in submission_dict_map.items():
            submission_dict.pop("id", None)
            submission_dict |= {
                "contest_name": contest_name,
                "username": record_dict["username"],
                "credit": question_credit_map[int(question_id)],
            }
        submissions.extend([Submission.model_validate(s) for s in submission_dict_map.values()])

    upsert_tasks = [
        Submission.find_one(
            Submission.contest_name == s.contest_name,
            Submission.username == s.username,
            Submission.data_region == s.data_region,
            Submission.question_id == s.question_id,
        ).upsert(
            Set({
                Submission.date: s.date,
                Submission.fail_count: s.fail_count,
                Submission.credit: s.credit,
                Submission.update_time: s.update_time,
                Submission.lang: s.lang,
            }),
            on_insert=s,
        )
        for s in submissions
    ]

    logger.info("Updating Submission collection")
    await gather_with_limit(upsert_tasks, max_con_num=5)

    await Submission.find(
        Submission.contest_name == contest_name,
        Submission.update_time < timestamp_now,
    ).delete()

    logger.success("Finished updating submissions, computing real-time question counts and ranks")
    await save_questions_real_time_count(contest_name)
    await _save_real_time_rank(contest_name)
