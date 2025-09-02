from datetime import datetime
from typing import List, Tuple

from beanie.odm.operators.update.general import Set
from loguru import logger

from core.crawler.contests_records import request_contest_records
from core.db.models import DATA_REGION, ArchiveRecord, PredictRecord, User
from core.handler.submission import save_submission
from core.handler.users import save_users_of_contest
from core.utils import log_exceptions_reraise, gather_with_limit


@log_exceptions_reraise
async def save_predict_contest_records(
    contest_name: str,
    data_region: DATA_REGION,
) -> None:
    async def _fill_user_rating_and_contest_count(record: PredictRecord):
        """Fill old_rating and attendedContestsCount for each user."""
        user = await User.find_one(
            User.username == record.username,
            User.data_region == record.data_region,
        )
        if user:
            record.old_rating = user.rating
            record.attendedContestsCount = user.attendedContestsCount
            await record.save()

    contest_record_list, _ = await request_contest_records(contest_name, data_region)

    await PredictRecord.find(
        PredictRecord.contest_name == contest_name,
    ).delete()

    contest_records = []
    unique_keys = set()

    for record_dict in contest_record_list:
        if data_region == "US":
            record_dict["username"] = record_dict.get("user_slug", record_dict.get("username"))

        key = (record_dict["data_region"], record_dict["username"])
        if key in unique_keys:
            logger.warning(f"Duplicate user record found: {record_dict=}")
            continue

        unique_keys.add(key)
        record_dict["contest_name"] = contest_name

        contest_record = PredictRecord.model_validate(record_dict)
        contest_records.append(contest_record)

    insert_tasks = [PredictRecord.insert_one(r) for r in contest_records]
    await gather_with_limit(insert_tasks, max_con_num=50)

    await save_users_of_contest(contest_name=contest_name, predict=True)

    fill_tasks = [
        _fill_user_rating_and_contest_count(record)
        for record in contest_records
        if record.score > 0
    ]
    await gather_with_limit(fill_tasks, max_con_num=50)


@log_exceptions_reraise
async def save_archive_contest_records(
    contest_name: str,
    data_region: DATA_REGION = "US",
    save_users: bool = True,
) -> None:
    time_point = datetime.utcnow()

    contest_record_list, nested_submission_list = await request_contest_records(
        contest_name, data_region
    )

    contest_records = []

    for record_dict in contest_record_list:
        if data_region == "US":
            record_dict["username"] = record_dict.get("user_slug", record_dict.get("username"))

        record_dict["contest_name"] = contest_name
        contest_record = ArchiveRecord.model_validate(record_dict)
        contest_records.append(contest_record)

    upsert_tasks = [
        ArchiveRecord.find_one(
            ArchiveRecord.contest_name == record.contest_name,
            ArchiveRecord.username == record.username,
            ArchiveRecord.data_region == record.data_region,
        ).upsert(
            Set({
                ArchiveRecord.rank: record.rank,
                ArchiveRecord.score: record.score,
                ArchiveRecord.finish_time: record.finish_time,
                ArchiveRecord.update_time: record.update_time,
            }),
            on_insert=record,
        )
        for record in contest_records
    ]
    await gather_with_limit(upsert_tasks, max_con_num=50)

    await ArchiveRecord.find(
        ArchiveRecord.contest_name == contest_name,
        ArchiveRecord.update_time < time_point,
    ).delete()

    if save_users:
        await save_users_of_contest(contest_name=contest_name, predict=False)
    else:
        logger.info(f"Skipping user save as save_users={save_users}")

    await save_submission(contest_name, contest_record_list, nested_submission_list)
