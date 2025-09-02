import asyncio
from datetime import datetime, timedelta
from typing import Optional

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger
from core.base.predictor import predict_contests
from core.settings import (
    WEEKLY_CONTEST_REF,
    WEEKLY_CONTEST_TIME,
    BIWEEKLY_CONTEST_REF,
    BIWEEKLY_CONTEST_TIME,
)
from core.handler.contests import (
    is_cn_data_ready,
    save_recent_and_next_two_contests,
)
from core.handler.contests_records import (
    save_predict_contest_records,
    save_archive_contest_records,
)
from core.utils import log_exceptions_reraise, weeks_passed_since

scheduler_instance: Optional[AsyncIOScheduler] = None


@log_exceptions_reraise
async def update_last_contests() -> None:
    now = datetime.utcnow()

    passed_biweeks = weeks_passed_since(now, BIWEEKLY_CONTEST_REF.start_dt)
    biweek_contest_name = f"biweekly-contest-{BIWEEKLY_CONTEST_REF.number + passed_biweeks // 2}"
    logger.info(f"Updating {biweek_contest_name} archive")
    await save_archive_contest_records(contest_name=biweek_contest_name, data_region="CN")

    passed_weeks = weeks_passed_since(now, WEEKLY_CONTEST_REF.start_dt)
    week_contest_name = f"weekly-contest-{WEEKLY_CONTEST_REF.number + passed_weeks}"
    logger.info(f"Updating {week_contest_name} archive")
    await save_archive_contest_records(contest_name=week_contest_name, data_region="CN")


@log_exceptions_reraise
async def run_prediction_pipeline(contest_name: str, max_attempts: int = 300) -> None:
    """Run the full prediction workflow for a contest"""
    attempt = 1
    while not (ready := await is_cn_data_ready(contest_name)) and attempt < max_attempts:
        await asyncio.sleep(60)
        attempt += 1
    if not ready:
        logger.error(f"Data incomplete after {attempt} attempts, continuing...")

    await save_recent_and_next_two_contests()
    await save_predict_contest_records(contest_name=contest_name, data_region="CN")
    await predict_contests(contest_name=contest_name)
    await save_archive_contest_records(contest_name=contest_name, data_region="CN", save_users=False)


async def pre_cache_users(contest_name: str) -> None:
    await save_predict_contest_records(contest_name, "CN")
    await save_predict_contest_records(contest_name, "US")


async def schedule_contest_jobs(contest_name: str) -> None:
    now = datetime.utcnow()
    global scheduler_instance

    for offset in [25, 70]:
        scheduler_instance.add_job(
            pre_cache_users,
            kwargs={"contest_name": contest_name},
            trigger="date",
            run_date=now + timedelta(minutes=offset),
        )

    scheduler_instance.add_job(
        run_prediction_pipeline,
        kwargs={"contest_name": contest_name},
        trigger="date",
        run_date=now + timedelta(minutes=95),
    )


async def dispatch_jobs() -> None:
    global scheduler_instance
    now = datetime.utcnow()
    current_time = (now.weekday(), now.hour, now.minute)

    if current_time == (WEEKLY_CONTEST_TIME.weekday, WEEKLY_CONTEST_TIME.hour, WEEKLY_CONTEST_TIME.minute):
        contest_num = weeks_passed_since(now, WEEKLY_CONTEST_REF.start_dt)
        contest_name = f"weekly-contest-{WEEKLY_CONTEST_REF.number + contest_num}"
        logger.info(f"Scheduling jobs for {contest_name}")
        await schedule_contest_jobs(contest_name)

    elif current_time == (BIWEEKLY_CONTEST_TIME.weekday, BIWEEKLY_CONTEST_TIME.hour, BIWEEKLY_CONTEST_TIME.minute):
        contest_num = weeks_passed_since(now, BIWEEKLY_CONTEST_REF.start_dt)
        if contest_num % 2 != 0:
            logger.info(f"Skipping biweekly contest, week count={contest_num}")
            return
        contest_name = f"biweekly-contest-{BIWEEKLY_CONTEST_REF.number + contest_num // 2}"
        logger.info(f"Scheduling jobs for {contest_name}")
        await schedule_contest_jobs(contest_name)

    else:
        scheduler_instance.add_job(save_recent_and_next_two_contests, trigger="date", run_date=now + timedelta(minutes=1))
        scheduler_instance.add_job(update_last_contests, trigger="date", run_date=now + timedelta(minutes=10))

    jobs = scheduler_instance.get_jobs()
    if len(jobs) > 1:
        logger.info(f"Scheduled jobs: {', '.join(str(job) for job in jobs)}")


async def start_scheduler() -> None:
    global scheduler_instance
    if scheduler_instance:
        logger.error("Scheduler already started!")
        return

    scheduler_instance = AsyncIOScheduler(timezone=pytz.utc)
    scheduler_instance.add_job(dispatch_jobs, "interval", minutes=1)
    scheduler_instance.start()
    logger.success("Scheduler started successfully")
