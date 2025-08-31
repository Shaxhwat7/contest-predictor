# app/handler/question_data.py
import asyncio
from datetime import datetime, timedelta
from typing import List

from beanie.odm.operators.update.general import Set
from loguru import logger

from crawler.question import request_question_list
from db.models import Question, Submission
from utils import gather_with_limit, get_contest_start_time


async def _count_submissions_at_time(
    contest_name: str, question_id: int, time_point: datetime
) -> int:
    """
    Count finished submissions for a question at a specific time point.
    """
    return await Submission.find(
        Submission.contest_name == contest_name,
        Submission.question_id == question_id,
        Submission.date <= time_point,
    ).count()


async def save_questions_real_time_count(
    contest_name: str, delta_minutes: int = 1
) -> None:
    """
    Track accepted submissions for each question at intervals of delta_minutes.
    """
    start_time = get_contest_start_time(contest_name)
    end_time = start_time + timedelta(minutes=90)

    time_points = []
    current_time = start_time
    while (current_time := current_time + timedelta(minutes=delta_minutes)) <= end_time:
        time_points.append(current_time)

    logger.info(f"{contest_name=} {time_points=}")

    questions = await Question.find(Question.contest_name == contest_name).to_list()
    for question in questions:
        tasks = [
            _count_submissions_at_time(contest_name, question.question_id, tp)
            for tp in time_points
        ]
        question.real_time_count = await gather_with_limit(tasks)
        await question.save()

    logger.success("Finished saving real-time question counts.")


async def save_questions(contest_name: str) -> List[Question]:
    """
    Fetch and save all questions for a given contest into MongoDB.
    """
    try:
        question_list = await request_question_list(contest_name)
        current_time = datetime.utcnow()
        additional_fields = {"contest_name": contest_name}

        questions = []
        for idx, question_data in enumerate(question_list):
            question_data.pop("id", None)
            question_data["qi"] = idx + 1
            question_obj = Question.model_validate(question_data | additional_fields)
            questions.append(question_obj)

        upsert_tasks = [
            Question.find_one(
                Question.question_id == q.question_id,
                Question.contest_name == contest_name,
            ).upsert(
                Set({
                    Question.credit: q.credit,
                    Question.title: q.title,
                    Question.title_slug: q.title_slug,
                    Question.update_time: q.update_time,
                    Question.qi: q.qi,
                }),
                on_insert=q,
            )
            for q in questions
        ]
        await asyncio.gather(*upsert_tasks)

        # Remove outdated questions
        await Question.find(
            Question.contest_name == contest_name,
            Question.update_time < current_time,
        ).delete()

        logger.success(f"Finished saving questions for {contest_name}.")
        return questions

    except Exception as e:
        logger.error(f"Failed to save questions for {contest_name}: {e}")
        return []
