import asyncio
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Request
from loguru import logger
from pydantic import BaseModel, Field, NonNegativeInt, conlist
from core.db.models import Question
from api.contest_util import validate_contest

router = APIRouter(
    prefix="/questions",
    tags=["questions"]
)

class QuestionQuery(BaseModel):
    contest_slug: Optional[str] = None
    questions_ids: Optional[List[NonNegativeInt]] = Field(
        default=None,
        min_items=1,
        max_items=4
    )

@router.get("/")
async def get_questions(request:Request, query:QuestionQuery)->List[Question]:
    if not (bool(query.contest_slug) ^ bool(query.question_ids)):
        msg = "Provide exactly one: contest_slug OR question_ids"
        logger.warning(msg)
        raise HTTPException(status_code=400, detail=msg)

    if query.contest_slug:
        await validate_contest(query.contest_slug)
        return await Question.find(Question.contest_name == query.contest_slug).to_list()

    tasks = (Question.find_one(Question.question_id == qid) for qid in query.question_ids)
    return await asyncio.gather(*tasks)