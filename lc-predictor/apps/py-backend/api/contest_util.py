from fastapi import HTTPException
from loguru import logger
from core.db.models import Contest as ContestModel

async def validate_contest(contest_slug:str)->None:
    contest_record = await ContestModel.find_one(ContestModel.titleSlug == contest_slug)
    if contest_record is None:
        msg = f"No contest found with slug: '{contest_slug}'"
        logger.warning(msg)
        raise HTTPException(status_code=404, detail=msg)