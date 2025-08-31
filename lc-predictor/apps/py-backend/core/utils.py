import asyncio
import math
import sys
from asyncio import iscoroutinefunction
from datetime import datetime, timedelta
from functools import partial, wraps
from typing import Any, Callable, Coroutine, List, Sequence, Union

from loguru import logger

from core.settings import WEEKLY_CONTEST_REF, BIWEEKLY_CONTEST_REF


async def gather_with_limit(
    coroutines: Sequence[Coroutine],
    max_concurrency: int = 10,
    return_exceptions: bool = False,
) -> List[Union[Exception, Any]]:

    semaphore = asyncio.Semaphore(max_concurrency)

    async def coro_with_semaphore(coro: Coroutine):
        async with semaphore:
            return await coro

    tasks = [coro_with_semaphore(c) for c in coroutines]
    return await asyncio.gather(*tasks, return_exceptions=return_exceptions)


def weeks_passed_since(base_time: datetime, now: datetime) -> int:

    return math.floor((now - base_time).total_seconds() / (7 * 24 * 60 * 60))


def infer_contest_start(contest_name: str) -> datetime:

    contest_num = int(contest_name.split("-")[-1])

    if contest_name.lower().startswith("weekly"):
        start_time = WEEKLY_CONTEST_REF.dt + timedelta(
            weeks=contest_num - WEEKLY_CONTEST_REF.num
        )
    else:
        start_time = BIWEEKLY_CONTEST_REF.dt + timedelta(
            weeks=(contest_num - BIWEEKLY_CONTEST_REF.num) * 2
        )

    logger.info(f"Contest {contest_name} inferred start time: {start_time}")
    return start_time


def init_loguru(process: str = "main") -> None:

    from core.config import get_yaml_config

    try:
        config = get_yaml_config().get("loguru").get(process)
        logger.add(
            sink=config["sink"],
            rotation=config["rotation"],
            level=config["level"],
        )
    except Exception as e:
        logger.exception(f"Failed to initialize loguru: {e}")
        sys.exit(1)


def log_exceptions(func: Callable[..., Any], reraise: bool) -> Callable[..., Any]:
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        try:
            logger.info(f"{func.__name__} starting...")
            result = await func(*args, **kwargs)
            logger.success(f"{func.__name__} finished.")
            return result
        except Exception as e:
            logger.exception(f"{func.__name__} error={e}")
            if reraise:
                raise e

    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        try:
            logger.info(f"{func.__name__} starting...")
            result = func(*args, **kwargs)
            logger.success(f"{func.__name__} finished.")
            return result
        except Exception as e:
            logger.exception(f"{func.__name__} args={args} kwargs={kwargs} error={e}")
            if reraise:
                raise e

    return async_wrapper if iscoroutinefunction(func) else sync_wrapper


log_exceptions_reraise = partial(log_exceptions, reraise=True)
log_exceptions_silence = partial(log_exceptions, reraise=False)
