import asyncio

from loguru import logger

from core.db.database import start_async_mongodb
from core.schedule import start_scheduler
from core.utils import init_loguru


async def start() -> None:
    init_loguru()
    await start_async_mongodb()
    await start_scheduler()
    logger.success("started all entry functions")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.create_task(start())
    try:
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit) as e:
        logger.critical(f"Closing loop. {e=}")
    finally:
        loop.close()
        logger.critical("Closed loop.")
