import sys 
import urllib.parse

from typing import Optional
from beanie import init_beanie
from loguru import logger
from motor.core import AgnosticClient, AgnosticCollection, AgnosticDatabase
from motor.motor_asyncio import AsyncIOMotorClient

from models import (
    Contest, 
    PredictRecord,
    ArchiveRecord,
    Question,
    Submission,
    User,
)
from config import get_yaml_config

async_mongodb_client : Optional[AsyncIOMotorClient] = None
def get_mongodb_config():
    config = get_yaml_config()
    return config.get("mongodb")


def get_async_mongodb_client() -> AgnosticClient:
    global async_mongodb_client
    if async_mongodb_client is None:
        cfg = get_mongodb_config()
        ip = cfg.get("ip")
        port = cfg.get("port")
        username = urllib.parse.quote_plus(cfg.get("username"))
        password = urllib.parse.quote_plus(cfg.get("password"))
        db = cfg.get("db")
        async_mongodb_client = AsyncIOMotorClient(
            f"mongodb://{username}:{password}@{ip}:{port}/{db}"
        )
    return async_mongodb_client


def get_async_mongodb_database(db_name: Optional[str] = None) -> AgnosticDatabase:
    if db_name is None:
        db_name = get_mongodb_config().get("db")
    client = get_async_mongodb_client()
    return client[db_name]


def get_async_mongodb_collection(col_name: str) -> AgnosticCollection:
    db = get_async_mongodb_database()
    return db[col_name]


async def start_async_mongodb() -> None:
    try:
        async_mongodb_database = get_async_mongodb_database()
        await init_beanie(
            database=async_mongodb_database,
            document_models=[
                Contest,
                PredictRecord,
                ArchiveRecord,
                User,
                Submission,
                Question,
            ],
        )
        logger.success("MongoDB connection started successfully")
    except Exception as e:
        logger.exception(f"Failed to start MongoDB. Error={e}")
        sys.exit(1)