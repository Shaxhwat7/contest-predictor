import sys 
import os
from dotenv import load_dotenv
import urllib.parse
import asyncio
from typing import Optional
from beanie import init_beanie
from loguru import logger
from motor.core import AgnosticClient, AgnosticCollection, AgnosticDatabase
from motor.motor_asyncio import AsyncIOMotorClient

from .models import (
    Contest, 
    PredictRecord,
    ArchiveRecord,
    Question,
    Submission,
    User,
)
from core.config import get_yaml_config
load_dotenv()
async_mongodb_client : Optional[AsyncIOMotorClient] = None
def get_mongodb_config():
    configyaml = get_yaml_config()
    print(configyaml.get("mongo"))
    return configyaml.get("mongodb")


def get_async_mongodb_client() -> AsyncIOMotorClient:
    global async_mongodb_client
    if async_mongodb_client is None:
        uri = os.getenv("MONGODB_URI")
        if not uri:
            raise EnvironmentError("MONGODB_URI environment variable not set")
        async_mongodb_client = AsyncIOMotorClient(uri)
    return async_mongodb_client



def get_async_mongodb_database(db_name: Optional[str] = None) -> AgnosticDatabase:
    if db_name is None:
        db_name = os.getenv("MONGODB_DB")
        if not db_name:
            raise EnvironmentError("MONGODB_DB environment variable not set")
    client = get_async_mongodb_client()
    return client[db_name]


def get_async_mongodb_collection(col_name: str) -> AgnosticCollection:
    db = get_async_mongodb_database()
    print(db[col_name])
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
if __name__ == "__main__":
    import asyncio
    print("Starting script...")
    asyncio.run(start_async_mongodb())
    print("Script finished")
