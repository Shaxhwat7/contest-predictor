import time
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from core.config import get_yaml_config
from core.db.database import start_async_mongodb
from core.utils import init_loguru

from api.routers import questions_api, contests_data, contests_api
app = FastAPI()
config = get_yaml_config().get("fastapi")


app.include_router(questions_api.router, prefix="/api/v1")
app.include_router(contests_api.router, prefix="/api/v1")
app.include_router(contests_data.router, prefix="/api/v1")

@app.on_event("startup")
async def on_startup():
    init_loguru(process = "api")
    await start_async_mongodb()

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.get("CORS_allow_origins"),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.middleware("http")
async def log_requests(request:Request,call_next):
    start_time = time.time()
    response = await call_next(request)
    duration = (time.time() - start_time)*1000
    logger.info(
        f"[{request.client.host}] {request.method} {request.url.path} | "
        f"Duration: {duration:.2f}ms | Status: {response.status_code}"
    )
    return response