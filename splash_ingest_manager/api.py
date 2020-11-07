import logging
from typing import Optional

from fastapi import Security, Depends, FastAPI, HTTPException
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
from pydantic import BaseModel, Field
from pymongo import MongoClient
from starlette.config import Config
from starlette.status import HTTP_403_FORBIDDEN

from .auth_service import get_stored_api_key, init_api_service
from .ingest_service import (
    init_ingest_service,
    start_job_poller,
    create_job,
    find_job,
    find_unstarted_jobs,
    create_mapping,
    find_mapping
    )

from splash_ingest.model import Mapping
from splash_ingest_manager.model import Job

API_KEY_NAME = "access_token"


api_key_query = APIKeyQuery(name=API_KEY_NAME, auto_error=False)
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
api_key_cookie = APIKeyCookie(name=API_KEY_NAME, auto_error=False)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
# ch.setLevel(logging.INFO)
root_logger.addHandler(ch)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger = logging.getLogger('splash_ingest')
app = FastAPI(    
    openapi_url="/api/ingest/openapi.json",
    docs_url="/api/ingest/docs",
    redoc_url="/api/ingest/redoc",)
config = Config(".env")
MONGO_DB_URI = config("MONGO_DB_URI", cast=str, default="mongodb://localhost:27017/splash")
SPLASH_DB_NAME = config("SPLASH_DB_NAME", cast=str, default="splash")


@app.on_event("startup")
async def startup_event():
    logger.info('!!!!!!!!!starting server')
    db = MongoClient(MONGO_DB_URI)[SPLASH_DB_NAME]
    init_ingest_service(db)
    init_api_service(db)
    start_job_poller()


async def get_api_key_from_request(
    api_key_query: str = Security(api_key_query),
    api_key_header: str = Security(api_key_header),
    api_key_cookie: str = Security(api_key_cookie)
):

    if api_key_query:
        return api_key_query
    elif api_key_header:
        return api_key_header
    elif api_key_cookie:
        return api_key_cookie
    else:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials"
        )


INGEST_JOBS_API = 'ingest_jobs'


class CreateJobRequest(BaseModel):
    file_path: str = Field(description="path to where file to ingest is located")
    mapping_name: str = Field(description="mapping name, used to find mapping file in database")


class CreateJobResponse(BaseModel):
    message: str = Field(description="return message")
    job_id: Optional[str] = Field(description="uid of newly created job, if created")


@app.post("/api/ingest/jobs", tags=['ingest_jobs'])
async def submit_job(request: CreateJobRequest, api_key: APIKey = Depends(get_api_key_from_request)) -> CreateJobResponse:

    client_key = get_stored_api_key('user1', api_key)
    logger.info(f'request client key {repr(client_key)}')
    if client_key is None or client_key.api != INGEST_JOBS_API:
        logger.info('forbidden')
        raise HTTPException(status_code=403)
    job = create_job(
        client_key.client,
        request.file_path,
        request.mapping_name)
    return CreateJobResponse(message="success", job_id=job.id)
  

@app.get("/api/ingest/jobs/{job_id}", tags=['ingest_jobs'])
async def get_job(job_id: str, api_key: APIKey = Depends(get_api_key_from_request)) -> Job:
    try:
        client_key = get_stored_api_key('user1', api_key)
        if client_key is None or client_key.api != INGEST_JOBS_API:
            return HTTP_403_FORBIDDEN
        job = find_job(job_id)
        return job
    except Exception as e:
        logger.error(e)
        raise e


@app.get("/api/ingest/jobs", tags=['ingest_jobs'])
async def get_unstarted_jobs(api_key: APIKey = Depends(get_api_key_from_request)) -> Job:
    try:
        client_key = get_stored_api_key('user1', api_key)
        if client_key is None or client_key.api != INGEST_JOBS_API:
            return HTTP_403_FORBIDDEN
        jobs = find_unstarted_jobs()
        return jobs
    except Exception as e:
        logger.error(e)
        raise e


class CreateMappingResponse(BaseModel):
    mapping_id: str
    message: str


@app.post("/api/ingest/mappings", tags=['mappings'])
async def insert_mapping(mapping: Mapping, api_key: APIKey = Depends(get_api_key_from_request)) -> CreateMappingResponse:
    try:
        client_key = get_stored_api_key('user1', api_key)
        if client_key is None or client_key.api != INGEST_JOBS_API:
            return HTTP_403_FORBIDDEN
        mapping_id = create_mapping(client_key.client, mapping)
        return CreateMappingResponse(mapping_id=mapping_id, message="success")
    except Exception as e:
        logger.error(e)
        raise e


@app.get("/api/ingest/mappings/{mapping_id}", tags=['mappings'])
async def get_mapping(mapping_id: str, api_key: APIKey = Depends(get_api_key_from_request)) -> Mapping:
    try:
        client_key = get_stored_api_key('user1', api_key)
        if client_key is None or client_key.api != INGEST_JOBS_API:
            return HTTP_403_FORBIDDEN
        mapping = find_mapping(client_key.client, mapping_id)
        return mapping
    except Exception as e:
        logger.error(e)
        raise e
