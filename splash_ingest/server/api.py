import logging
from typing import Optional, List

from fastapi import Security, Depends, FastAPI, HTTPException
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
from pydantic import BaseModel, Field
from pymongo import MongoClient
from starlette.config import Config
from starlette.status import HTTP_403_FORBIDDEN

from .api_auth_service import init_api_service, verify_api_key
from .ingest_service import (
    init_ingest_service,
    create_job,
    find_job,
    find_unstarted_jobs,
    create_mapping,
    find_mapping,
    JobNotFoundError
    )

from splash_ingest.model import Mapping
from .model import Job, IngestType

API_KEY_NAME = "api_key"
INGEST_JOBS_API = 'ingest_jobs'

api_key_query = APIKeyQuery(name=API_KEY_NAME, auto_error=False)
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
api_key_cookie = APIKeyCookie(name=API_KEY_NAME, auto_error=False)

config = Config(".env")
DATABROKER_DB_URI = config("DATABROKER_DB_URI", cast=str, default="mongodb://localhost:27017/databroker")
DATABROKER_DB_NAME = config("DATABROKER_DB_NAME", cast=str, default="databroker")
INGEST_DB_URI = config("INGEST_DB_URI", cast=str, default="mongodb://localhost:27017/ingest")
INGEST_DB_NAME = config("INGEST_DB_NAME", cast=str, default="ingest")
INGEST_LOG_LEVEL = config("INGEST_LOG_LEVEL", cast=str, default="INFO")

logger = logging.getLogger('splash_ingest.api_auth')


def init_logging():

    ch = logging.StreamHandler()
    # ch.setLevel(logging.INFO)
    # root_logger.addHandler(ch)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(INGEST_LOG_LEVEL)


init_logging()

app = FastAPI(    
    openapi_url="/api/ingest/openapi.json",
    docs_url="/api/ingest/docs",
    redoc_url="/api/ingest/redoc",)


@app.on_event("startup")
async def startup_event():
    logger.info('starting api server')
    logger.info(f"DATABROKER_DB_URI {DATABROKER_DB_URI}")
    logger.info(f"DATABROKER_DB_NAME {DATABROKER_DB_NAME}") 
    logger.info(f"INGEST_DB_URI {INGEST_DB_URI}")
    logger.info(f"INGEST_DB_NAME {INGEST_DB_NAME}")
    databroker_db = MongoClient(DATABROKER_DB_URI)[DATABROKER_DB_NAME]
    ingest_db = MongoClient(INGEST_DB_URI)[INGEST_DB_NAME]
    init_ingest_service(ingest_db, databroker_db)
    init_api_service(ingest_db)
    # start_job_poller()


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


class CreateJobRequest(BaseModel):
    file_path: str = Field(description="path to where file to ingest is located")
    mapping_name: str = Field(description="mapping name, used to find mapping file in database")
    ingest_types: List[IngestType] = Field(description="Type of ingestions to be done")


class CreateJobResponse(BaseModel):
    message: str = Field(description="return message")
    job_id: Optional[str] = Field(description="uid of newly created job, if created")


@app.post("/api/ingest/jobs", tags=['ingest_jobs'], response_description="Returns information about the status of creating the Job")
async def submit_job(request: CreateJobRequest, api_key: APIKey = Depends(get_api_key_from_request)) \
         -> CreateJobResponse:
    client_key: APIKey = verify_api_key(api_key)
    if not client_key:
        logger.info('forbidden  {api_key}')
        raise HTTPException(status_code=403)

    job = create_job(
        client_key.client,
        request.file_path,
        request.mapping_name,
        request.ingest_types)
    return CreateJobResponse(message="success", job_id=job.id)
  

@app.get("/api/ingest/jobs/{job_id}", tags=['ingest_jobs'], response_model=Job, response_description="Returns the requested Job")
async def get_job(job_id: str, api_key: APIKey = Depends(get_api_key_from_request)) -> Job:
    try:
        client_key: APIKey = verify_api_key(api_key)
        if not client_key:
            logger.info('forbidden  {api_key}')
            raise HTTPException(status_code=403)
        job = find_job(job_id)
        return job
    except JobNotFoundError:
        raise HTTPException(404)
    except Exception as e:
        logger.error(e)
        raise e


@app.get("/api/ingest/jobs", tags=['ingest_jobs'], response_model=List[Job], response_description="Returns all Jobs waiting to be started")
async def get_unstarted_jobs(api_key: APIKey = Depends(get_api_key_from_request)) -> List[Job]:
    try:
        client_key: APIKey = verify_api_key(api_key)
        if not client_key:
            logger.info('forbidden  {api_key}')
            raise HTTPException(status_code=403)
        jobs = find_unstarted_jobs()
        return jobs
    except Exception as e:
        logger.error(e)
        raise e


class CreateMappingResponse(BaseModel):
    mapping_id: str
    message: str


@app.post("/api/ingest/mappings", tags=['mappings'],response_model=CreateMappingResponse, response_description="Information about the creation of the Mapping")
async def insert_mapping(mapping: Mapping, 
                         api_key: APIKey = Depends(get_api_key_from_request)) -> CreateMappingResponse:
    try:
        client_key: APIKey = verify_api_key(api_key)
        if not client_key:
            logger.info('forbidden  {api_key}')
            raise HTTPException(status_code=403)
        mapping_id = create_mapping(client_key.client, mapping)
        return CreateMappingResponse(mapping_id=mapping_id, message="success")
    except Exception as e:
        logger.error(e)
        raise e


@app.get("/api/ingest/mappings/{mapping_id}", tags=['mappings'], response_model=Mapping, response_description="Return a Mapping")
async def get_mapping(mapping_id: str, api_key: APIKey = Depends(get_api_key_from_request)) -> Mapping:
    try:
        client_key: APIKey = verify_api_key(api_key)
        if not client_key:
            logger.info('forbidden  {api_key}')
            raise HTTPException(status_code=403)
        mapping = find_mapping(client_key.client, mapping_id)
        return mapping
    except Exception as e:
        logger.error(e)
        raise e
 