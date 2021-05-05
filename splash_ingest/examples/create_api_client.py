import os

from pymongo import MongoClient

from splash_ingest.server.api_auth_service import create_api_client, init_api_service
from splash_ingest.server.api import INGEST_JOBS_API

db = MongoClient(os.getenv("INGEST_DB_URI")).ingest
init_api_service(db)

key = create_api_client("submitter", "user", INGEST_JOBS_API)
print(f"create key {key}  for api {INGEST_JOBS_API}")
