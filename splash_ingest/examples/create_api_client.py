from pymongo import MongoClient

from splash_ingest.server.api_auth_service import create_api_client, init_api_service
from splash_ingest.server.api import INGEST_JOBS_API

db = MongoClient("mongodb://db:27017").splash
init_api_service(db)

key = create_api_client("foo", "dylan", INGEST_JOBS_API)
print(f"create key {key}  for api {INGEST_JOBS_API}")
