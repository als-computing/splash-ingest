import os
from splash_ingest.server.model import IngestType

import dotenv
import requests

from splash_ingest.server.api import CreateJobRequest

def create_job(url, api_key, file_path, mapping_name):
    request = CreateJobRequest(
        file_path=file_path,
        mapping_name=mapping_name, ingest_types=[IngestType.scicat_databroker])


if __name__ == "__main__":
    dotenv.load_dotenv()
    url = os.getenv("INGEST_API_URL")
    api_key = os.getenv("INGEST_API_KEY")
    if not api_key:
        print("API_KEY not provided, exiting")
    else:
        print(f"Talking to endpoints at {url}")
    create_job(url, api_key)