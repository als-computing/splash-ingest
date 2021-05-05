from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
from mongomock import MongoClient
import pytest

from splash_ingest.model import Mapping
from splash_ingest.server.api import app, CreateJobRequest, CreateJobResponse, CreateMappingResponse
from ..api_auth_service import create_api_client, init_api_service
from ..ingest_service import init_ingest_service
from ..model import IngestType, Job
from ..api import INGEST_JOBS_API, API_KEY_NAME




@pytest.fixture()
def client():
    client = TestClient(app)
    databroker_db = MongoClient().databroker_db
    ingest_db = MongoClient().ingest_db
    init_api_service(ingest_db)
    init_ingest_service(ingest_db, databroker_db)
    return client

def test_create_job_api(client: TestClient):
    key = create_api_client('user1', 'sirius_cybernetics_gpp', INGEST_JOBS_API)
    request = CreateJobRequest(file_path="/foo/bar.hdf5", mapping_name="beamline_mappings",
                               mapping_version="42", ingest_types=[IngestType.databroker, IngestType.scicat_databroker])
    response: CreateJobResponse = client.post(url="/api/ingest/jobs", data=request.json(), headers={API_KEY_NAME: key})
    assert response.status_code == 200
    job_id = response.json()['job_id']

    response = client.get(url="/api/ingest/jobs/" + job_id)
    assert response.status_code == 403, 'ingest jobs wihtout api key'

    response = client.get(url="/api/ingest/jobs/" + job_id + "?" + API_KEY_NAME + "=" + key)
    job = Job(**response.json())
    assert job.document_path == "/foo/bar.hdf5"


def test_mapping_api(client: TestClient):
    key = create_api_client('user1', 'sirius_cybernetics_gpp', INGEST_JOBS_API)
    request = Mapping(name="foo", description="bar", resource_spec="blah")
    response: CreateMappingResponse = client.post(url="/api/ingest/mappings",
                                                  data=request.json(),
                                                  headers={API_KEY_NAME: key})
    assert response.status_code == 200
    response = client.get(url="/api/ingest/mappings/" + "foo",
                          headers={API_KEY_NAME: key})
    mapping = Mapping(**response.json())   # first item because we have a version tag in there
    assert mapping.name == "foo"


def test_job_not_found(client: TestClient):
    key = create_api_client('user1', 'sirius_cybernetics_gpp', INGEST_JOBS_API)
    response = client.get(url="/api/ingest/jobs/BAD_ID" + "?" + API_KEY_NAME + "=" + key)
    assert response.status_code == 404, "404 with unknown job id"