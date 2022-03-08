import datetime
import h5py
import pytest
from mongomock import MongoClient
from splash_ingest.server.api_auth_service import (
    create_api_client,
    init_api_service as init_api_key,
)
from splash_ingest.server.model import IngestType
from ..ingest_service import (
    find_job,
    find_unstarted_jobs,
    init_ingest_service,
    service_context,
    create_job,
    set_job_status,
)
from ..model import JobStatus, StatusItem


@pytest.fixture(scope="session", autouse=True)
def init_mongomock():
    ingest_db = MongoClient().ingest_db
    init_ingest_service(ingest_db)
    init_api_key(ingest_db)
    create_api_client("user1", "sirius_cybernetics_gpp", "door_operation")


def test_jobs_init():
    assert (
        service_context.ingest_jobs is not None
    ), "test that init creates a collection"
    assert len(service_context.ingest_jobs.index_information()) == 4


def test_job_create():
    document_path = "/foo/bar.hdf5"

    job = create_job("user1", document_path, "magrathia_42", [IngestType.databroker])
    assert job.id is not None, "Job gets a new uid"
    assert job.submit_time is not None, "Job gets a submit time"
    assert job.submitter == "user1", "Job gets provided submitter"
    assert job.status == JobStatus.submitted, "Job gets provided submitter"

    return_job = find_job(job.id)
    assert return_job.submit_time is not None, "return Job gets a submit time"
    assert return_job.submitter == "user1", "return Job gets provided submitter"
    assert (
        return_job.status == JobStatus.submitted
    ), "return Job gets provided submitter"


def test_update_non_existant_job():
    result = set_job_status(
        "42",
        StatusItem(
            submitter="slartibartfast",
            time=datetime.datetime.utcnow(),
            status=JobStatus.running,
        ),
    )
    assert not result, "tested return code for non-existent job"


def test_query_unstarted_jobs():
    document_path = "/foo/bar.hdf5"

    job = create_job("user1", document_path, "magrathia", [IngestType.databroker])
    job = create_job("user1", document_path, "magrathia", [IngestType.databroker])

    jobs = find_unstarted_jobs()
    for job in jobs:
        assert job.status == JobStatus.submitted
        time = datetime.datetime.utcnow()
        set_job_status(
            job.id,
            StatusItem(
                time=time,
                submitter="slartibartfast",
                status=JobStatus.running,
                log="rebuild earth",
            ),
        )
        job = find_job(job.id)
        assert len(job.status_history) > 1
        assert (
            job.status_history[-1].submitter == "slartibartfast"
        ), "most recent status correct user"
        assert abs(job.status_history[-1].time - time) < datetime.timedelta(
            milliseconds=1
        ), "most recent status data within Mongo accuracy of milliseconds"
        assert (
            job.status_history[-1].status == JobStatus.running
        ), "most recent status correct user"
        assert (
            job.status_history[-1].log == "rebuild earth"
        ), "most recent status correct user"

    jobs = list(find_unstarted_jobs())
    assert len(jobs) == 0, "all jobs should be set to started"


@pytest.fixture
def sample_file(tmp_path):
    file = h5py.File(tmp_path / "test.hdf5", "w")
    file.create_dataset("/measurement/sample/name", data=b"my sample", dtype="|S256")
    file.close()
    file = h5py.File(tmp_path / "test.hdf5", "r")
    yield file
    print("closing file")
    file.close()
