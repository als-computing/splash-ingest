import datetime
import h5py
from pluggy.hooks import normalize_hookimpl_opts
import pytest
import pytz
from mongomock import MongoClient
import numpy as np
from splash_ingest_manager.auth_service import create_api_key, init_api_service as init_api_key
from splash_ingest.model import Mapping
from ..ingest_service import (
    bluesky_context,
    find_job,
    find_unstarted_jobs,
    init_ingest_service,
    service_context,
    create_job,
    set_job_status,
    create_mapping,
    find_mapping,
    ingest
)
from ..model import JobStatus, StatusItem


@pytest.fixture(scope="session", autouse=True)
def init_mongomock():
    db = MongoClient().ingest_db
    init_ingest_service(db)
    init_api_key(db)
    create_api_key('user1', 'sirius_cybernetics_gpp', 'door_operation')



def test_jobs_init():
    assert service_context.ingest_jobs is not None, "test that init creates a collection"
    assert len(service_context.ingest_jobs.index_information()) == 4

    assert service_context.ingest_mappings is not None, "test that init creates a collection"
    assert len(service_context.ingest_mappings.index_information()) == 3


def test_job_create():
    document_path = "/foo/bar.hdf5"

    job = create_job("user1", document_path, "magrathia_42")
    assert job.id is not None, "Job gets a new uid"
    assert job.submit_time is not None, "Job gets a submit time"
    assert job.submitter == "user1", "Job gets provided submitter"
    assert job.status == JobStatus.submitted, "Job gets provided submitter"

    return_job = find_job(job.id)
    assert return_job.submit_time is not None, "return Job gets a submit time"
    assert return_job.submitter == "user1", "return Job gets provided submitter"
    assert return_job.status == JobStatus.submitted, "return Job gets provided submitter"


def test_update_non_existant_job():
    result = set_job_status("42",
                            StatusItem(
                                submitter="slartibartfast",
                                time=datetime.datetime.utcnow(),
                                status=JobStatus.running))
    assert not result, "tested return code for non-existent job"


def test_query_unstarted_jobs():
    document_path = "/foo/bar.hdf5"

    job = create_job("user1", document_path, "magrathia")
    job = create_job("user1", document_path, "magrathia")

    jobs = find_unstarted_jobs()
    for job in jobs:
        assert job.status == JobStatus.submitted
        time = datetime.datetime.utcnow()
        set_job_status(job.id,
                       StatusItem(
                           time=time,
                           submitter="slartibartfast",
                           status=JobStatus.running,
                           log="rebuild earth"))
        job = find_job(job.id)
        assert len(job.status_history) > 1
        assert job.status_history[-1].submitter == "slartibartfast", "most recent status correct user"
        assert (abs(job.status_history[-1].time - time) < datetime.timedelta(milliseconds=1)), \
            "most recent status data within Mongo accuracy of milliseconds"
        assert job.status_history[-1].status == JobStatus.running, "most recent status correct user"
        assert job.status_history[-1].log == "rebuild earth", "most recent status correct user"

    jobs = list(find_unstarted_jobs())
    assert len(jobs) == 0, "all jobs should be set to started"

    


@pytest.fixture
def sample_file(tmp_path):
    num_frames_primary = 2
    string_dt = h5py.string_dtype(encoding='ascii')
    local = pytz.timezone("America/Los_Angeles")
    # data = np.empty((num_frames_primary, 5, 5))
    data = np.empty((num_frames_primary, 5, 5))
    pimary_timestamps = np.empty((num_frames_primary), dtype='S256')
    start_time = datetime.datetime.now()
    primary_sample_position_x = []
    for frame_num in range(0, num_frames_primary):
        data[frame_num] = np.random.random_sample((5, 5))
        timestamp = start_time + datetime.timedelta(0, 1)  # add a second to each
        pimary_timestamps[frame_num] = str(local.localize(timestamp, is_dst=None))
        primary_sample_position_x.append(float(frame_num))
    start_time = datetime.datetime.now()

    file = h5py.File(tmp_path / 'test.hdf5', 'w')
    file.create_dataset('/measurement/sample/name', data=np.array([b'my sample'], dtype='|S256'))
    file.create_dataset('/measurement/instrument/name', data=np.array([b'my station'], dtype='|S256'))
    file.create_dataset('/measurement/instrument/source/beamline', data=np.array([b'my beam'], dtype='|S256'))
    file.create_dataset('/exchange/data', data=data)
    file.create_dataset('/process/acquisition/sample_position_x', data=primary_sample_position_x)
    file.create_dataset('/process/acquisition/time_stamp', data=np.array(pimary_timestamps), dtype=string_dt)
    file.close()
    file = h5py.File(tmp_path / 'test.hdf5', 'r')
    yield file
    print('closing file')
    file.close()


def test_ingest(sample_file, init_mongomock):
    mapping = Mapping(**mapping_dict)
    create_mapping("slartibartfast", mapping)
    mapping, revision = find_mapping("slartibartfast", "magrathia")
    assert mapping.resource_spec == "MultiKeySlice", "test a field"
    job = create_job("user1", sample_file.filename, "magrathia")
    start_uid = ingest("slartibartfast", job)
    job = find_job(job.id)
    assert job is not None
    assert job.status == JobStatus.successful

    assert bluesky_context.db['run_start'].find_one({"uid": start_uid}) is not None, "job wrote start doc"
    

mapping_dict = {
        "name": "magrathia",
        "description": "test descriptions",
        "version": "42",
        "resource_spec": "MultiKeySlice",
        "md_mappings": [
            {"field": "/measurement/sample/name"}
        ],
        "stream_mappings": {
            "primary": {
                "time_stamp": "/process/acquisition/time_stamp",
                "mapping_fields": [
                    {"field": "/exchange/data", "external": True},
                    {"field": "/process/acquisition/sample_position_x", "description": "tile_xmovedist"}
                ]
            }
        }
    }