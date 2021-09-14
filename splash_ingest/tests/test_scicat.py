
from pathlib import Path
from typing import List

import h5py
import pytest
import requests_mock

from ..scicat import (
    project_start_doc,
    ScicatIngestor,
    build_search_terms,
    calculate_access_controls)

from ..model import Issue

@pytest.fixture
def sample_file(tmp_path):
    # data = np.empty((num_frames_primary, 5, 5))
    file = h5py.File(tmp_path / 'test.hdf5', 'w')
    file.create_dataset('/measurement/sample/name', data=b'my sample', dtype='|S256')
    yield file
    print('closing file')
    file.close()


def test_projected_start():

    # no projections
    start = {
        "a": "1",
        "b": 2
    }
    with pytest.raises(Exception):
        project_start_doc(start, "test")

    # empty projections
    start = {
        "a": "1",
        "b": 2,
        "projections": []
    }
    with pytest.raises(Exception):
        project_start_doc(start, "test")

    # no projections for filter
    start = {
        "a": "1",
        "b": 2,
        "projections": [{
            "configuration": {
                 "intent": "test"
            }
        }]
    }
    with pytest.raises(Exception):
        project_start_doc(start, "test")

    # multiple projections for filter
    start = {
        "a": "1",
        "b": 2,
        "projections": [{
            "configuration": {
                 "intent": "test"
            }
            },
            {
                "configuration": {
                    "intent": "test"
                }
            }]
    }

    with pytest.raises(Exception):
        project_start_doc(start, "test")

    # find a projection
    projected = project_start_doc(start_doc, "app")
    assert projected['sample_name'] == "sample_name"
    assert projected['collection_date'] == [1619564232.0]



def add_mock_requests(mock_request):
    mock_request.post("http://localhost:3000/api/v3/Users/login", json={"id": "foobar"})
    mock_request.post("http://localhost:3000/api/v3/Samples", json={"sampleId": "dataset_id"})
    mock_request.post("http://localhost:3000/api/v3/RawDatasets/replaceOrCreate", json={"pid": "42"})
    mock_request.post("http://localhost:3000/api/v3/RawDatasets/42/origdatablocks", json={"response": "random"})

def test_scicate_ingest(sample_file):
    with requests_mock.Mocker() as mock_request:
        add_mock_requests(mock_request)
        issues: List[Issue] = []
        scicat = ScicatIngestor("dataset_id", issues, host="localhost:3000")
        scicat.ingest_run(Path(sample_file.filename), start_doc, descriptor_doc)
        assert len(issues) == 0


# def test_scicat_ingest_posts(sample_file):
#     from requests.models import Response
#     def mock_post(url, json={}, **kwargs):
        
#         def token():
#             return {"id": "foobar"}
        
#         def sample():
#             return {"sampleId": "dataset_id"}

#         def data_set():
#             return {"pid": "42"}

#         def orig_datablock():
#             return {"response": "random"}

#         response = Response()
#         response.status_code = 200
#         if url == "http://localhost:3000/api/v3/Users/login":
#             response.json = token
#         elif url == "http://localhost:3000/api/v3/Samples":
#             response.json = sample
#         elif url == "http://localhost:3000/api/v3/RawDatasets/replaceOrCreate":
#             response.json = data_set
#         elif url == "http://localhost:3000/api/v3/RawDatasets/42/origdatablocks":
#             response.json = orig_datablock
#         return response


#     issues: List[Issue] = []
#     scicat = ScicatIngestor("dataset_id", issues, host="localhost:3000")
#     import requests
#     requests.post = mock_post
#     scicat.ingest_run(Path(sample_file.filename), start_doc, descriptor_doc)
#     assert len(issues) == 0
    

def test_build_search_terms():
    terms = build_search_terms({"sample_name": "Time-is_an illusion. Lunchtime/2x\\so."})
    assert "time" in terms
    assert "is" in terms
    assert "an" in terms
    assert "illusion" in terms
    assert "lunchtime" in terms
    assert "2x" in terms
    assert "so" in terms


def test_get_field():
    with requests_mock.Mocker() as mock_request:
        add_mock_requests(mock_request)
        issues: List[Issue] = []
        scicat = ScicatIngestor("dataset_id", issues, host="localhost:3000")
        projected_doc = {
            "foo": "bar"
        }
        assert scicat._get_field("foo", projected_doc, "nothing") == "bar"
        assert scicat._get_field("foo2", projected_doc, "nothing") == "nothing"
        assert len(scicat.issues) == 1
        assert "missing field" in scicat.issues[0].msg


def test_extract_scientific_metadata():
    descriptor = {
        "configuration": {
            "all": {
                "data": {
                    "/a/b/1": "one",
                    "/a/b/2": "five",
                    "/a/d/1": 3
                }
            }
        }
    }
    event_sample ={
        "/a/c/1": [2]
    }
    sci_meta = ScicatIngestor._extract_scientific_metadata(descriptor, event_sample)
    keys = sci_meta.keys()
    assert list(keys) == sorted(keys)

    

def test_access_controls():
    # no propsal, no beamline
    projected_start_doc = {}
    username = "slartibartfast"
    access_controls = calculate_access_controls(username, projected_start_doc)
    assert access_controls["owner_group"] == "slartibartfast"
    assert access_controls["access_groups"] == []

    # propoosal and no beamline
    projected_start_doc = {"proposal": "42"}
    access_controls = calculate_access_controls(username, projected_start_doc)
    assert access_controls["owner_group"] == "42"

    # no propoosal and beamline
    projected_start_doc = {"beamline": "10.3.1"}
    access_controls = calculate_access_controls(username, projected_start_doc)
    assert access_controls["owner_group"] == "slartibartfast"
    assert access_controls["access_groups"] == ["10.3.1"]

    # proposal and beamline
    projected_start_doc = {"beamline": "10.3.1", "proposal": "42"}
    access_controls = calculate_access_controls(username, projected_start_doc)
    assert access_controls["owner_group"] == "42"
    assert access_controls["access_groups"] == ["10.3.1"]

    # special 8.3.2 mapping
    projected_start_doc = {"beamline": "bl832", "proposal": "42"}
    access_controls = calculate_access_controls(username, projected_start_doc)
    assert access_controls["owner_group"] == "42"
    assert "8.3.2" in access_controls["access_groups"]
    assert "bl832" in access_controls["access_groups"]

start_doc = {
    "uid": "dataset_id",
    "image_data": [1],
    "sample_name": "sample_name",
    "sample_id": "sample_id",
    "instrument_name": "instrument_name",
    "beamline": "beamline",
    "collection_date": [1619564232.0],
    "proposal": "proposal",
    "experiment_title": "experiment_title",
    "experimenter_name": "experimenter_name",
    "experimenter_email": "experimenter_email",
    "pi_name": "pi_name",
    "projections": [
    {
        "name": "dx_app",
        "configuration": {
            "intent": "app"
        },
        "projection": {
            "image_data": {
                "type": "linked",
                "location": "event",
                "stream": "primary",
                "field": "image_data"
            },
            "sample_name": {
                "type": "configuration",
                "field": "sample_name",
                "location": "start"
            },
            "sample_id": {
                "type": "configuration",
                "field": "sample_id",
                "location": "start"
            },
            "instrument_name": {
                "type": "configuration",
                "field": "instrument_name",
                "location": "start"
            },
            "beamline": {
                "type": "configuration",
                "field": "beamline",
                "location": "start"
            },
            "collection_date": {
                "type": "configuration",
                "field": "collection_date",
                "location": "start"
            },
            "proposal": {
                "type": "configuration",
                "field": "proposal",
                "location": "start"
            },
            "experiment_title": {
                "type": "configuration",
                "field": "experiment_title",
                "location": "start"
            },
            "experimenter_name": {
                "type": "configuration",
                "field": "experimenter_name",
                "location": "start"
            },
             "experimenter_email": {
                "type": "configuration",
                "field": "experimenter_email",
                "location": "start"
            },
            "pi_name": {
                "type": "configuration",
                "field": "pi_name",
                "location": "start"
            }
            
        }
    }
]
}

descriptor_doc = {
    "configuration": {
        "all": {
            "data":{
                "foo": "bar"
            }
        }
    }
}