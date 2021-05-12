import h5py
from pathlib import Path
import pytest
# import requests
import requests_mock

from ..scicat import project_start_doc, ScicatIngestor, build_search_terms
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


def test_scicate_ingest(sample_file):
    with requests_mock.Mocker() as mock_request:
        mock_request.post("http://localhost:3000/api/v3/Users/login", json={"id": "foobar"})
        mock_request.post("http://localhost:3000/api/v3/Samples", json={"sampleId": "sample_id"})
        mock_request.post("http://localhost:3000/api/v3/RawDatasets/replaceOrCreate", json={"pid": "42"})
        mock_request.post("http://localhost:3000/api/v3/RawDatasets/42/origdatablocks", json={"response": "random"})
        issues: list[Issue] = []
        scicat = ScicatIngestor(issues, host="localhost:3000")
        scicat.ingest_run(Path(sample_file.filename), start_doc, descriptor_doc)
        assert len(issues) == 0


def test_build_search_terms():
    terms = build_search_terms({"sample_name": "Time-is_an illusion. Lunchtime/2x\\so."})
    assert "time" in terms
    assert "is" in terms
    assert "an" in terms
    assert "illusion" in terms
    assert "lunchtime" in terms
    assert "2x" in terms
    assert "so" in terms



start_doc = {
    "image_data": [1],
    "sample_name": "sample_name",
    "sample_id": "sample_id",
    "instrument_name": "instrument_name",
    "beamline": "beamline",
    "collection_date": [1619564232.0],
    "proposal": "proposal",
    "experiment_title": "experiment_title",
    "experimenter_name": "experimenter_name",
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