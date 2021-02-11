import datetime
import os
from pathlib import Path
import h5py
import numpy as np
import pytest

from databroker.core import SingleRunCache
from splash_ingest.ingestors import (
    MappedHD5Ingestor,
    MappingNotFoundError,
    EmptyTimestampsError,
    calc_num_events,
    encode_key,
    decode_key
)
from splash_ingest.model import Mapping

num_frames_primary = 3
num_frames_darks = 1

mapping_dict = {
        "name": "test name",
        "description": "test descriptions",
        "version": "42",
        "resource_spec": "MultiKeySlice",
        "md_mappings": [
            {"field": "/measurement/sample/name"},
            {"field": "/measurement/instrument/name"},
            {"field": "/measurement/instrument/source/beamline"},
        ],
        "stream_mappings": {
            "primary": {
                "time_stamp": "/process/acquisition/time_stamp",
                "conf_mappings": [
                        {"device": "all",
                         "mapping_fields": [
                            {"field": "/measurement/instrument/detector/dark_field_value"},
                            {"field": "/measurement/instrument/attenuator/setup/filter_y"}
                        ]
                    }
                ],
                "mapping_fields":
                [
                    {"field": "/exchange/data", "external": True},
                    {"field": "/process/acquisition/sample_position_x", "description": "tile_xmovedist"}
                ]
            },
            "darks": {
                "time_stamp": "/process/acquisition/time_stamp",
                "mapping_fields": [
                    {"field": "/exchange/dark", "external": True},
                    {"field": "/process/acquisition/sample_position_x", "description": "tile_xmovedist"}
                ]
            },
        },
        "projections": [{
            "name": "foo_bar",
            "version": "2020.1",
            "configuration": None,
            "projection": {
                'sampel_name': {"type": "linked", "location": "start", "field": "sample"},
            }
        }]
    }


def test_build_mapping():
    mapping = Mapping(**mapping_dict)
    assert mapping.name == 'test name'
    assert mapping.stream_mappings['primary'].mapping_fields[0].field == '/exchange/data'


@pytest.fixture
def sample_file(tmp_path):
    # data = np.empty((num_frames_primary, 5, 5))
    data = np.empty((num_frames_primary, 5, 5))
    data_dark = np.empty((num_frames_darks, 5, 5))
    primary_timestamps = np.empty((num_frames_primary), dtype='float64')
    dark_timestamps = np.empty((num_frames_darks), dtype='float64')
    start_time = datetime.datetime.now()
    primary_sample_position_x = []
    for frame_num in range(0, num_frames_primary):
        data[frame_num] = np.random.random_sample((5, 5))
        primary_timestamps[frame_num] = (start_time + datetime.timedelta(0, frame_num)).timestamp()  # add a second for each frame
        primary_sample_position_x.append(float(frame_num))
    start_time = datetime.datetime.now()
    
    for dark_num in range(0, num_frames_darks):
        data_dark[dark_num, :, :] = np.random.random_sample((5, 5))
        dark_timestamps[dark_num] = (start_time + datetime.timedelta(0, num_frames_darks)).timestamp()  # add a second to each

    file = h5py.File(tmp_path / 'test.hdf5', 'w')
    file.create_dataset('/measurement/sample/name', data=b'my sample', dtype='|S256')
    file.create_dataset('/measurement/instrument/name', data=b'my station', dtype='|S256')
    file.create_dataset('/measurement/instrument/source/beamline', data=b'my beam', dtype='|S256')
    file.create_dataset('/exchange/data', data=data)
    file.create_dataset('/exchange/dark', data=data_dark)
    file.create_dataset('/process/acquisition/sample_position_x', data=primary_sample_position_x)
    file.create_dataset('/process/acquisition/time_stamp', data=primary_timestamps, dtype='float64')
    file.create_dataset('/process/acquisition/dark_time_stamp', data=dark_timestamps, dtype='float64')
    
    # stream configuration fields
    file.create_dataset('/measurement/instrument/detector/dark_field_value', data=dark_timestamps, dtype='float64')
    file.create_dataset('/measurement/instrument/attenuator/setup/filter_y', data=dark_timestamps, dtype='float64')

    file.close()
    file = h5py.File(tmp_path / 'test.hdf5', 'r')
    yield file
    print('closing file')
    file.close()


def test_hdf5_mapped_ingestor(sample_file, tmp_path):
    ingestor = MappedHD5Ingestor(Mapping(**mapping_dict), sample_file, "test_root", thumbs_root=tmp_path)
    run_cache = SingleRunCache()
    descriptors = []
    result_events = []
    result_datums = []
    start_found = False
    stop_found = False
    run_uid = ""
    for name, doc in ingestor.generate_docstream():
        run_cache.callback(name, doc)
        if name == "start":
            assert doc[":measurement:sample:name"] == "my sample", "metadata in start doc"
            assert doc["projections"][0]['name'] == "foo_bar", "projection is in start doc"
            start_found = True
            run_uid = doc['uid']
            continue
        if name == "descriptor":
            descriptors.append(doc)
            continue
        if name == "resource":
            doc["spec"] == mapping_dict["resource_spec"]
            continue
        if name == "datum":
            result_datums.append(doc)
            continue
        if name == "resource":
            result_events.append(doc)
            continue
        if name == "event":
            result_events.append(doc)
        if name == "stop":
            stop_found = True
            assert doc["num_events"]["primary"] == num_frames_primary
            # assert doc["num_events"]["darks"] == num_frames_darks
            continue

    assert start_found, "a start document was produced"
    assert stop_found, "a stop document was produced"

    assert len(descriptors) == 2, "return two descriptors"
    assert descriptors[0]["name"] == "primary", "first descriptor is primary"
    assert descriptors[1]["name"] == "darks", "second descriptor is darks"
    assert len(descriptors[0]["data_keys"].keys()) == 2, "primary has two data_keys"
    assert descriptors[0]['configuration'] is not None

    assert len(result_datums) == num_frames_primary + num_frames_darks
    assert len(result_events) == num_frames_primary + num_frames_darks

    run = run_cache.retrieve()
    stream = run["primary"].to_dask()
    assert stream
    dir = Path(tmp_path)
    file = run_uid + ".png"
    assert Path(dir / file).exists()


def test_mapped_ingestor_bad_stream_field(sample_file):
    mapping_dict_bad_stream_field = {
        "name": "test name",
        "description": "test descriptions",
        "version": "42",
        "resource_spec": "MultiKeySlice",
        "md_mappings": [
            {"field": "/measurement/sample/name"}
        ],
        "stream_mappings":
        {
            "primary": { 
                "time_stamp": "/does/not/exist",
                "mapping_fields": [
                    {"field": "raise_exception"}
                ]
            },
        }
    }
    ingestor = MappedHD5Ingestor(Mapping(**mapping_dict_bad_stream_field), sample_file, "test_root")
    list(ingestor.generate_docstream())
    assert "Error finding stream mapping" in ingestor.issues[0]


def test_mapped_ingestor_bad_metadata_field(sample_file):
    mapping_dict_bad_metadata_field = {
        "name": "test name",
        "description": "test descriptions",
        "version": "42",
        "resource_spec": "MultiKeySlice",
        "md_mappings": [
            {"field": "raise_exception"},
        ],
        "stream_mappings": {}
    }
    ingestor = MappedHD5Ingestor(Mapping(**mapping_dict_bad_metadata_field), sample_file, "test_root")
    list(ingestor.generate_docstream())
    assert "Error finding run_start mapping" in ingestor.issues[0]


def test_calc_num_events(sample_file):
    stream_mapping = Mapping(**mapping_dict).stream_mappings["primary"].mapping_fields
    num_events = calc_num_events(stream_mapping, sample_file)
    assert num_events == num_frames_primary, "primary stream has same number events as primary stream frames"
    assert calc_num_events({}, sample_file) == 0, "no fields returns none"


def test_timestamp_error(sample_file):
    mapping = {
        "name": "test name",
        "description": "test descriptions",
        "version": "42",
        "resource_spec": "MultiKeySlice",
        "md_mappings": [],
        "stream_mappings": {
            "do_not_cross": {
                "time_stamp": "/does/not/exist",
                "mapping_fields": [
                    {"field": "/exchange/data", "external": True},
                    {"field": "/process/acquisition/sample_position_x", "description": "tile_xmovedist"}
                ]
            }
        }
    }
    ingestor = MappedHD5Ingestor(Mapping(**mapping), sample_file, "test_root")

    list(ingestor.generate_docstream())
    assert "Error fetching timestamp" in ingestor.issues[0]

def test_key_transformation():
    key = "/don't panic"
    assert decode_key(encode_key(key)) == key, "Encoded then decoded key is equal"


def test_data_groups(sample_file):
    ingestor = MappedHD5Ingestor(Mapping(**mapping_dict), sample_file, "test_root")
    for name, doc in ingestor.generate_docstream():
        if name == 'start':
            assert doc['data_groups'] == []
            continue

    ingestor = MappedHD5Ingestor(Mapping(**mapping_dict), sample_file, "test_root", data_groups=['bealine1', 'users1'])
    for name, doc in ingestor.generate_docstream():
        if name == 'start':
            assert doc['data_groups'] == ['bealine1', 'users1']
            continue
