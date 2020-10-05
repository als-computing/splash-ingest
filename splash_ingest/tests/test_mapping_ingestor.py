import datetime
from unittest import result
import h5py
import numpy as np
import pytest
import pytz

from databroker.core import SingleRunCache
from splash_ingest import (
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
        'name': 'test name',
        'description': 'test descriptions',
        'version': '42',
        'resource_spec': 'MultiKeySlice',
        'metadata_mappings': {
            '/measurement/sample/name': 'dataset',
            '/measurement/instrument/name': 'end_station',
            '/measurement/instrument/source/beamline': 'beamline',
        },
        'stream_mappings': {
            "primary": {
                "time_stamp": '/process/acquisition/time_stamp',
                "mapping_fields": [
                    {'name': '/exchange/data', 'external': True},
                    {'name': '/process/acquisition/sample_position_x', 'description': 'tile_xmovedist'}
                ]
            },
            "darks": {
                "time_stamp": '/process/acquisition/time_stamp',
                "mapping_fields": [
                    {'name': '/exchange/dark', 'external': True},
                    {'name': '/process/acquisition/sample_position_x', 'description': 'tile_xmovedist'}
                ]
            }
        }
    }


def test_build_mapping():
    mapping = Mapping(**mapping_dict)
    assert mapping.name == 'test name'
    assert mapping.stream_mappings['primary'].mapping_fields[0].name == '/exchange/data'


@pytest.fixture
def sample_file(tmp_path):
    string_dt = h5py.string_dtype(encoding='ascii')
    local = pytz.timezone("America/Los_Angeles")
    # data = np.empty((num_frames_primary, 5, 5))
    data = np.empty((3, 5, 5))
    data_dark = np.empty((num_frames_darks, 5, 5))
    pimary_timestamps = np.empty((num_frames_primary), dtype=object)
    start_time = datetime.datetime.now()
    primary_sample_position_x = []
    for frame_num in range(0, num_frames_primary):
        data[frame_num] = np.random.random_sample((5, 5))
        timestamp = start_time + datetime.timedelta(0, 1)  # add a second to each
        pimary_timestamps[frame_num] = (str(local.localize(timestamp, is_dst=None)))
        primary_sample_position_x.append(float(frame_num))
    start_time = datetime.datetime.now()
    dark_timestamps = np.empty((num_frames_primary), dtype=object)
    for dark_num in range(0, num_frames_darks):
        data_dark[dark_num, :, :] = np.random.random_sample((5, 5))
        timestamp = start_time + datetime.timedelta(0, 1)  # add a second to each
        dark_timestamps[dark_num] = (str(local.localize(timestamp, is_dst=None)))

    file = h5py.File(tmp_path / 'test.hdf5', 'w')
    file.create_dataset('/measurement/sample/name', data='my sample')
    file.create_dataset('/measurement/instrument/name', data='my station')
    file.create_dataset('/measurement/instrument/source/beamline', data='my beam')
    file.create_dataset('/exchange/data', data=data)
    file.create_dataset('/exchange/dark', data=data_dark)
    file.create_dataset('/process/acquisition/sample_position_x', data=primary_sample_position_x)
    file.create_dataset('/process/acquisition/time_stamp', data=pimary_timestamps, dtype=string_dt)
    file.create_dataset('/process/acquisition/dark_time_stamp', data=dark_timestamps, dtype=string_dt)
    file.close()
    file = h5py.File(tmp_path / 'test.hdf5', 'r')
    yield file
    print('closing file')
    file.close()


def test_hdf5_mapped_ingestor(sample_file):
    ingestor = MappedHD5Ingestor(Mapping(**mapping_dict), sample_file, 'test_root')
    run_cache = SingleRunCache()
    descriptors = []
    result_events = []
    result_datums = []
    start_found = False
    stop_found = False
    for name, doc in ingestor.generate_docstream():
        run_cache.callback(name, doc)
        if name == 'start':
            assert doc[':measurement:sample:name'] == 'my sample', 'metadata in start doc'
            start_found = True
            continue
        if name == 'descriptor':
            descriptors.append(doc)
            continue
        if name == 'resource':
            doc['spec'] == mapping_dict['resource_spec']
            continue
        if name == 'datum':
            result_datums.append(doc)
            continue
        if name == 'resource':
            result_events.append(doc)
            continue
        if name == 'event':
            result_events.append(doc)
        if name == 'stop':
            stop_found = True
            assert doc['num_events']['primary'] == num_frames_primary
            # assert doc['num_events']['darks'] == num_frames_darks
            continue

    assert start_found, 'a start document was produced'
    assert stop_found, 'a stop document was produced'

    assert len(descriptors) == 2, 'return two descriptors'
    assert descriptors[0]['name'] == 'primary', 'first descriptor is primary'
    assert descriptors[1]['name'] == 'darks', 'second descriptor is darks'
    assert len(descriptors[0]['data_keys'].keys()) == 2, 'primary has two data_keys'

    assert len(result_datums) == num_frames_primary + num_frames_darks
    assert len(result_events) == num_frames_primary + num_frames_darks

    run = run_cache.retrieve()
    stream = run['primary'].to_dask()
    assert stream


def test_mapped_ingestor_bad_stream_field(sample_file):
    mapping_dict_bad_stream_field = {
        'name': 'test name',
        'description': 'test descriptions',
        'version': '42',
        'resource_spec': 'MultiKeySlice',
        'metadata_mappings': {
            '/measurement/sample/name': 'dataset',
        },
        'stream_mappings':
        {
            "primary": { 
                "time_stamp": '/does/not/exist',
                "mapping_fields": [
                    {"name": "raise_exception"}
                ]
            },
        }
    }
    ingestor = MappedHD5Ingestor(Mapping(**mapping_dict_bad_stream_field), sample_file, 'test_root')
    with pytest.raises(MappingNotFoundError):
        list(ingestor.generate_docstream())


def test_mapped_ingestor_bad_metadata_field(sample_file):
    mapping_dict_bad_metadata_field = {
        'name': 'test name',
        'description': 'test descriptions',
        'version': '42',
        'resource_spec': 'MultiKeySlice',
        'metadata_mappings': {
            'raise_exception': 'dataset',
        },
        'stream_mappings': {}
    }
    ingestor = MappedHD5Ingestor(Mapping(**mapping_dict_bad_metadata_field), sample_file, 'test_root')
    with pytest.raises(MappingNotFoundError):
        for name, in ingestor.generate_docstream():
            pass


def test_calc_num_events(sample_file):
    stream_mapping = Mapping(**mapping_dict).stream_mappings['primary'].mapping_fields
    num_events = calc_num_events(stream_mapping, sample_file)
    assert num_events == num_frames_primary, 'primary stream has same number events as primary stream frames'
    assert calc_num_events({}, sample_file) == 0, 'no fields returns none'


def test_timestamp_error(sample_file):
    mapping = {
        'name': 'test name',
        'description': 'test descriptions',
        'version': '42',
        'resource_spec': 'MultiKeySlice',
        'metadata_mappings': {},
        'stream_mappings': {
            "do_not_cross": {
                "time_stamp": '/does/not/exist',
                "mapping_fields": [
                    {'name': '/exchange/data', 'external': True},
                    {'name': '/process/acquisition/sample_position_x', 'description': 'tile_xmovedist'}
                ]
            }
        }
    }
    ingestor = MappedHD5Ingestor(Mapping(**mapping), sample_file, 'test_root')

    with pytest.raises(EmptyTimestampsError) as ex_info:
        list(ingestor.generate_docstream())
    assert ex_info.value.args[0] == 'do_not_cross'
    assert ex_info.value.args[1] == '/does/not/exist'


def test_key_transformation():
    key = "/don't/panic"
    assert decode_key(encode_key(key)) == key, 'Encoded then decoded key is equal'