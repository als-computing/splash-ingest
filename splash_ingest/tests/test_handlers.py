import pytest
import h5py
import numpy as np
from ..handlers import MultiKeyHDF5DatasetSliceHandler


@pytest.fixture
def sample_file(tmp_path):
    num_frames_primary = 3
    num_frames_darks = 1
    # data = np.empty((num_frames_primary, 5, 5))
    data = np.empty((num_frames_primary, 5, 5))
    data_dark = np.empty((num_frames_darks, 5, 5))
    for frame_num in range(0, num_frames_primary):
        data[frame_num] = np.random.random_sample((5, 5))
    for dark_num in range(0, num_frames_darks):
        data_dark[dark_num, :, :] = np.random.random_sample((5, 5))
    file = h5py.File(tmp_path / 'test.hdf5', 'w')
    file.create_dataset('/exchange/data', data=data)
    file.create_dataset('/exchange/dark', data=data_dark)
    file.close()
    file = h5py.File(tmp_path / 'test.hdf5', 'r')
    yield file
    print('closing file')
    file.close()


def test_multi_key_handler(sample_file):
    handler = MultiKeyHDF5DatasetSliceHandler(sample_file.filename, '/exchange/data', frame_per_point=1)
    data = handler(0, '/exchange/data')
    assert data.shape == (5, 5)
    data = handler(0, '/exchange/dark')
    assert data.shape == (5, 5)
