import dask

from area_detector_handlers.handlers import HDF5DatasetSliceHandlerPureNumpy


class MultiKeyHDF5DatasetSliceHandler(HDF5DatasetSliceHandlerPureNumpy):
    return_type = {'delayed': True}
    _datasets = {}

    def __call__(self, point_number, key):
        key = key.replace(":", "/")
        # Don't read out the dataset until it is requested for the first time.
        if not self._datasets.get(key):
            self._datasets[key] = self._file[key]
        
        start = point_number * self._fpp
        stop = (point_number + 1) * self._fpp
        return dask.array.from_array(self._datasets[key])[start:stop].squeeze(0)