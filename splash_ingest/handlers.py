import dask

from area_detector_handlers.handlers import HDF5DatasetSliceHandlerPureNumpy


class MultiKeyHDF5DatasetSliceHandler(HDF5DatasetSliceHandlerPureNumpy):
    return_type = {"delayed": True}
    _datasets = {}

    def __init__(self, filename, frame_per_point=1):
        super().__init__(filename, "foo", frame_per_point)

    def __call__(self, point_number, key):
        key = key.replace(":", "/")
        # Don't read out the dataset until it is requested for the first time.
        if not self._datasets.get(key):
            self._datasets[key] = self._file[key]

        start = point_number * self._fpp
        stop = (point_number + 1) * self._fpp
        # print(str(point_number))
        return dask.array.from_array(self._datasets[key])[start:stop][0, :, :]
        # return self._datasets[key][start:stop][0, :, :]
