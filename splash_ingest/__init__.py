import logging
from typing import List
import event_model
from .model import (
    Mapping,
    StreamMapping,
    StreamMappingField
)

logger = logging.getLogger(__name__)


class MappingNotFoundError(Exception):
    def __init__(self, location, missing_field):
        self.missing_field = missing_field
        self.location = location
        super().__init__(f"Cannot find mapping file: {location} - {missing_field}")


class EmptyTimestampsError(Exception):
    def __ini__(self, stream, field):
        self.stream = stream
        self.field = field
        super().__init__(f"Cannot find mapping timestamps for stream {stream} using timestamp mapping: {field}")


class MappedHD5Ingestor():
    """Provides an ingestor (make of event_model docstreams) based on a single hdf5 file
    and mapping document.

    Creates a reference document mapping to provided file.

    This intended to be used and sub-classed for more complicated scenariors.


    """
    def __init__(self, mapping: Mapping, file, reference_root_name, projections=None):
        """

        Parameters
        ----------
        mapping : Mapping
            mapping document to interrogate to find events and metadata
        file : h5py File instance
            The file to read for data
        reference_root_name : str
            placed into the root field of the generated resource document...when
            a docstream is used by databroker, this will map the the root_map field
            in intake configuration. 
        projections : dict, optional
            projection to insert into the run_start document, by default None
        """
        super().__init__()
        self._mapping = mapping
        self._file = file
        self._reference_root_name = reference_root_name
        self._projections = projections

    def generate_docstream(self):
        """Generates docstream documents
        Several things to note about what documnets are produced:
    
        - run_stop : one run stop document will be produced. Fields will be added
        at the root level that correspond to the metadata_mappings section of the Mappings.
        Additionally, if projections are provided in the init, they will be added at the root.

        - descriptor: one descriptor will be produced for every stream in the stream_mappings of the provided Mappings

        - reference: one reference will be produced pointing to the hdf5 file

        - datum: one datum will be be produced for every timestep of every stream field that is greater than 1D
            - for 1D data (a single value at each time step), data is returned in events directly
            - all data with shapes greater than 1D will be returned at datum

        Yields
        -------
        name: str, doc: dict
            name of the document (run_start, reference, event, etc.) and the document itself
        """
        metadata = self._extract_metadata()
        run_bundle = event_model.compose_run(metadata=metadata)
        start_doc = run_bundle.start_doc
        start_doc['projections'] = self._projections
        yield 'start', start_doc

        hd5_resource = run_bundle.compose_resource(
            spec=self._mapping.resource_spec,
            root=self._reference_root_name,
            resource_path=self._file.filename,  # need to calculate a relative path
            resource_kwargs={
                "key": "/exchange/data"
            })
        yield 'resource', hd5_resource.resource_doc

        # produce documents for each stream
        stream_mappings: StreamMapping = self._mapping.stream_mappings
        for stream_name in stream_mappings.keys():
            stream_timestamp_field = stream_mappings[stream_name].time_stamp
            mapping = stream_mappings[stream_name].mapping_fields

            descriptor_keys = self._extract_stream_descriptor_keys(mapping)
            stream_bundle = run_bundle.compose_descriptor(
                data_keys=descriptor_keys,
                name=stream_name)
            yield 'descriptor', stream_bundle.descriptor_doc

            num_events = calc_num_events(stream_mappings[stream_name].mapping_fields, self._file)
            if num_events == 0:  # test this
                continue
            
            # produce documents for each event (event and datum)
            for x in range(0, num_events):
                try:
                    time_stamp_dataset = self._file[stream_timestamp_field][()]
                except Exception as e:
                    raise EmptyTimestampsError(stream_name, stream_timestamp_field) from e
                if time_stamp_dataset is None or len(time_stamp_dataset) == 0:
                    raise EmptyTimestampsError(stream_name, stream_timestamp_field)
                event_data = {}
                event_timestamps = {}
                filled_fields = {}
                # create datums and events
                for field in stream_mappings[stream_name].mapping_fields:
                    # Go through each field in the stream. If field not marked
                    # as external, extract the value. Otherwise create a datum
                    dataset = self._file[field.name]
                    encoded_key = encode_key(field.name)
                    event_timestamps[encoded_key] = time_stamp_dataset[x]
                    if field.external:
                        if logger.isEnabledFor(logging.INFO):
                            logger.info(f'event for {field.external} inserted as datum')
                        # field's data provided in datum
                        datum = hd5_resource.compose_datum(datum_kwargs={
                                "key": encoded_key,
                                "point_number": x})  # need kwargs for HDF5 datum
                        yield 'datum', datum
                        event_data[encoded_key] = datum['datum_id']
                        filled_fields[encoded_key] = False
                    else:
                        # field's data provided in event
                        if logger.isEnabledFor(logging.INFO):
                            logger.info(f'event for {field.external} inserted in event')
                        event_data[encoded_key] = dataset[x]

                yield 'event', stream_bundle.compose_event(
                    data=event_data,
                    filled=filled_fields,
                    seq_num=x,
                    timestamps=event_timestamps
                )

        stop_doc = run_bundle.compose_stop()
        yield 'stop', stop_doc

    def _extract_metadata(self):
        metadata = {}
        for key in self._mapping.metadata_mappings.keys():
            # event_model won't accept / in metadata keys, so
            # we replace them with :, after removing the leading slash
            encoded_key = encode_key(key)
            try:
                metadata[encoded_key] = self._file[key][()]
            except Exception:
                raise MappingNotFoundError('metadata', key)
        return metadata

    def _extract_stream_descriptor_keys(self, stream_mapping: StreamMapping):
        descriptors = {}
        for mapping_field in stream_mapping:
            # build an event_model descriptor
            try:
                hdf5_dataset = self._file[mapping_field.name]
            except Exception:
                raise MappingNotFoundError('stream', mapping_field.name)
            units = hdf5_dataset.attrs.get('units')
            # descriptor = EMDescriptor(
            #         dtype='number',
            #         source='file',
            #         shape=hdf5_dataset.shape[1::],
            #         units=units)
            descriptor = dict(
                    dtype='number',
                    source='file',
                    shape=hdf5_dataset.shape[1::],
                    units=units)
            if mapping_field.external:
                descriptor['external'] = 'FILESTORE:'

            encoded_key = encode_key(mapping_field.name)
            descriptors[encoded_key] = descriptor
        return descriptors


def encode_key(key):
    return key.replace("/", ":")


def decode_key(key):
    return key.replace(":", "/")


def calc_num_events(mapping_fields: List[StreamMappingField], file):
    # grab the first dataset referenced in the map,
    # then see how many events using first dimension of shape
    # of first dataset
    if len(mapping_fields) == 0:
        return 0
    name = mapping_fields[0].name
    return file[name].shape[0]

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
