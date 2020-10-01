from typing import Dict
import event_model
from .model import Mapping, StreamMapping, EMDescriptor

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
            resource_kwargs={})
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
                # create datums and events
                for field_name in stream_mappings[stream_name].mapping_fields:
                    # Go through each field in the stream. For
                    # 1D fields, extract the value. For greater
                    # than 1D, build a datum and references that in the event.
                    dataset = self._file[field_name]
                    transformed_key = self._transform_key(field_name)
                    event_timestamps[transformed_key] = time_stamp_dataset[x]
                    if (len(dataset.shape)) == 1:
                        event_data[transformed_key] = dataset[()]
                    else:
                        datum = hd5_resource.compose_datum(datum_kwargs={'point_number': x})  # need kwargs for HDF5 datum
                        yield 'datum', datum
                        event_data[transformed_key] = datum['datum_id']

                yield 'event', stream_bundle.compose_event(
                    data=event_data,
                    filled={transformed_key: False},
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
            transformed_key = self._transform_key(key)
            try:
                metadata[transformed_key] = self._file[key][()]
            except Exception:
                raise MappingNotFoundError('metadata', key)
        return metadata

    def _extract_stream_descriptor_keys(self, stream_mapping: Dict[str, str]):
        descriptors = {}
        for key in stream_mapping.keys():
            # build an event_model descriptor
            try:
                hdf5_dataset = self._file[key]
            except Exception:
                raise MappingNotFoundError('stream', key)
            units = hdf5_dataset.attrs.get('units')
            # TODO add a way to discriminate between extenral and internal fields
            descriptor = EMDescriptor(
                dtype='number',
                source='file',
                external='FILESTORE:',
                # shape=hdf5_dataset.shape[1::],
                shape=(5, 5),
                units=units
            )
            transformed_key = self._transform_key(key)
            descriptors[transformed_key] = descriptor.dict()
        return descriptors

    def _transform_key(self, key):
        return key[1:].replace("/", ":")


def calc_num_events(stream_mappings, file):
    # grab the first dataset referenced in the map,
    # then see how many events using first dimension of shape
    # of first dataset
    if len(stream_mappings) == 0:
        return 0
    key = next(iter(stream_mappings.keys()))
    return file[key].shape[0]
