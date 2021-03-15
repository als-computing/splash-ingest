import logging
from pathlib import Path
from re import S
from typing import List

import event_model
import numpy as np
from PIL import Image, ImageOps

from .model import (
    ConfigurationMapping,
    Mapping,
    MappingField,
    StreamMapping,
    StreamMappingField
)

logger = logging.getLogger('splash_ingest')


class MappingNotFoundError(Exception):
    def __init__(self, location, missing_field):
        self.missing_field = missing_field
        self.location = location
        super().__init__(f"Cannot find mapping file: {location} - {missing_field}")


class FieldNotInResourceError(Exception):
    def __ini__(self, stream, field):
        self.stream = stream
        self.field = field
        super().__init__(f"Cannot find mapping timestamps for stream {stream} using timestamp mapping: {field}")


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
    def __init__(self, mapping: Mapping, file, reference_root_name, pack_pages=True, data_groups=[], thumbs_root=None):
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
        self._data_groups = data_groups
        self._thumbs_root = thumbs_root
        self._thumbnails: [Path] = []
        self._issues = []
        self._run_bundle = None
        self._pack_pages = pack_pages
        if self._pack_pages:
            self._events = []
            self._datums = []

    @property
    def thumbnails(self):
        return self._thumbnails

    @property
    def issues(self):
        return self._issues

    def generate_docstream(self):
        """Generates docstream documents
        Several things to note about what documnets are produced:
    
        - run_stop : one run stop document will be produced. Fields will be added
        at the root level that correspond to the md_mappings section of the Mappings.
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
        logger.info(f"Beginning ingestion for {self._file} using mapping {self._mapping.name}"
                    " for data_groups {self._data_groups}")
        metadata = self._extract_metadata()
        if logger.isEnabledFor(logging.DEBUG):
            keys = metadata.keys()
            logger.debug(f"Metdata keys : {list(keys)}")
        self._run_bundle = event_model.compose_run(metadata=metadata)
        start_doc = self._run_bundle.start_doc
        start_doc['projections'] = self._mapping.projections
        start_doc['data_groups'] = self._data_groups
        yield 'start', start_doc
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"run: {start_doc['uid']} Start doc created")
        h5_resource = self._run_bundle.compose_resource(
            spec=self._mapping.resource_spec,
            root=self._reference_root_name,
            resource_path=self._file.filename,  # need to calculate a relative path
            resource_kwargs={})
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"run: {start_doc['uid']} resource doc created uid: {h5_resource.resource_doc['uid']}")
        yield 'resource', h5_resource.resource_doc

        # produce documents for each stream
        if self._mapping.stream_mappings is not None:
            for stream_name in self._mapping.stream_mappings.keys():
                yield from self._process_stream(stream_name, h5_resource)

        stop_doc = self._run_bundle.compose_stop()
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"run: {start_doc['uid']} stop doc {str(stop_doc)}")
        yield 'stop', stop_doc
        if len(self._issues) > 0:
            logger.info(f" run: {start_doc['uid']} had issues {str(self._issues)}")

    def _process_stream(self, stream_name, resource_doc):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"run: Creating stream: {stream_name}")
        stream_timestamp_field = self._mapping.stream_mappings[stream_name].time_stamp
        stream_mapping = self._mapping.stream_mappings[stream_name]

        descriptor_keys = self._extract_stream_descriptor_keys(stream_mapping.mapping_fields)
        configuration = self._extract_stream_configuration(stream_mapping.conf_mappings)
        stream_bundle = self._run_bundle.compose_descriptor(
            data_keys=descriptor_keys,
            name=stream_name,
            configuration=configuration
            )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"run: Creating descriptor with "
                         f"uid: {stream_bundle.descriptor_doc['uid']}")
        yield 'descriptor', stream_bundle.descriptor_doc
        num_events = 0
        try:
            num_events = calc_num_events(stream_mapping.mapping_fields, self._file)
        except FieldNotInResourceError as e:
            self.issues.append(e)
        if num_events == 0:  # test this
            return

        logger.info(f"expecting {str(num_events)} events")
        # produce documents for each event (event and datum)
        for x in range(0, num_events):
            try:
                time_stamp_dataset = self._file[stream_timestamp_field][()]
            except Exception as e:
                self._issues.append(f"Error fetching timestamp for {stream_name}"
                                    f"slice: {str(x)} - {str(e.args[0])}")
                break
            if time_stamp_dataset is None or len(time_stamp_dataset) == 0:
                self._issues.append(f"Missing timestamp for"
                                    f"{stream_name} slice: {str(x)}")
                break
            event_data = {}
            event_timestamps = {}
            filled_fields = {}
            # create datums and events
            for field in stream_mapping.mapping_fields:
                # Go through each field in the stream. If field not marked
                # as external, extract the value. Otherwise create a datum
                try:
                    dataset = self._file[field.field]
                except Exception as e:
                    self._issues.append(f"Error finding event mapping {field.field}")
                    continue

                encoded_key = encode_key(field.field)
                event_timestamps[encoded_key] = time_stamp_dataset[x]
                if field.external:
                    # if logger.isEnabledFor(logging.DEBUG):
                    #     logger.debug(f"run: {start_doc['uid']} event for {field.external} inserted as datum")
                    # field's data provided in datum
                    datum = resource_doc.compose_datum(datum_kwargs={
                            "key": encoded_key,
                            "point_number": x})  # need kwargs for HDF5 datum
                    # if logger.isEnabledFor(logging.DEBUG):
                    #     logger.debug(f"run: {start_doc['uid']} Creating datum with uid: {datum['datum_id']}")
                    if self._pack_pages:
                        self._datums.append(datum)
                    else:
                        yield 'datum', datum
                    event_data[encoded_key] = datum['datum_id']
                    filled_fields[encoded_key] = False
                else:
                    # field's data provided in event
                    if logger.isEnabledFor(logging.INFO):
                        logger.info(f'event for {field.external} inserted in event')
                    event_data[encoded_key] = dataset[x]
            
                if (stream_mapping.thumbnails and stream_mapping.thumbnails > 0
                    and self._thumbs_root is not None and len(dataset.shape) == 3):
                    file = self._build_thumbnail(self._run_bundle.start_doc['uid'], self._thumbs_root, dataset)
                    self._thumbnails.append(file)

            event = stream_bundle.compose_event(
                data=event_data,
                filled=filled_fields,
                seq_num=x,
                timestamps=event_timestamps
            )

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Creating event with uid: {event['uid']}")

            if self._pack_pages:
                self._events.append(event)
            else:
                yield 'event', event

        if self._pack_pages:
            if len(self._events) > 0:
                yield "event_page", event_model.pack_event_page(*self._events)
            if len(self._datums) > 0:
                yield "datum_page", event_model.pack_datum_page(*self._datums)



    def _build_thumbnail(self, uid, directory, data):
        middle_image = round(data.shape[0] / 2)
        log_image = np.array(data[middle_image, :, :])
        log_image = log_image - np.min(log_image) + 1.001
        log_image = np.log(log_image)
        log_image = 205*log_image/(np.max(log_image))
        auto_contrast_image = Image.fromarray(log_image.astype('uint8'))
        auto_contrast_image = ImageOps.autocontrast(
                                auto_contrast_image, cutoff=0.1)
        # auto_contrast_image = resize(np.array(auto_contrast_image),
                                                # (size, size))                   
        dir = Path(directory)
        filename = uid + ".png"
        # file = io.BytesIO()
        file = dir / Path(filename)
        auto_contrast_image.save(file, format='PNG')
        return file

    def _extract_metadata(self):
        metadata = {}
        for mapping in self._mapping.md_mappings:
            # event_model won't accept / in metadata keys, so
            # we replace them with :, after removing the leading slash
            encoded_key = encode_key(mapping.field)
            try:
                metadata[encoded_key] = get_dataset_value(self._file[mapping.field])
            except Exception as e:
                self._issues.append(f"Error finding run_start mapping {mapping.field}")
                continue
        return metadata

    def _extract_stream_descriptor_keys(self, stream_mapping: StreamMapping):
        descriptors = {}
        for mapping_field in stream_mapping:
            # build an event_model descriptor
            try:
                hdf5_dataset = self._file[mapping_field.field]
            except Exception as e:
                self._issues.append(f"Error finding stream mapping {mapping_field}")
                continue
            units = hdf5_dataset.attrs.get('units')
            if units is not None:
                units = units.decode()
            descriptor = dict(
                    dtype='number',
                    source='file',
                    shape=hdf5_dataset.shape[1::],
                    units=units)
            if mapping_field.external:
                descriptor['external'] = 'FILESTORE:'

            encoded_key = encode_key(mapping_field.field)
            descriptors[encoded_key] = descriptor
        return descriptors


    def _extract_stream_configuration(self, configuration_mapping: ConfigurationMapping, ):
        """Builds a single configuration object for the streams's event descriptor.
            https://blueskyproject.io/event-model/event-descriptors.html#configuration
        Parameters
        ----------
        configuration_mapping : ConfigurationMapping
            [description]
        """
     
        confguration = {}
        if configuration_mapping is None:
            return confguration

        for conf_mapping in configuration_mapping:
            device_config = {
                "data": {},
                "timestamps": {},
                "data_keys": {}
            }

            for field in conf_mapping.mapping_fields:
                encoded_key = encode_key(field.field)
                try:
                    hdf5_dataset = self._file[field.field]
                except Exception as e:
                    self._issues.append(f"Error finding event desc configuration mapping {field.field}")
                    continue
                units = hdf5_dataset.attrs.get('units')
                if units is not None:
                    units = units.decode()
                data_keys = dict(
                        dtype='number',
                        source='file',
                        shape=hdf5_dataset.shape[1::],
                        units=units)
                
                device_config['data'][encoded_key] = get_dataset_value(hdf5_dataset)
                # device_config['timestamps']['field'] = 
                device_config['data_keys'][encoded_key] = data_keys
            confguration[conf_mapping.device] = device_config

        return confguration


def get_dataset_value(data_set):
    if "S" in data_set.dtype.str:
        return data_set[()].decode("utf-8")
    else:
        return data_set[()]

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
    name = mapping_fields[0].field
    try:
        return file[name].shape[0]
    except KeyError as e:
        raise FieldNotInResourceError("timestamp check", name)

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
