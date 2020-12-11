import logging
import os
from pathlib import Path
from typing import List

import event_model
import numpy as np
from PIL import Image, ImageOps

from bluesky_live.bluesky_run import BlueskyRun, DocumentCache
from bluesky_widgets.models import auto_plot_builders
from bluesky_widgets.headless.figures import HeadlessFigures

from .model import (
    Mapping,
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
    def __init__(self, mapping: Mapping, file, reference_root_name, data_groups=[], thumbs_root=None):
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
        self._issues = []

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
        # As we compose new documents, we will add them to this in-memory
        # DocumentCache and yield them. At the end, we'll wrap this cache in a
        # BlueskyRun and use them to create thumbnail/preview images.
        document_cache = DocumentCache()
        metadata = self._extract_metadata()
        if logger.isEnabledFor(logging.DEBUG):
            keys = metadata.keys()
            logger.debug(f"Metdata keys : {list(keys)}")
        run_bundle = event_model.compose_run(metadata=metadata)
        start_doc = run_bundle.start_doc
        start_doc['projections'] = self._mapping.projections
        start_doc['data_groups'] = self._data_groups
        document_cache('start', start_doc)
        yield 'start', start_doc
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"run: {start_doc['uid']} Start doc created")
        hd5_resource = run_bundle.compose_resource(
            spec=self._mapping.resource_spec,
            root=self._reference_root_name,
            resource_path=self._file.filename,  # need to calculate a relative path
            resource_kwargs={})
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"run: {start_doc['uid']} resource doc created uid: {hd5_resource.resource_doc['uid']}")
        document_cache('resource', hd5_resource.resource_doc)
        yield 'resource', hd5_resource.resource_doc

        # produce documents for each stream
        stream_mappings: StreamMapping = self._mapping.stream_mappings
        if stream_mappings is not None:
            for stream_name in stream_mappings.keys():
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"run: {start_doc['uid']} Creating stream: {stream_name}")
                stream_timestamp_field = stream_mappings[stream_name].time_stamp
                mapping = stream_mappings[stream_name]

                descriptor_keys = self._extract_stream_descriptor_keys(mapping.mapping_fields)
                stream_bundle = run_bundle.compose_descriptor(
                    data_keys=descriptor_keys,
                    name=stream_name)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"run: {start_doc['uid']} Creating descriptor with "
                                 f"uid: {stream_bundle.descriptor_doc['uid']}")
                yield 'descriptor', stream_bundle.descriptor_doc
                document_cache('descriptor', stream_bundle.descriptor_doc)
                num_events = 0
                try:
                    num_events = calc_num_events(mapping.mapping_fields, self._file)
                except FieldNotInResourceError as e:
                    self.issues.append(e)
                if num_events == 0:  # test this
                    continue

                logger.info(f" run: {start_doc['uid']} expecting {str(num_events)} events")
                # produce documents for each event (event and datum)
                for x in range(0, num_events):
                    try:
                        time_stamp_dataset = self._file[stream_timestamp_field][()]
                    except Exception as e:
                        self._issues.append(f"run: {start_doc['uid']} Error fetching timestamp for {stream_name}"
                                            f"slice: {str(x)} - {str(e.args[0])}")
                        break
                    if time_stamp_dataset is None or len(time_stamp_dataset) == 0:
                        self._issues.append(f"run: {start_doc['uid']} Missing timestamp for"
                                            f"{stream_name} slice: {str(x)}")
                        break
                    event_data = {}
                    event_timestamps = {}
                    filled_fields = {}
                    # create datums and events
                    for field in mapping.mapping_fields:
                        # Go through each field in the stream. If field not marked
                        # as external, extract the value. Otherwise create a datum
                        dataset = self._file[field.field]
                        encoded_key = encode_key(field.field)
                        event_timestamps[encoded_key] = time_stamp_dataset[x]
                        if field.external:
                            # if logger.isEnabledFor(logging.DEBUG):
                            #     logger.debug(f"run: {start_doc['uid']} event for {field.external} inserted as datum")
                            # field's data provided in datum
                            datum = hd5_resource.compose_datum(datum_kwargs={
                                    "key": encoded_key,
                                    "point_number": x})  # need kwargs for HDF5 datum
                            # if logger.isEnabledFor(logging.DEBUG):
                            #     logger.debug(f"run: {start_doc['uid']} Creating datum with uid: {datum['datum_id']}")
                            document_cache('datum', datum)
                            yield 'datum', datum
                            event_data[encoded_key] = datum['datum_id']
                            filled_fields[encoded_key] = False
                        else:
                            # field's data provided in event
                            if logger.isEnabledFor(logging.INFO):
                                logger.info(f'event for {field.external} inserted in event')
                            event_data[encoded_key] = dataset[x]
                    event = stream_bundle.compose_event(
                        data=event_data,
                        filled=filled_fields,
                        seq_num=x,
                        timestamps=event_timestamps
                    )
                    document_cache('event', event)
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"run: {start_doc['uid']} Creating event with uid: {event['uid']}")
                    yield 'event', event

        stop_doc = run_bundle.compose_stop()
        document_cache('stop', stop_doc)
        bluesky_run = BlueskyRun(document_cache)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"run: {start_doc['uid']} stop doc {str(stop_doc)}")
        yield 'stop', stop_doc
        if self._thumbs_root is not None:
            self._build_thumbnail(start_doc["uid"], self._thumbs_root, bluesky_run)
        if len(self._issues) > 0:
            logger.info(f" run: {start_doc['uid']} had issues {str(self._issues)}")

    def _build_thumbnail(self, uid, directory, bluesky_run):
        model = SplashAutoImages()
        model.add_run(bluesky_run)
        view = HeadlessFigures(model.figures)
        os.makedirs(Path(directory, uid), exist_ok=True)
        filenames = view.export_all(Path(directory, uid), format='png')
        print("WROTE", filenames)

    def _extract_metadata(self):
        metadata = {}
        for mapping in self._mapping.md_mappings:
            # event_model won't accept / in metadata keys, so
            # we replace them with :, after removing the leading slash
            encoded_key = encode_key(mapping.field)
            try:
                data_value = self._file[mapping.field]
                metadata[encoded_key] = data_value[()].item().decode()
            except Exception as e:
                self._issues.append(f"Error finding mapping {encoded_key} - {str(e.args)}")
                continue
        return metadata

    def _extract_stream_descriptor_keys(self, stream_mapping: StreamMapping):
        descriptors = {}
        for mapping_field in stream_mapping:
            # build an event_model descriptor
            try:
                hdf5_dataset = self._file[mapping_field.field]
            except Exception as e:
                self._issues.append(f"Error finding mapping {mapping_field} - {str(e.args)}")
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
    except KeyError:
        raise FieldNotInResourceError("timestamp check", name)


def _log_and_auto_contrast(data):
    log_image = data - np.min(data) + 1.001
    log_image = np.log(log_image)
    log_image = 205*log_image/(np.max(log_image))
    auto_contrast_image = Image.fromarray(log_image.astype('uint8'))
    auto_contrast_image = ImageOps.autocontrast(
                            auto_contrast_image, cutoff=0.1)
    return np.asarray(auto_contrast_image)


class _ImageWithCustomScaling(auto_plot_builders._ShimmedImage):
    ...

    # def _transform(self, run, field):
    #     # I am not 100% sure this is the best language feature to use for injecting
    #     # a custom transform but otherwise reusing the rest of the logic in Images.
    #     # That's why this method is currently private. This will either become
    #     # public or we'll choose a different mechanism. - Dan Allan, Dec 2020

    #     # The base class gives us a 2D image by slicing the middle of any
    #     # higher dimensions.
    #     data = super()._transform(run, field)
    #     return _log_and_auto_contrast(data)


class SplashAutoImages(auto_plot_builders.AutoImages):
    """
    A plot builder

    Reuse `Images` plot builder but wrap it in particular logic for scaling.
    Also reuse AutoImages' heuristic for identifying images in a Run.
    """

    @property
    def _plot_builder(self):
        return _ImageWithCustomScaling


from ._version import get_versions  # noqa: E402
__version__ = get_versions()['version']
del get_versions
