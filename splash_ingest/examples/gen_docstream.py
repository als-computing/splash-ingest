import datetime
from importlib import reload
import json
import logging
import os
from pprint import pprint
import pytz
import sys
import tempfile
from IPython.utils.tempdir import TemporaryWorkingDirectory
from IPython.display import FileLink
import h5py
import numpy as np
from splash_ingest.docstream import MappedH5Generator, MappingNotFoundError
from splash_ingest.model import Mapping
from splash_ingest.scicat import NPArrayEncoder

logger = logging.getLogger("splash_ingest")

def init_logging():
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    logger.setLevel(logging.DEBUG)

init_logging()

def build_mapping():
    mapping_dict = {}
    with open('./mappings/832Mapping.json') as json_file:
       mapping_dict = json.load(json_file)
    # mapping_dict = mapping.mapping_dict
    # construct a mapping object from dict to validate that we typed it correctly
    return Mapping(**mapping_dict)

def ingest():
    detailed_output = True

    # file_name = build_file()
    file_name = '/home/dylan/data/beamlines/als832/20210511_163010_test1313.h5'
    print(file_name)
    with h5py.File(file_name, 'r') as my_file:

        mapping = build_mapping()

        ingestor = MappedH5Generator(mapping, my_file, "/tmp", single_event=True)

        start_doc = {}
        stop_doc = {}

        # fill up a dictionary to later run a projection from
        from databroker.core import BlueskyRun, SingleRunCache
        run_cache = SingleRunCache()
        try:
            for name, doc in ingestor.generate_docstream():
                run_cache.callback(name, doc)
                if name == "start":
                    start_doc = doc
                if name == 'descriptor':
                    print("\n\n===============")
                    print("Document:  " + name)
                    pprint(doc)
                if name == 'datum':
                    print("\n\n===============")
                    print("Document:  " + name)
                if name == 'event':
                    print("\n\n===============")
                    print("Document:  " + name)

                    # print (json.dumps(doc, indent=1, cls=NPArrayEncoder))
                else:
                    if name == 'start' or name == 'stop':
                        doc_str = json.dumps(doc, indent=4, cls=NPArrayEncoder)
                        print(f'============ {name}')
                        print(doc_str)

        except MappingNotFoundError as e:
            print('Indigestion! ' + repr(e))

    run = run_cache.retrieve()
    pprint(ingestor.issues)

if __name__ == "__main__":
    ingest()