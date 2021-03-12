import json
from pprint import pprint

import h5py

from pymongo import MongoClient
from suitcase.mongo_normalized import Serializer

from splash_ingest.ingestors import MappedHD5Ingestor
from splash_ingest.model import Mapping

db = MongoClient('mongodb://localhost:27017/splash')
serializer = Serializer(metadatastore_db='mongodb://localhost:27017/splash', asset_registry_db='mongodb://localhost:27017/splash')
with open('/home/dylan/work/als-computing/splash-ingest/.scratch/832Mapping.json', 'r') as f:
    data = json.load(f)

mapping = Mapping(**data)

file = h5py.File("/home/dylan/data/beamlines/als832/20210209_163124_n124_y531.h5", "r")
ingestor = MappedHD5Ingestor(mapping, file, "root_canal")

for name, doc in ingestor.generate_docstream():
    print("_________  \n")
    print(name)
    pprint(doc)
    serializer(name, doc)
pprint(ingestor.issues)

