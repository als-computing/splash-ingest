import json
from pprint import pprint

import h5py

from splash_ingest.ingestors import MappedHD5Ingestor
from splash_ingest.model import Mapping

with open('/home/dylan/work/als-computing/splash-ingest/.scratch/832Mapping.json', 'r') as f:
    data = json.load(f)

mapping = Mapping(**data)

file = h5py.File("/home/dylan/data/beamlines/als832/20210129_150855_ddd.h5", "r")
ingestor = MappedHD5Ingestor(mapping, file, "root_canal")
for name, doc in ingestor.generate_docstream():
    print("_________  \n")
    print(name)
    pprint(doc)

pprint(ingestor.issues)