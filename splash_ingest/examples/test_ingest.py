import json
from pprint import pprint
import sys
import h5py


from splash_ingest.ingestors import MappedHD5Ingestor
from splash_ingest.model import Mapping


def test(mapping_file, data_file):
    with open(mapping_file, 'r') as f:
        data = json.load(f)

        mapping = Mapping(**data)

        file = h5py.File(data_file, "r")
        ingestor = MappedHD5Ingestor(mapping, file, "root_canal")

        for name, doc in ingestor.generate_docstream():
            print("_________  \n")
            print(name)
            pprint(doc)
            # serializer(name, doc)
        pprint(ingestor.issues)

def main():
    if len(sys.argv) != 3:
        print ("Usage: <command> mapping.json data.h5, Given: ", sys.argv)
        return
    mapping_file = sys.argv[1]
    h5_file = sys.argv[2]
    test(mapping_file, h5_file)

if __name__ == "__main__":
    main()
