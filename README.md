# splash-ingest
Splash ingest contains tools for ingesting file system resources into bluesky [document stream](https://blueskyproject.io/event-model/) documents. 

These tools are mean to assist in mapping tasks. For example, instruments that produce hdf5 files and have no other method for ingesting metadata can use the MappedHD5Ingestor class. This class reads a mapping file and an hdf5 file and produces a document stream. This document stream can then be serialized into a number of formats, including Mongo.

For an example of this, see [ingestion notebook](examples/mapping_ingestor.ipynb).