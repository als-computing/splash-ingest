# splash-ingest  
![Python application](https://github.com/als-computing/splash-ingest/workflows/Python%20application/badge.svg)

Splash ingest contains tools for ingesting file system resources into bluesky [document stream](https://blueskyproject.io/event-model/) documents. 

These tools are mean to assist in mapping tasks. For example, instruments that produce hdf5 files and have no other method for ingesting metadata can use the MappedHD5Ingestor class. This class reads a mapping file and an hdf5 file and produces a document stream. This document stream can then be serialized into a number of formats, including Mongo.

<!-- For an example of this, see [ingestion notebook](examples/mapping_ingestor.ipynb). -->

The repository implements functionality for accepting lists of ingestion jobs. An ingestion job takes a information about a file on the file system and a mapping document in a database. The jobs api simply creates a new job for future processing.

Once a job is created, a separate process polls the jobs collection in mongo for new jobs to process. This process picks up a job (which has information about the file to ingest and the mapping document to run) and ingests the document into mongo using the the [mongo_normalized serializer](https://github.com/bluesky/suitcase-mongo).

The repository supports two runtime processes. One is a restful API (implemented with [FastApi](https://fastapi.tiangolo.com/ ) ). This supports submitting and retreiving information about jobs.

The second is the job poller, which polls mongo for unprocessed jobs and performs the ingestion specified in the job.

For information on deployment, including how to stand up a local instance as a developer, see [depoloyment](./docs/deployment.md)