# splash-ingest  
![Python application](https://github.com/als-computing/splash-ingest/workflows/Python%20application/badge.svg)

Splash ingest contains tools for ingesting file system resources into bluesky [document stream](https://blueskyproject.io/event-model/) documents and Scicat.

These tools are mean to assist in mapping tasks. For example, instruments that produce hdf5 files and have no other method for ingesting metadata can use the MappedH5Generator class. This class reads a mapping file and an hdf5 file and produces a document stream. This document stream can then be serialized into a number of formats, including Mongo.

See [Components](./docs/components.md) for details about the system.

See [Ingesting](./docs/ingesting.md) for details about submiting ingestion requests.

See [Deployment](./docs/deployment.md) for details about deploying builds.

## Releases

### v0.1.23
New to v0.1.23 is the ability to ingest into Scicat. Also, improved docuemntation as well as an example python client.