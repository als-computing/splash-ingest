# Deployment
The repository contains a [Makefile](../Makefile) that assists in locally creating docker images and pushing those images to a docker repository (currently, repository.spin.nersc.gov)

Deployment requires a running docker enviornmnet on your local machine.

### Settings
Borth servicews accept several environment variables to be set. These can be passed into docker using the -e flag. You can also 
create a .env file that stores them.

```
MONGO_DB_URI - complete url for accessing mongo (defaults to mongodb://localhost:27017/splash)
LOG_LEVEL - defaults to INFO
THUMBS_ROOT - direcrotery where thumbnails will be stored during ingestion, only used by poller

```

## API Service


### Build
To build a docker image of  the API web service, run:

`make build_service`

This will create an image tagged with the repository url and version take from `git describe --tags`


### Run locally
Once built, you can launch the docker image using:

`run.sh`

### Push to registry
Once you're happy with the image, you can push it to the registry using:

`make push_service`

### 


## Job Polling Service
### Build
To build a docker image of the polling service:

`make build_poller`

### Run locally
Once build, you can launch the docker image using:

`run_poller.sh`

### Push to registry
Once you're happy with the image, you can push it to the registry using:

`make push_poller`


## Developer
Some developers like to do development outside of containers. You can certainly run both processes locally, which is neato especially for using development tools like debuggers.

Both process read an optional .env file in the root folder for storing and passing environmnent variables, if desired. Here's a sample of contents:


```
MONGO_DB_URI=mongodb://localhost:27017/splash
LOG_LEVEL=INFO
THUMBS_ROOT=/path/to/thumbs/
```


To launch the webservice:

`uvicorn splash_ingest.server.api:app --reload` 

The optional `--reload` parameter detects file changes and reloads python.

To laucn the poller:

`splash_ingest_manager/poller.py`
