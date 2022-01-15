from dataclasses import dataclass
from datetime import datetime
import json
import logging
from pathlib import Path
from pprint import pprint
import sys
from typing import List
import traceback
from uuid import uuid4

import h5py
import numpy as np
import pandas as pd
from PIL import Image, ImageOps
from prefect import task, Flow, Parameter
from pydantic import parse_obj_as
from pymongo import MongoClient
from pymongo.collection import Collection

from suitcase.mongo_normalized import Serializer

from splash_ingest.model import Mapping, Issue, Severity, StreamMapping, ThumbnailInfo
from splash_ingest.docstream import MappedH5Generator
from splash_ingest.server.model import (
    IngestType,
    Job,
    JobStatus,
    StatusItem,
    RevisionStamp
)
from splash_ingest.scicat import ScicatIngestor, project_start_doc

logger = logging.getLogger("splash_ingest.ingest_service")
# these context objects help us inject dependencies, useful
# in unit testing
from starlette.config import Config
config = Config(".env")
DATABROKER_DB_URI = config("DATABROKER_DB_URI", cast=str, default="mongodb://localhost:27017/databroker")
DATABROKER_DB_NAME = config("DATABROKER_DB_NAME", cast=str, default="databroker")
INGEST_DB_URI = config("INGEST_DB_URI", cast=str, default="mongodb://localhost:27017/ingest")
INGEST_DB_NAME = config("INGEST_DB_NAME", cast=str, default="ingest")
INGEST_LOG_LEVEL = config("INGEST_LOG_LEVEL", cast=str, default="INFO")
POLLER_MAX_THREADS = config("POLLER_MAX_THREADS", cast=int, default=1)
POLLER_SLEEP_SECONDS = config("POLLER_SLEEP_SECONDS", cast=int, default=5)
THUMBS_ROOT = config("THUMBS_ROOT", cast=str, default="thumbs")
SCICAT_BASEURL = config("SCICAT_BASEURL", cast=str, default="http://localhost:3000/api/v3")
SCICAT_INGEST_USER = config("SCICAT_INGEST_USER", cast=str, default="ingest")
SCICAT_INGEST_PASSWORD = config("SCICAT_INGEST_PASSWORD", cast=str, default="aman")


class JobNotFoundError(Exception):
    pass

@dataclass
class ServiceMongoCollectionsContext():
    db: MongoClient = None
    ingest_jobs: Collection = None
    ingest_mappings: Collection = None


@dataclass
class BlueskyContext():
    serializer = None
    db: MongoClient = None


service_context = ServiceMongoCollectionsContext()
bluesky_context = BlueskyContext()


def init_ingest_service(ingest_db: MongoClient, databroker_db: MongoClient):
    bluesky_context.serializer = Serializer(metadatastore_db=databroker_db, asset_registry_db=databroker_db)
    bluesky_context.db = databroker_db
    service_context.db = ingest_db
    service_context.ingest_jobs = ingest_db['ingest_jobs']
    service_context.ingest_jobs.create_index(
        [
            ('submit_time', -1)
        ]
    )

    service_context.ingest_jobs.create_index(
        [
            ('status', -1)
        ]
    )

    service_context.ingest_jobs.create_index(
        [
            ('id', 1),
        ],
        unique=True
    )

    service_context.ingest_mappings = ingest_db['ingest_mappings']
    service_context.ingest_mappings.create_index(
        [
            ('name', -1),
            ('version', -1),
        ],
        unique=True
    )
    service_context.ingest_mappings.create_index(
        [
            ('id', 1),
        ],
        unique=True
    )

async def startup_event():
    logger.info('starting api server')
    logger.info(f"DATABROKER_DB_URI {DATABROKER_DB_URI}")
    logger.info(f"DATABROKER_DB_NAME {DATABROKER_DB_NAME}") 
    logger.info(f"INGEST_DB_URI {INGEST_DB_URI}")
    logger.info(f"INGEST_DB_NAME {INGEST_DB_NAME}")
    databroker_db = MongoClient(DATABROKER_DB_URI)[DATABROKER_DB_NAME]
    ingest_db = MongoClient(INGEST_DB_URI)[INGEST_DB_NAME]
    init_ingest_service(ingest_db, databroker_db)
    # init_api_service(ingest_db)

def create_job(
        submitter,
        document_path: str,
        mapping_id: str,
        ingest_types: List[IngestType]):

    job = Job(document_path=document_path, ingest_types=ingest_types)
    job.id = str(uuid4())
    job.mapping_id = mapping_id
    job.submit_time = datetime.utcnow()
    job.submitter = submitter
    job.status = JobStatus.submitted
    job.status_history.append(StatusItem(
        time=job.submit_time,
        status=job.status,
        submitter=submitter))
    service_context.ingest_jobs.insert_one(job.dict())
    # TODO check that file exists and throw error 
    return job


def find_job(job_id: str) -> Job:
    job_dict = service_context.ingest_jobs.find_one({"id": job_id})
    if not job_dict:
        raise JobNotFoundError(f"Job not found {job_id}")
    return Job(**job_dict)


def find_unstarted_jobs() -> List[Job]:
    jobs = list(service_context.ingest_jobs.find({"status": JobStatus.submitted}))
    return parse_obj_as(List[Job], jobs)


def set_job_status(job_id, status_item: StatusItem):
    update_result = service_context.ingest_jobs.update_one({"id": job_id},
                                                       {"$set": {
                                                            "start_time": status_item.time,
                                                            "status": status_item.status,
                                                            "submitter": status_item.submitter}})
    update_result = service_context.ingest_jobs.update_one({"id": job_id},
                                                       {"$push": {
                                                            "status_history":
                                                            status_item.dict()}})
    return update_result.modified_count == 1


def create_mapping(submitter, mapping: Mapping):
    id = str(uuid4())
    revision = RevisionStamp(user=submitter, time=datetime.utcnow(), version_id=id)
    insert_dict = mapping.dict()
    insert_dict['id'] = id
    insert_dict['revision'] = revision.dict()
    service_context.ingest_mappings.insert_one(insert_dict)
    return id


def sample_event_page(event_page, sample_size=10):
    df = pd.DataFrame(data=event_page['data'])
    df = df.rename(columns=lambda s: s.replace(":", "/"))
    if len(df) == 0:
        return {}
    step = len(df) // sample_size
    if step == 0:
        step == 1
    return json.loads(df[0:: step].to_json())

@task
def find_mapping(mapping_id: str) -> Mapping:
    mapping_dict = service_context.ingest_mappings.find_one({"name": mapping_id})
    if mapping_dict is None:
        raise Exception(f"cannot find mapping {mapping_id}")
    revision = mapping_dict.pop('revision')
    return Mapping(**mapping_dict)

@task 
def ingest_event_model(issues, dataset_loc, mapping, persist_event_model):
        with h5py.File(dataset_loc, "r") as file:
            doc_generator = MappedH5Generator(issues, mapping, file, 'mapping_ingestor', pack_pages=True)
            # we always generate a docstream, but depending on the ingestors listed in the job we may do different things
            # with the docstream

            start_uid = None
            start_doc = {}
            descriptor_doc = {}
            event_sample = {}
            for name, document in doc_generator.generate_docstream():
                if name == 'start':
                    start_uid = document['uid']
                    start_doc = document
                if name == 'descriptor':
                    descriptor_doc = document
                if name == 'event_page':
                    event_sample = sample_event_page(document)
                if not persist_event_model:
                    continue
                try:
                    bluesky_context.serializer(name, document)
                except Exception as e:
                    logger.error("Exception storing document %s", name, exc_info=1)
                    pprint(document)
                    raise e
            logger.info(f"{dataset_loc} databroker ingestion complete")
            return {
                "start": start_doc,
                "descriptor": descriptor_doc,
                "event_sample": event_sample
            }


@task
def ingest_scicat(issues, job_id, scicat_config, dataset_loc, event_model_docs, thumbnails):
    scicat_ingestor = ScicatIngestor(
        issues,
        baseurl=scicat_config['scicat_baseurl'],
        username=scicat_config['scicat_user'],
        password=scicat_config['scicat_password'],
        job_id=job_id)

    scicat_ingestor.ingest_run(
        Path(dataset_loc),
        event_model_docs['start'],
        event_model_docs['descriptor'],
        event_sample=event_model_docs['event_sample'],
        thumbnails=thumbnails)

class ThumnailDimensionError(Exception):
    ''' specified thumnail has unsupported dimensions '''
    pass

@task
def build_thumbnails(issues, dataset_loc, mapping: Mapping, thumbs_root):
    with h5py.File(dataset_loc, "r") as h5file:
        thumbnails = []
        for stream in mapping.stream_mappings.keys():
            stream_mapping = mapping.stream_mappings[stream]
            dataset = h5file[stream_mapping.thumbnail_info.field]
            if len(dataset.shape) != 3:
                raise ThumnailDimensionError(f"Thumbnail for {stream_mapping.thumbnail_info.field} expected 3 dimensions "
                                                f"got {len(dataset.shape)}")
            middle_image = round(dataset.shape[0] / 2)
            log_image = np.array(dataset[middle_image, :, :])
            log_image = log_image - np.min(log_image) + 1.001
            log_image = np.log(log_image)
            log_image = 205*log_image/(np.max(log_image))
            auto_contrast_image = Image.fromarray(log_image.astype('uint8'))
            auto_contrast_image = ImageOps.autocontrast(
                                    auto_contrast_image, cutoff=0.1)
            # auto_contrast_image = resize(np.array(auto_contrast_image),
                                                    # (size, size))                   
            dir = Path(thumbs_root)
            filename = str(uuid4()) + ".png"
            file = dir / Path(filename)
            auto_contrast_image.save(file, format='PNG')
            thumbnails.append(file)
        return thumbnails


def register( job: Job, scicat_config, thumbs_root=None) -> str:

    with Flow("Ingestion") as flow:
        issues = []
        mapping_id = Parameter("mapping_id")
        dataset_loc = Parameter("dataset_loc")
        thumbs_root_param = Parameter("thumbs_root_param")
        persist_event_model = Parameter("persist_event_model", default=True)
        mapping = find_mapping(mapping_id)
        event_model_docs = ingest_event_model(issues, dataset_loc, mapping, persist_event_model)
        thumbnails = build_thumbnails(issues, dataset_loc, mapping, thumbs_root_param)
        ingest_scicat(issues, "0", scicat_config, dataset_loc, event_model_docs, thumbnails)



    flow.register(project_name="ingest")
    flow.run(
        mapping_id=job.mapping_id,
        dataset_loc=job.document_path,
        thumbs_root_param=thumbs_root,
        persist_event_model=True)



if __name__ == "__main__":
    job = Job(**{
        "mapping_id": "als832_dx_3",
        "document_path": "/data/beamlines/als832/20210204_172250_ddd.h5",
        "ingest_types": ["databroker", "scicat"]
    })
    scicat_config = {
        "scicat_baseurl": "http://catamel:3000/api/v3",
        "scicat_user": "ingestor",
        "scicat_password": "aman",
    }
    register( job, scicat_config)
    