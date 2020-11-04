from dataclasses import dataclass
from datetime import datetime
import h5py
import logging
import sys
from threading import Thread
import time
from typing import List
import traceback
from uuid import uuid4


from pydantic import parse_obj_as
from pymongo import MongoClient
from pymongo.collection import Collection

from suitcase.mongo_normalized import Serializer

from splash_ingest.model import Mapping
from splash_ingest import MappedHD5Ingestor
from .model import Job, JobStatus, StatusItem, RevisionStamp

logger = logging.getLogger(__name__)
# these context objects help us inject dependencies, useful
# in unit testing

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


def init_ingest_service(db: MongoClient):
    bluesky_context.serializer = Serializer(metadatastore_db=db, asset_registry_db=db)
    bluesky_context.db = db
    service_context.db = db
    service_context.ingest_jobs = db['ingest_jobs']
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

    service_context.ingest_mappings = db['ingest_mappings']
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


def start_job_poller():
    find_jobs_thread = Thread(target=poll_for_new_jobs, daemon=True)
    find_jobs_thread.start()
    logger.info('polling thread started, service initialization complete')


def create_job(submitter, document_path: str, mapping_id: str):
    job = Job(document_path=document_path,)
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
    return job


def find_job(job_id: str) -> Job:
    job_dict = service_context.ingest_jobs.find_one({"id": job_id})
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


def find_mapping(submitter, mapping_id: str) -> Mapping:
    mapping_dict = service_context.ingest_mappings.find_one({"name": mapping_id})
    if mapping_dict is None:
        return None
    revision = mapping_dict.pop('revision')
    return Mapping(**mapping_dict), revision


def create_mapping(submitter, mapping: Mapping):
    id = str(uuid4())
    revision = RevisionStamp(user=submitter, time=datetime.utcnow(), version_id=id)
    insert_dict = mapping.dict()
    insert_dict['id'] = id
    insert_dict['revision'] = revision.dict()
    service_context.ingest_mappings.insert_one(insert_dict)
    return id


def poll_for_new_jobs():
    while True:
        try:
            job_list = find_unstarted_jobs()
            if len(job_list) == 0:
                time.sleep(1)
            else:
                ingest('system', job_list[-1])
                # with ProcessPoolExecutor(max_workers=3) as executor:
                #     executor.submit(ingest, "mongodb://127.0.0.1/splash", 'system', job_list[-1])
        except Exception as e:
            logger.exception('polling thread exception', e)


def ingest(submitter: str, job: Job):
    try:
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"started job {repr(job)}")

        # this can be run on many processes, so
        # we can use thread locks to assure that the 
        # job hasn't already been started, so check here first. Not perfect...
        persisted_job = find_job(job.id)
        if persisted_job.status != JobStatus.submitted:
            logger.info(f"Job {job.id} on document {job.document_path} already started, exiting.")
            return
        
        set_job_status(job.id,
                       StatusItem(
                        time=datetime.utcnow(),
                        submitter=job.submitter,
                        status=JobStatus.running,
                        log='Starting job'))
        mapping_with_revision = find_mapping(submitter, job.mapping_id)

        if mapping_with_revision is None:
            log = f"no mapping found for {job.id} - {job.mapping_id.name} {job.mapping_id.version}"
            logger.info(log)
            set_job_status(job.id, StatusItem(time=datetime.utcnow(), status=JobStatus.error, submitter=submitter, log=log))
            return
        
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"mapping found for {job}")    
        mapping = mapping_with_revision[0]
        file = h5py.File(job.document_path, "r")
        ingestor = MappedHD5Ingestor(mapping, file, 'mapping_ingestor', None)
        start_uid = None
        for name, document in ingestor.generate_docstream():
            if name == 'start':
                start_uid = document['uid']
            bluesky_context.serializer(name, document)
        log = f'succesfully ingested start doc: {start_uid}'
        status = StatusItem(time=datetime.utcnow(), status=JobStatus.successful, submitter=submitter, log=log)
        set_job_status(job.id, status)
        return start_uid

    except Exception:
        exc_type, exc_value, exc_tb = sys.exc_info()
        log = traceback.format_exception(exc_type, exc_value, exc_tb)
        status = StatusItem(time=datetime.utcnow(), status=JobStatus.error, submitter=submitter, log=str(log))
        set_job_status(job.id, status)