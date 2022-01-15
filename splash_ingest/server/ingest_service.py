from dataclasses import dataclass
from datetime import datetime
from  importlib.util import spec_from_file_location, module_from_spec
import json
import logging
from pathlib import Path
import sys
import time
from typing import List
import traceback
from uuid import uuid4

import h5py
import pandas as pd
from pydantic import parse_obj_as
from pymongo import MongoClient
from pymongo.collection import Collection

from splash_ingest.model import Mapping
from .model import (
    IngestType,
    Job,
    JobStatus,
    StatusItem,
    RevisionStamp
)
# from pyscicat import from_credentials

logger = logging.getLogger("splash_ingest.ingest_service")
# these context objects help us inject dependencies, useful
# in unit testing


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

# Load scicat reader python files from from ../readers
reader_modules = {}

def init_ingest_service(ingest_db: MongoClient, readers_dir: Path = None):
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

    # Load all reader modules from the reader directory
    if not readers_dir:
        readers_dir = Path(Path().absolute(), 'readers')
    for file in readers_dir.glob('*.py'):
        try:
            spec = spec_from_file_location(file.stem, file)
            reader_module = module_from_spec(spec)
            spec.loader.exec_module(reader_module)
            if reader_module.spec in reader_modules.keys():
                logger.warning(f'Reader module {file} contains a duplicate spec: {reader_module.spec}. Ignoring.')
                continue
            reader_modules[reader_module.spec] = reader_module
        except Exception as e:
            logger.error(f' Error loading {file}: {e}')

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


def find_mapping(submitter, mapping_id: str) -> Mapping:
    mapping_dict = service_context.ingest_mappings.find_one({"name": mapping_id})
    if mapping_dict is None:
        return None
    revision = mapping_dict.pop('revision')
    return Mapping(**mapping_dict)


def create_mapping(submitter, mapping: Mapping):
    id = str(uuid4())
    revision = RevisionStamp(user=submitter, time=datetime.utcnow(), version_id=id)
    insert_dict = mapping.dict()
    insert_dict['id'] = id
    insert_dict['revision'] = revision.dict()
    service_context.ingest_mappings.insert_one(insert_dict)
    return id


def poll_for_new_jobs(
    sleep_interval,
    scicat_baseurl,
    scicat_user,
    scicat_password,
    terminate_requested,
    thumbs_root=None
):

    logger.info(f"Beginning polling, waiting {sleep_interval} each time")
    while True:
        try:
            job_list = find_unstarted_jobs()
            if terminate_requested.state:
                logger.info("Terminate requested, exiting")
                return
            if len(job_list) == 0:
                time.sleep(sleep_interval)
            else:
                job: Job = job_list[-1]
                logger.info(f"ingesting path: {job.document_path} mapping: {job.mapping_id}")
                ingest('system',
                       job_list[-1],
                       thumbs_root,
                       scicat_baseurl,
                       scicat_user,
                       scicat_password)
        except Exception as e:
            logger.exception('polling thread exception', e)


def ingest(submitter: str, job: Job, thumbs_root=None, scicat_baseurl=None, scicat_user=None, scicat_password=None) -> str:
    """Updates job status and calls ingest method specified in job

    Parameters
    ----------
    submitter : str
        user identification of submitter
    job : Job
        job tracking this ingestion

    Returns
    -------
    str
        uid of the newly created start document
    """
    try:
        logger.info(f"{job.id} started job {job.id}")

        # this can be run on many processes, so
        # we can use thread locks to assure that the 
        # job hasn't already been started, so check here first. Not perfect...
        persisted_job = find_job(job.id)
        if persisted_job.status != JobStatus.submitted:
            logger.info(f"{job.id} on document {job.document_path} already started, exiting.")
            return

        set_job_status(job.id,
                       StatusItem(
                        time=datetime.utcnow(),
                        submitter=job.submitter,
                        status=JobStatus.running,
                        log='Starting job'))
        mapping_with_revision = find_mapping(submitter, job.mapping_id)

        if mapping_with_revision is None:
            job_log = f"{job.id} no mapping found for {job.mapping_id}"
            logger.info(job_log)
            set_job_status(job.id, StatusItem(time=datetime.utcnow(),
                           status=JobStatus.error, submitter=submitter, log=job_log))
            return

        logger.info(f"mapping found for {job.id} for file {job.document_path}") 
        mapping = mapping_with_revision
        file = h5py.File(job.document_path, "r")


        if IngestType.scicat in job.ingest_types:
            

            logger.info(f"{job.id} scicat ingestion starting")
        #     scicat_ingestor = ScicatIngestor(
        #         start_uid,
        #         issues,
        #         baseurl=scicat_baseurl,
        #         username=scicat_user,
        #         password=scicat_password,
        #         job_id=job.id)
        #     scicat_ingestor.ingest_run(
        #         Path(job.document_path),
        #         start_doc,
        #         descriptor_doc,
        #         event_sample=event_sample,
        #         thumbnails=doc_generator.thumbnails)
        #     logger.info(f"{job.id} scicat ingestion complete")
        # job_log = f'ingested start doc: {start_uid}'

        # if issues and len(issues) > 0:
        #     status = JobStatus.complete_with_issues
        #     for issue in issues:
        #         if issue.severity == Severity.error:
        #             status = JobStatus.error
        #         job_log += f"\n :  {issue.msg}"
        #         if issue.exception:
        #             job_log += f"\n    Exception: {issue.exception}"
        #     status = StatusItem(time=datetime.utcnow(), status=status,
        #                         submitter=submitter, log=job_log, issues=issues)
        # else:
        #     status = StatusItem(time=datetime.utcnow(), status=JobStatus.successful, submitter=submitter, log=job_log)
        # set_job_status(job.id, status)
        # return start_uid

    except Exception:
        exc_type, exc_value, exc_tb = sys.exc_info()
        job_log = traceback.format_exception(exc_type, exc_value, exc_tb)
        status = StatusItem(time=datetime.utcnow(), status=JobStatus.error, submitter=submitter, log=str(job_log))
        set_job_status(job.id, status)


def sample_event_page(event_page, sample_size=10):
    df = pd.DataFrame(data=event_page['data'])
    df = df.rename(columns=lambda s: s.replace(":", "/"))
    if len(df) == 0:
        return {}
    step = len(df) // sample_size
    if step == 0:
        step == 1
    return json.loads(df[0:: step].to_json())