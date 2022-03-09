from dataclasses import dataclass
from datetime import datetime
from importlib.util import spec_from_file_location, module_from_spec
import json
import logging
from pathlib import Path
import sys
import time
from typing import List
import traceback
from uuid import uuid4

import pandas as pd
from pydantic import parse_obj_as
from pymongo import MongoClient
from pymongo.collection import Collection

from .model import IngestType, Job, JobStatus, StatusItem

from splash_ingest.ingestors.utils import Issue, Severity

from pyscicat.client import from_credentials

logger = logging.getLogger("splash_ingest.ingest_service")


class JobNotFoundError(Exception):
    pass


# these context objects help us inject dependencies, useful
# in unit testing
@dataclass
class ServiceMongoCollectionsContext:
    db: MongoClient = None
    ingest_jobs: Collection = None


service_context = ServiceMongoCollectionsContext()

# Load scicat reader python files from from ../readers
ingestor_modules = {}


def init_ingest_service(ingest_db: MongoClient, ingestors_dir: Path = None):
    service_context.db = ingest_db
    service_context.ingest_jobs = ingest_db["ingest_jobs"]
    service_context.ingest_jobs.create_index([("submit_time", -1)])

    service_context.ingest_jobs.create_index([("status", -1)])

    service_context.ingest_jobs.create_index(
        [
            ("id", 1),
        ],
        unique=True,
    )

    # Load all reader modules from the reader directory
    if not ingestors_dir:
        ingestors_dir = Path(Path().absolute(), "splash_ingest", "ingestors")
    for file in ingestors_dir.glob("ingest_*.py"):
        try:
            spec = spec_from_file_location(file.stem, file)
            ingestor_module = module_from_spec(spec)
            spec.loader.exec_module(ingestor_module)
            if ingestor_module.ingest_spec in ingestor_modules.keys():
                logger.warning(
                    f"Reader module {file} contains a duplicate spec: {ingestor_module.spec}. Ignoring."
                )
                continue
            ingestor_modules[ingestor_module.ingest_spec] = ingestor_module
            logger.info(
                f"loaded ingestor with spec {ingestor_module.ingest_spec} from {file}"
            )
        except Exception:
            logger.exception(f" Error loading {file}")


def create_job(
    submitter, document_path: str, mapping_id: str, ingest_types: List[IngestType]
):

    job = Job(document_path=document_path, ingest_types=ingest_types)
    job.id = str(uuid4())
    job.mapping_id = mapping_id
    job.submit_time = datetime.utcnow()
    job.submitter = submitter
    job.status = JobStatus.submitted
    job.status_history.append(
        StatusItem(time=job.submit_time, status=job.status, submitter=submitter)
    )
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
    update_result = service_context.ingest_jobs.update_one(
        {"id": job_id},
        {
            "$set": {
                "start_time": status_item.time,
                "status": status_item.status,
                "submitter": status_item.submitter,
            }
        },
    )
    update_result = service_context.ingest_jobs.update_one(
        {"id": job_id}, {"$push": {"status_history": status_item.dict()}}
    )
    return update_result.modified_count == 1


def poll_for_new_jobs(
    sleep_interval,
    scicat_baseurl,
    scicat_user,
    scicat_password,
    terminate_requested,
    thumbs_root=None,
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
                logger.info(
                    f"ingesting path: {job.document_path} mapping: {job.mapping_id}"
                )
                ingest(
                    "system",
                    job_list[-1],
                    thumbs_root,
                    scicat_baseurl,
                    scicat_user,
                    scicat_password,
                )
        except Exception as e:
            logger.exception("polling thread exception", e)


def ingest(
    submitter: str,
    job: Job,
    thumbs_root=None,
    scicat_baseurl=None,
    scicat_user=None,
    scicat_password=None,
) -> str:
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
            logger.info(
                f"{job.id} on document {job.document_path} already started, exiting."
            )
            return

        set_job_status(
            job.id,
            StatusItem(
                time=datetime.utcnow(),
                submitter=job.submitter,
                status=JobStatus.running,
                log="Starting job",
            ),
        )

        issues = []
        ingestor_module = ingestor_modules.get(job.mapping_id)
        if not ingestor_module:
            issues.append(
                Issue(
                    severity=Severity.error,
                    msg=f"mapping is not configured {job.document_path} and mapping {job.mapping_id}",
                )
            )
            logger.warn(
                f"ingest job {job.document_path} and mapping {job.mapping_id} failed, \
                          mapping is not configured"
            )

        if job.mapping_id in ingestor_modules:
            logger.info(f"{job.id} scicat ingestion starting")
            scicat_client = from_credentials(
                scicat_baseurl, scicat_user, scicat_password
            )
            dataset_id = ingestor_module.ingest(
                scicat_client, scicat_user, job.document_path, Path(thumbs_root), issues
            )
            logger.info(f"ingested {dataset_id}")

        job_log = f"ingested dataset: {job.document_path} as {dataset_id}"
        if issues and len(issues) > 0:
            status = JobStatus.complete_with_issues
            for issue in issues:
                if issue.severity == Severity.error:
                    status = JobStatus.error
                job_log += f"\n :  {issue.msg}"
                if issue.exception:
                    job_log += f"\n    Exception: {issue.exception}"
            status = StatusItem(
                time=datetime.utcnow(),
                status=status,
                submitter=submitter,
                log=job_log,
                issues=issues,
            )
        else:
            status = StatusItem(
                time=datetime.utcnow(),
                status=JobStatus.successful,
                submitter=submitter,
                log=job_log,
            )
        set_job_status(job.id, status)
        return dataset_id

    except Exception:
        exc_type, exc_value, exc_tb = sys.exc_info()
        job_log = traceback.format_exception(exc_type, exc_value, exc_tb)
        status = StatusItem(
            time=datetime.utcnow(),
            status=JobStatus.error,
            submitter=submitter,
            log=str(job_log),
        )
        set_job_status(job.id, status)


def sample_event_page(event_page, sample_size=10):
    df = pd.DataFrame(data=event_page["data"])
    df = df.rename(columns=lambda s: s.replace(":", "/"))
    if len(df) == 0:
        return {}
    step = len(df) // sample_size
    if step == 0:
        step == 1
    return json.loads(df[0::step].to_json())
