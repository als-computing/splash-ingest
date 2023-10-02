from datetime import datetime
from enum import Enum

from typing import List, Optional

from pydantic import BaseModel

from splash_ingest.ingestors.utils import Issue


class RevisionStamp(BaseModel):
    user: str
    time: datetime
    version_id: str


class IngestType(str, Enum):
    databroker = "databroker"
    scicat = "scicat"


class JobStatus(str, Enum):
    submitted = "submitted"
    running = "running"
    complete_with_issues = "complete_with_issues"
    successful = "successful"
    error = "error"


class StatusItem(BaseModel):
    time: datetime
    status: JobStatus
    log: Optional[str] = None
    submitter: str
    issues: Optional[List[Issue]] = None


class Job(BaseModel):
    id: Optional[str] = None
    submit_time: Optional[datetime] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    document_path: str
    status: JobStatus = None
    mapping_id: Optional[str] = None
    submitter: Optional[str] = None
    status_history: Optional[List[StatusItem]] = []
    ingest_types: Optional[List[IngestType]] = None


class Entity(BaseModel):
    uid: str
    name: str
    org: str
    hashed_pw: str
