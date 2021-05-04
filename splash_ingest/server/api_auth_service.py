from dataclasses import dataclass
from datetime import datetime
import logging
from logging import getLogger
from threading import Thread, ThreadError
from typing import List
from uuid import uuid4


from pydantic import BaseModel, Field, parse_obj_as
from pymongo import MongoClient
from pymongo.collection import Collection
from passlib.hash import pbkdf2_sha256

logger = logging.getLogger('splash_ingest')


@dataclass
class ServiceContext():
    db: MongoClient = None
    api_client_collection: Collection = None


context = ServiceContext()


def init_api_service(db: MongoClient):
    context.db = db
    context.api_client_collection = db['api_clients']


class APIClient(BaseModel):
    hashed_key: str = Field(description="API key that can be given to a client")
    client: str = Field(description="Name of client who key is given to")
    api: str = Field(description="Name of API that key gives access to")


def create_api_client(submitter: str, client: str, api: str) -> str:
    try:
        key = str(uuid4())
        hashed_key = pbkdf2_sha256.hash(key)
        client_key = APIClient(hashed_key=hashed_key, client=client, api=api)
        context.api_client_collection.insert_one(client_key.dict())
        return key
    except Exception as e:
        logging.error(e)
        raise e


def verify_api_key(key):
    for api_client in get_api_clients('sys'):  # looping through all clients is inefficient, but list issmall
        if pbkdf2_sha256.verify(key, api_client.hashed_key):
            return api_client
    return None


def get_api_clients(submitter: str) -> List[APIClient]:
    try:
        keys = list(context.api_client_collection.find())
        return parse_obj_as(List[APIClient], keys)
    except Exception as e:
        logging.error(e)
        raise e
