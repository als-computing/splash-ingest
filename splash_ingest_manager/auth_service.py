from dataclasses import dataclass
from datetime import datetime
import logging
from threading import Thread, ThreadError
from typing import List
from uuid import uuid4


from pydantic import BaseModel, Field, parse_obj_as
from pymongo import MongoClient
from pymongo.collection import Collection


logger = logging.getLogger('splash_ingest')


@dataclass
class ServiceContext():
    db: MongoClient = None
    api_client_collection: Collection = None


context = ServiceContext()


def init_api_service(db: MongoClient):
    context.db = db
    context.api_client_collection = db['api_keys']

    context.api_client_collection.create_index(
        [
            ('key', 1),
        ],
        unique=True
    )


class APIClientKey(BaseModel):
    key: str = Field(description="API key that can be given to a client")
    client: str = Field(description="Name of client who key is given to")
    api: str = Field(description="Name of API that key gives access to")


def create_api_key(submitter: str, client: str, api: str) -> str:
    try:
        key = str(uuid4())
        client_key = APIClientKey(key=key, client=client, api=api)
        context.api_client_collection.insert_one(client_key.dict())
        return key
    except Exception as e:
        logging.error(e)
        raise e


def get_stored_api_key(submitter: str, key: str) -> APIClientKey:
    try:
        logger.info('!!!!!!!!!get_stored_api_key')
        key_dict = context.api_client_collection.find_one({"key": key})
        if key_dict is None:
            return None
        return APIClientKey(**key_dict)
    except Exception as e:
        logging.error(e)
        raise e


def get_api_keys(submitter: str) -> List[APIClientKey]:
    try:
        keys = list(context.api_client_collection.find())
        return parse_obj_as(List[APIClientKey], keys)
    except Exception as e:
        logging.error(e)
        raise e
