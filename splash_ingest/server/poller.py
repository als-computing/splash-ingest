import logging

from pymongo import MongoClient
from starlette.config import Config

from splash_ingest.server.ingest_service import init_ingest_service, poll_for_new_jobs

config = Config(".env")
DATABROKER_DB_URI = config("DATABROKER_DB_URI", cast=str, default="mongodb://localhost:27017/databroker")
DATABROKER_DB_NAME = config("DATABROKER_DB_NAME", cast=str, default="databroker")
INGEST_DB_URI = config("INGEST_DB_URI", cast=str, default="mongodb://localhost:27017/ingest")
INGEST_DB_NAME = config("INGEST_DB_NAME", cast=str, default="ingest")
INGEST_LOG_LEVEL = config("SPLASH_LOG_LEVEL", cast=str, default="INFO")
POLLER_MAX_THREADS = config("POLLER_MAX_THREADS", cast=int, default=1)
POLLER_SLEEP_SECONDS = config("POLLER_SLEEP_SECONDS", cast=int, default=5)
THUMBS_ROOT = config("THUMBS_ROOT", cast=str, default="thumbs")
SCICAT_BASEURL = config("SCICAT_BASEURL", cast=str, default="http://localhost:3000/api/v3")
SCICAT_INGEST_USER = config("SCICAT_INGEST_USER", cast=str, default="ingest")
SCICAT_INGEST_PASSWORD = config("SCICAT_INGEST_PASSWORD", cast=str, default="aman")
logger = logging.getLogger(__name__)


def init_logging():
    ch = logging.StreamHandler()
    ch.setLevel(INGEST_LOG_LEVEL)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    logger.setLevel(INGEST_LOG_LEVEL)


init_logging()
logger = logging.getLogger(__name__)

logger.info("starting poller")
logger.info(f"DATABROKER_DB_URI {DATABROKER_DB_URI}")
logger.info(f"DATABROKER_DB_NAME {DATABROKER_DB_NAME}")
logger.info(f"INGEST_DB_URI {INGEST_DB_URI}")
logger.info(f"INGEST_DB_NAME {INGEST_DB_NAME}")
logger.info(f"INGEST_LOG_LEVEL {INGEST_LOG_LEVEL}")
logger.info(f"POLLER_MAX_THREADS {POLLER_MAX_THREADS}")
logger.info(f"POLLER_SLEEP_SECONDS {POLLER_SLEEP_SECONDS}")
logger.info(f"THUMBS_ROOT {THUMBS_ROOT}")
logger.info(f"SCICAT_BASEURL {SCICAT_BASEURL}")
logger.info(f"SCICAT_INGEST_USER {SCICAT_INGEST_USER}")
logger.info("SCICAT_INGEST_PASSWORD ...")
databroker_db = MongoClient(DATABROKER_DB_URI)[DATABROKER_DB_NAME]
ingest_db = MongoClient(INGEST_DB_URI)[INGEST_DB_NAME]

init_ingest_service(ingest_db, databroker_db)
poll_for_new_jobs(
    POLLER_SLEEP_SECONDS,
    SCICAT_BASEURL,
    SCICAT_INGEST_USER,
    SCICAT_INGEST_PASSWORD,
    THUMBS_ROOT
)
