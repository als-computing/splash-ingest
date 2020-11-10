import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from pymongo import MongoClient
from starlette.config import Config

from splash_ingest_manager.ingest_service import init_ingest_service, poll_for_new_jobs

config = Config(".env")
MONGO_DB_URI = config("MONGO_DB_URI", cast=str, default="mongodb://localhost:27017/splash")
SPLASH_DB_NAME = config("SPLASH_DB_NAME", cast=str, default="splash")
SPLASH_LOG_LEVEL = config("SPLASH_LOG_LEVEL", cast=str, default="INFO")
POLLER_MAX_THREADS = config("POLLER_MAX_THREADS", cast=int, default=1)
POLLER_SLEEP_SECONDS = config("POLLER_SLEEP_SECONDS", cast=int, default=5)
logger = logging.getLogger('splash_ingest')


def init_logging():
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    logger.setLevel(SPLASH_LOG_LEVEL)


init_logging()

logger.info("starting poller")
db = MongoClient(MONGO_DB_URI)[SPLASH_DB_NAME]
init_ingest_service(db)
poll_for_new_jobs(POLLER_SLEEP_SECONDS)
