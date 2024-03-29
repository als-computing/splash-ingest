import logging
import signal

# import threading

from pymongo import MongoClient
from starlette.config import Config

from splash_ingest.server.ingest_service import init_ingest_service, poll_for_new_jobs

config = Config(".env")
INGEST_DB_URI = config(
    "INGEST_DB_URI", cast=str, default="mongodb://localhost:27017/ingest"
)
INGEST_DB_NAME = config("INGEST_DB_NAME", cast=str, default="ingest")
INGEST_LOG_LEVEL = config("INGEST_LOG_LEVEL", cast=str, default="INFO")
POLLER_MAX_THREADS = config("POLLER_MAX_THREADS", cast=int, default=1)
POLLER_SLEEP_SECONDS = config("POLLER_SLEEP_SECONDS", cast=int, default=5)
THUMBS_ROOT = config("THUMBS_ROOT", cast=str, default="thumbs")
SCICAT_BASEURL = config(
    "SCICAT_BASEURL", cast=str, default="http://localhost:3000/api/v3"
)
SCICAT_INGEST_USER = config("SCICAT_INGEST_USER", cast=str, default="ingest")
SCICAT_INGEST_PASSWORD = config("SCICAT_INGEST_PASSWORD", cast=str, default="aman")

logger = logging.getLogger("splash_ingest")


def init_logging():
    logger.setLevel(INGEST_LOG_LEVEL)
    ch = logging.StreamHandler()
    ch.setLevel(INGEST_LOG_LEVEL)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)
    logger.handlers.clear()
    logger.addHandler(ch)


init_logging()


logger.info("starting poller")
logger.info(f"INGEST_DB_URI {INGEST_DB_URI}")
logger.info(f"INGEST_DB_NAME {INGEST_DB_NAME}")
logger.info(f"INGEST_LOG_LEVEL {INGEST_LOG_LEVEL}")
logger.info(f"POLLER_MAX_THREADS {POLLER_MAX_THREADS}")
logger.info(f"POLLER_SLEEP_SECONDS {POLLER_SLEEP_SECONDS}")
logger.info(f"THUMBS_ROOT {THUMBS_ROOT}")
logger.info(f"SCICAT_BASEURL {SCICAT_BASEURL}")
logger.info(f"SCICAT_INGEST_USER {SCICAT_INGEST_USER}")
logger.info("SCICAT_INGEST_PASSWORD ...")
ingest_db = MongoClient(INGEST_DB_URI)[INGEST_DB_NAME]

init_ingest_service(ingest_db)


class TerminateRequested:
    state = False


terminate_requested = TerminateRequested


def sigterm_handler(signum, frame):
    logger.info(f"sig terminate received: {signum}")
    terminate_requested.state = True


signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigterm_handler)

# ingest_thread = threading.Thread(target=poll_for_new_jobs, args=(
#     POLLER_SLEEP_SECONDS,
#     SCICAT_BASEURL,
#     SCICAT_INGEST_USER,
#     SCICAT_INGEST_PASSWORD,
#     terminate_requested,
#     THUMBS_ROOT
# ))

# logger.info("starting polling thread")
# ingest_thread.start()
# ingest_thread.join()
poll_for_new_jobs(
    POLLER_SLEEP_SECONDS,
    SCICAT_BASEURL,
    SCICAT_INGEST_USER,
    SCICAT_INGEST_PASSWORD,
    terminate_requested,
    THUMBS_ROOT,
)
