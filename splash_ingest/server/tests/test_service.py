from pathlib import Path

from mongomock import MongoClient

from ..ingest_service import init_ingest_service, reader_modules


def test_ingest_service_init_readers():
    # test the loading of reader modules
    db = MongoClient().db
    readers_dir = Path(Path(__file__).parent, "readers")
    init_ingest_service(db, readers_dir)
    assert (
        len(reader_modules) == 1
    ), "duplicate specs in reader modules, only one should be loaded"
    reader_module = reader_modules["reader"]
    assert (
        reader_module.ingest(None, "test_reader_1") == "test_reader_1"
    ), "reader1:ingest successfully called"
