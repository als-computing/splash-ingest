import pytest
from mongomock import MongoClient
from ..auth_service import init_api_service, create_api_key, get_stored_api_key, get_api_keys


@pytest.fixture(scope="session", autouse=True)
def init_mongomock():
    init_api_service(MongoClient().ingest_db)


def test_key():
    # init(MongoClient().ingest_db)
    create_api_key('user1', 'sirius_cybernetics_gpp', 'door_operation')
    key = create_api_key('user1', 'sirius_cybernetics_gpp', 'infinite_probability_drive')
    api_client_key = get_stored_api_key('user1', key)
    assert api_client_key.client == 'sirius_cybernetics_gpp'
    assert api_client_key.api == 'infinite_probability_drive'
    assert api_client_key.key == key
    keys = get_api_keys('user1')
    sirius_keys = []
    [sirius_keys.append(key) for key in keys if key.client == 'sirius_cybernetics_gpp']
    assert len(sirius_keys) == 2


def test_non_existent_key():
    api_client_key = get_stored_api_key('user1', "foobar")
    assert api_client_key is None
    keys = get_api_keys('user1')
    print(f"!!!!!!!!!{keys}")
    assert len(keys) > 0
