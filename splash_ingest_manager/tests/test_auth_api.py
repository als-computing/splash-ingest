import pytest
from mongomock import MongoClient
from ..api_auth_service import init_api_service, create_api_client, verify_api_key, get_api_clients


@pytest.fixture(scope="session", autouse=True)
def init_mongomock():
    init_api_service(MongoClient().ingest_db)


def test_key():
    # init(MongoClient().ingest_db)
    create_api_client('user1', 'sirius_cybernetics_gpp', 'door_operation')
    key = create_api_client('user1', 'sirius_cybernetics_gpp', 'infinite_probability_drive')
    api_client_key = verify_api_key(key)
    assert api_client_key.client == 'sirius_cybernetics_gpp'
    assert api_client_key.api == 'infinite_probability_drive'
    api_clients = get_api_clients('user1')
    sirius_keys = []
    [sirius_keys.append(key) for api_client in api_clients if api_client.client == 'sirius_cybernetics_gpp']
    assert len(sirius_keys) == 2


def test_non_existent_key():
    create_api_client('user1', 'sirius_cybernetics_gpp', 'door_operation')
    create_api_client('user1', 'sirius_cybernetics_gpp', 'infinite_probability_drive')
    api_client_key = verify_api_key("foobar")
    assert api_client_key is None
    clients = get_api_clients('user1')
    assert len(clients) > 0
