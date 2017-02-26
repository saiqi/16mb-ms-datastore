import eventlet
eventlet.monkey_patch()

import pytest
from nameko.containers import ServiceContainer


def pytest_addoption(parser):
    parser.addoption('--test-db-host', action='store', dest='TEST_DB_HOST')
    parser.addoption('--test-db-user', action='store', dest='TEST_DB_USER')
    parser.addoption('--test-db-password', action='store', dest='TEST_DB_PASSWORD')
    parser.addoption('--test-db-database', action='store', dest='TEST_DB_DATABASE')
    parser.addoption('--test-db-port', action='store', dest='TEST_DB_PORT')
    parser.addoption('--test-db-url', action='store', dest='TEST_DB_URL')


@pytest.fixture
def db_url(request):
    return request.config.getoption('TEST_DB_URL')


@pytest.fixture
def host(request):
    return request.config.getoption('TEST_DB_HOST')


@pytest.fixture
def user(request):
    return request.config.getoption('TEST_DB_USER')


@pytest.fixture
def password(request):
    return request.config.getoption('TEST_DB_PASSWORD')


@pytest.fixture
def database(request):
    return request.config.getoption('TEST_DB_DATABASE')


@pytest.fixture
def port(request):
    return request.config.getoption('TEST_DB_PORT')


@pytest.yield_fixture
def container_factory():

    all_containers = []

    def make_container(service_cls, config, worker_ctx_cls=None):
        container = ServiceContainer(service_cls, config, worker_ctx_cls)
        all_containers.append(container)
        return container

    yield make_container

    for c in all_containers:
        try:
            c.stop()
        except:
            pass
