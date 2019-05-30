import eventlet
eventlet.monkey_patch()

import os
import pytest
from nameko.containers import ServiceContainer


@pytest.fixture
def host(request):
    return os.getenv('TEST_DB_HOST')


@pytest.fixture
def user(request):
    return os.getenv('TEST_DB_USER')


@pytest.fixture
def password(request):
    return os.getenv('TEST_DB_PASSWORD')


@pytest.fixture
def database(request):
    return os.getenv('TEST_DB_DATABASE')


@pytest.fixture
def port(request):
    return os.getenv('TEST_DB_PORT')


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
