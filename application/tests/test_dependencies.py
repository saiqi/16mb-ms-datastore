from weakref import WeakKeyDictionary

import pytest
from mock import Mock
from nameko.testing.services import dummy, entrypoint_hook
from nameko.containers import  WorkerContext
import pymonetdb

from application.dependencies.monetdb import MonetDbConnection


class DummyService(object):
    name = 'dummy_service'

    connection = MonetDbConnection()

    @dummy
    def insert(self):
        cursor = self.connection.cursor()

        cursor.execute('INSERT INTO TEST_TABLE (ID) VALUES (1)');

    @dummy
    def select(self):
        cursor = self.connection.cursor()

        cursor.execute('SELECT ID FROM TEST_TABLE WHERE ID = 1')

        return cursor.fetchone()

    @dummy
    def create(self):
        self.connection.execute('CREATE TABLE TEST_TABLE (ID INTEGER)')

    @dummy
    def drop(self):
        self.connection.execute('DROP TABLE TEST_TABLE')


@pytest.fixture
def config(host, user, password, database, port):
    return {
        'MONETDB_USER': user,
        'MONETDB_PASSWORD': password,
        'MONETDB_HOST': host,
        'MONETDB_DATABASE': database,
        'MONETDB_PORT': port
    }


@pytest.fixture
def container(config):
    return Mock(spec=DummyService, config=config, service_name='dummy_service')


@pytest.fixture
def connection(container):
    return MonetDbConnection().bind(container, 'connection')


def test_setup(connection):
    connection.setup()
    assert isinstance(connection.connection, pymonetdb.sql.connections.Connection)


def test_stop(connection):
    connection.setup()
    assert connection.connection

    connection.stop()
    assert not hasattr(connection, 'connection')


def test_get_dependency(connection):
    connection.setup()

    worker_ctx = Mock(spec=WorkerContext)
    conn = connection.get_dependency(worker_ctx)
    assert isinstance(conn, pymonetdb.sql.connections.Connection)
    assert connection.connections[worker_ctx] is conn


def test_multiple_workers(connection):
    connection.setup()

    worker_ctx_1 = Mock(spec=WorkerContext)
    connection_1 = connection.get_dependency(worker_ctx_1)
    assert isinstance(connection_1, pymonetdb.sql.connections.Connection)
    assert connection.connections[worker_ctx_1] is connection_1

    worker_ctx_2 = Mock(spec=WorkerContext)
    connection_2 = connection.get_dependency(worker_ctx_2)
    assert isinstance(connection_2, pymonetdb.sql.connections.Connection)
    assert connection.connections[worker_ctx_2] is connection_2

    assert connection.connections == WeakKeyDictionary({
        worker_ctx_1: connection_1,
        worker_ctx_2: connection_2
    })


def test_weakref(connection):
    connection.setup()

    worker_ctx = Mock(spec=WorkerContext)
    conn = connection.get_dependency(worker_ctx)
    assert isinstance(conn, pymonetdb.sql.connections.Connection)
    assert connection.connections[worker_ctx] is conn

    connection.worker_teardown(worker_ctx)
    assert worker_ctx not in connection.connections


def test_end_to_end(host, user, password, database, port, container_factory):
    config = {
        'MONETDB_USER': user,
        'MONETDB_PASSWORD': password,
        'MONETDB_HOST': host,
        'MONETDB_DATABASE': database,
        'MONETDB_PORT': port
    }

    container = container_factory(DummyService, config)
    container.start()

    with entrypoint_hook(container, 'create') as create:
        create()

    with entrypoint_hook(container, 'insert') as insert:
        insert()

    with entrypoint_hook(container, 'select') as select:
        res = select()
        assert 1 in res

    with entrypoint_hook(container, 'drop') as drop:
        drop()
