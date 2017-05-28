from weakref import WeakKeyDictionary
from queue import Queue

import pymonetdb
from nameko.extensions import DependencyProvider


class MonetDbConnection(DependencyProvider):

    def __init__(self):
        self.connection_pool = None
        self.connections = WeakKeyDictionary()

    def _get_connection(self):
        conn = pymonetdb.connect(hostname=self.container.config['MONETDB_HOST'],
                                            username=self.container.config['MONETDB_USER'],
                                            password=self.container.config['MONETDB_PASSWORD'],
                                            database=self.container.config['MONETDB_DATABASE'],
                                            autocommit=True)

        return conn

    def setup(self):

        self.maxsize = self.container.config['max_workers']
        self.connection_pool = Queue(maxsize=self.maxsize)

        for c in range(self.maxsize):
            self.connection_pool.put(self._get_connection())

    def stop(self):
        for c in range(self.maxsize):
            conn = self.connection_pool.get()
            del conn
            self.connection_pool.put(None)

        del self.connection_pool

    def get_dependency(self, worker_ctx):
        connection = self.connection_pool.get()

        try:
            connection.execute('SELECT 1')
        except pymonetdb.exceptions.OperationalError:
            connection = self._get_connection()
            pass

        self.connections[worker_ctx] = connection

        return self.connections[worker_ctx]

    def worker_teardown(self, worker_ctx):
        connection = self.connections.pop(worker_ctx)
        self.connection_pool.put(connection)
