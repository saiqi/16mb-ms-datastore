from weakref import WeakKeyDictionary

import pymonetdb
from nameko.extensions import DependencyProvider


class MonetDbConnection(DependencyProvider):

    def __init__(self):
        self.connection = None
        self.connections = WeakKeyDictionary()

    def setup(self):
        self.connection = pymonetdb.connect(hostname=self.container.config['MONETDB_HOST'],
                                            username=self.container.config['MONETDB_USER'],
                                            password=self.container.config['MONETDB_PASSWORD'],
                                            database=self.container.config['MONETDB_DATABASE'],
                                            autocommit=False)

    def stop(self):
        self.connection.close()
        del self.connection

    def get_dependency(self, worker_ctx):
        self.connections[worker_ctx] = self.connection

        return self.connection

    def worker_teardown(self, worker_ctx):
        self.connections.pop(worker_ctx)
