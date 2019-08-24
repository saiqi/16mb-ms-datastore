import logging
from logging import getLogger
import time
from nameko.rpc import rpc
from nameko.dependency_providers import DependencyProvider
import pymonetdb
import pymonetdb.exceptions
from bson.json_util import loads

from application.dependencies.monetdb import MonetDbConnection

logging.getLogger('pymonetdb').setLevel(logging.ERROR)
_log = getLogger(__name__)

class ErrorHandler(DependencyProvider):

    def worker_result(self, worker_ctx, res, exc_info):
        if exc_info is None:
            return

        exc_type, exc, tb = exc_info
        _log.error(str(exc))


class DatastoreService(object):
    name = 'datastore'
    error = ErrorHandler()
    connection = MonetDbConnection()

    def _create_table(self, table_name, meta, is_merge_table=False, query=None, params=None):
        _log.info('Creating table {} table_name'.format(table_name))
        if meta is not None:
            columns = ','.join('{name} {type}'.format(name=name, type=data_type) for name, data_type in meta)

        cursor = self.connection.cursor()

        try:
            if query is None:
                if is_merge_table:
                    self.connection.execute(
                        'CREATE MERGE TABLE {table} ({columns})'.format(table=table_name, columns=columns))
                else:
                    self.connection.execute(
                        'CREATE TABLE {table} ({columns})'.format(table=table_name, columns=columns))
            else:
                if params is None:
                    self.connection.execute(
                        'CREATE TABLE {table} AS {query} WITH NO DATA'.format(table=table_name, query=query))
                else:
                    cursor.execute(
                        'CREATE TABLE {table} AS {query} WITH NO DATA'.format(table=table_name, query=query),
                        params)
        finally:
            cursor.close()

    def _drop_table(self, table_name):
        self.connection.execute('DROP TABLE {table}'.format(table=table_name))

    def _check_if_table_exists(self, table_name):
        cursor = self.connection.cursor()
        table_exists = True
        try:
            cursor.execute('SELECT 1 FROM {} LIMIT 1'.format(table_name))
        except pymonetdb.exceptions.OperationalError:
            table_exists = False
            pass
        finally:
            cursor.close()

        return table_exists

    @staticmethod
    def _handle_records(records):
        if isinstance(records, str):
            converted = loads(records)
            if isinstance(converted, list):
                return converted
            else:
                return [converted]
        return records

    @staticmethod
    def _chunk_records(l, n):
        for i in range(0, len(l), n):
            yield l[i:i+n]

    @rpc
    def add_partition(self, target_table, merge_table, meta):
        _log.info('Adding partition on  table {}'.format(merge_table))
        table_exists = self._check_if_table_exists(merge_table)

        if table_exists is False:
            self._create_table(merge_table, meta, True)

        self.connection.execute('ALTER TABLE {} ADD TABLE {}'.format(merge_table, target_table))

    @rpc
    def drop_partition(self, target_table, merge_table):
        _log.info('Dropping partition on  table {}'.format(merge_table))
        table_exists = self._check_if_table_exists(merge_table)
        partition_exists = self._check_if_table_exists(target_table)

        if table_exists is True and partition_exists is True:
            self.connection.execute('ALTER TABLE {} DROP TABLE {}'.format(merge_table, target_table))

    @rpc
    def insert_from_select(self, target_table, query, params):
        _log.info('Inserting data into {} from select'.format(target_table))
        table_exists = self._check_if_table_exists(target_table)

        if table_exists is False:
            self._create_table(target_table, None, False, query, params)

        cursor = self.connection.cursor()

        try:
            if params is None:
                cursor.execute('INSERT INTO {table} {query}'.format(table=target_table, query=query))
            else:
                cursor.execute('INSERT INTO {table} {query}'.format(table=target_table, query=query), params)
        finally:
            cursor.close()
        _log.info('Success !')

    @rpc
    def insert(self, target_table, records, meta):
        _log.info('Inserting records into {}'.format(target_table))
        table_exists = self._check_if_table_exists(target_table)

        if table_exists is False:
            self._create_table(target_table, meta)

        cursor = self.connection.cursor()

        try:
            for row in self._handle_records(records):
                cursor.execute(
                    'INSERT INTO {table} ({columns}) VALUES ({records})'.format(table=target_table,
                                                                                columns=','.join(k for k in row),
                                                                                records=','.join(['%s'] * len(row))),
                    list(row.values()))
        finally:
            cursor.close()
        _log.info('Success !')

    @rpc
    def delete(self, target_table, delete_keys):
        _log.info('Deleting records into {}'.format(target_table))
        records = self._handle_records(delete_keys)

        table_exists = self._check_if_table_exists(target_table)

        cursor = self.connection.cursor()

        if table_exists:
            if len(records.keys()) > 1:
                raise NotImplementedError('Not supporting delete on multiple keys')

            column = list(records.keys())[0]

            try:
                cursor.execute('DELETE FROM {table} WHERE {column} = %s'.format(table=target_table, column=column),
                               list(records.values()))
            finally:
                cursor.close()
        _log.info('Success !')

    @rpc
    def truncate(self, target_table):
        _log.info('Truncating records into {}'.format(target_table))
        table_exists = self._check_if_table_exists(target_table)

        cursor = self.connection.cursor()

        if table_exists:
            try:
                cursor.execute('DELETE FROM {}'.format(target_table))
            finally:
                cursor.close()
        _log.info('Success !')

    @rpc
    def update(self, target_table, update_key, updated_records):
        _log.info('Updating records into {}'.format(target_table))
        cursor = self.connection.cursor()

        try:
            for row in self._handle_records(updated_records):
                params = list(row.values())
                params.append(row[update_key])
                columns = ','.join(k + ' = %s' for k in row)
                cursor.execute(
                    'UPDATE {table} SET {columns} WHERE {update_key} = %s'.format(table=target_table,
                                                                                  columns=columns,
                                                                                  update_key=update_key)
                    , params)
        finally:
            cursor.close()
        _log.info('Success !')

    @rpc
    def upsert(self, target_table, upsert_key, records, meta):
        _log.info('Upserting records into {}'.format(target_table))
        table_exists = self._check_if_table_exists(target_table)

        if table_exists is False:
            self._create_table(target_table, meta)

        cursor = self.connection.cursor()

        try:
            for row in self._handle_records(records):
                n = cursor.execute('SELECT 1 FROM {table} WHERE {upsert_key} = %s'.format(table=target_table,
                                                                                          upsert_key=upsert_key),
                                   [row[upsert_key]])
                if n > 0:
                    params = list(row.values())
                    params.append(row[upsert_key])
                    columns = ','.join(k + ' = %s' for k in row)
                    cursor.execute(
                        'UPDATE {table} SET {columns} WHERE {upsert_key} = %s'.format(table=target_table,
                                                                                      columns=columns,
                                                                                      upsert_key=upsert_key)
                        , params)
                else:
                    cursor.execute(
                        'INSERT INTO {table} ({columns}) VALUES ({records})'.format(table=target_table,
                                                                                    columns=','.join(
                                                                                        k for k in row),
                                                                                    records=','.join(
                                                                                        ['%s'] * len(row))),
                        list(row.values()))
        finally:
            cursor.close()
        _log.info('Success !')

    @rpc
    def bulk_insert(self, target_table, records, meta, mapping=None, chunk_size=2500):
        _log.info('Bulk inserting records into {}'.format(target_table))
        table_exists = self._check_if_table_exists(target_table)
        if table_exists is False:
            self._create_table(target_table, meta)

        clean_records = self._handle_records(records)

        for chunk in self._chunk_records(clean_records, chunk_size):
            _log.info('Processing a {} chunk'.format(str(chunk_size)))
            string_records = list()
            n = 0
            for r in chunk:
                ordered_record = list()
                for m in meta:
                    if mapping is None:
                        key = m[0]
                    else:
                        key = mapping[m[0]]
                    ordered_record.append('' if r[key] is None else str(r[key]))
                string_records.append('|'.join(ordered_record))
                n += 1

            data = '\n'.join(string_records)

            cmd = 'sCOPY {n} RECORDS INTO {table} FROM STDIN NULL AS \'\';{data}\n'.format(n=n, table=target_table,
                                                                                           data=data)
            self.connection.command(cmd)
        _log.info('Success !')

    @rpc
    def create_or_replace_view(self, view_name, query, params=None):
        _log.info('Creating view {}'.format(view_name))
        cursor = self.connection.cursor()
        existed = self._check_if_table_exists(view_name)

        try:
            if existed is True:
                cursor.execute('DROP VIEW {}'.format(view_name))

            if params is not None:
                cursor.execute('CREATE VIEW {} AS {}'.format(view_name, query), params)
            else:
                cursor.execute('CREATE VIEW {} AS {}'.format(view_name, query))
        finally:
            cursor.close()

    @rpc
    def check_if_function_exists(self, name):
        cursor = self.connection.cursor()

        exists = False

        try:
            cursor.execute('SELECT COUNT(*) FROM SYS.FUNCTIONS WHERE NAME = %s', [name])

            n = cursor.fetchone()[0]

            if n != 0:
                exists = True
        finally:
            cursor.close()

        return exists

    @rpc
    def create_or_replace_python_function(self, name, script):
        _log.info('Creating python function into {}'.format(name))
        cursor = self.connection.cursor()

        is_function_exists = self.check_if_function_exists(name)

        try:
            if is_function_exists:
                cursor.execute('DROP FUNCTION {}'.format(name))

            cursor.execute(script)
        finally:
            cursor.close()
