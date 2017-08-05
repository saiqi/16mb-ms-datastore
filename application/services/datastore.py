from nameko.rpc import rpc
import pymonetdb
from bson.json_util import loads

from application.dependencies.monetdb import MonetDbConnection


class DatastoreService(object):
    name = 'datastore'

    connection = MonetDbConnection()

    def _create_table(self, table_name, meta, query=None, params=None):

        if meta is not None:
            columns = ','.join('{name} {type}'.format(name=name, type=data_type) for name, data_type in meta)

        cursor = self.connection.cursor()

        try:
            if query is None:
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

    @staticmethod
    def _handle_records(records):
        if isinstance(records, str):
            converted = loads(records)
            if isinstance(converted, list):
                return converted
            else:
                return [converted]
        return records

    @rpc
    def insert_from_select(self, target_table, query, params):
        cursor = self.connection.cursor()

        try:
            cursor.execute('SELECT 1 FROM {}'.format(target_table))
        except pymonetdb.exceptions.OperationalError:
            self._create_table(target_table, None, query, params)
            pass

        try:
            if params is None:
                cursor.execute('INSERT INTO {table} {query}'.format(table=target_table, query=query))
            else:
                cursor.execute('INSERT INTO {table} {query}'.format(table=target_table, query=query), params)
        finally:
            cursor.close()

    @rpc
    def insert(self, target_table, records, meta):
        cursor = self.connection.cursor()

        try:
            cursor.execute('SELECT 1 FROM {table}'.format(table=target_table))
        except pymonetdb.exceptions.OperationalError:
            self._create_table(target_table, meta)
            pass

        try:
            for row in self._handle_records(records):
                cursor.execute(
                    'INSERT INTO {table} ({columns}) VALUES ({records})'.format(table=target_table,
                                                                                columns=','.join(k for k in row),
                                                                                records=','.join(['%s'] * len(row))),
                    list(row.values()))
        finally:
            cursor.close()

    @rpc
    def delete(self, target_table, delete_keys):
        records = self._handle_records(delete_keys)

        table_exists = True

        cursor = self.connection.cursor()

        try:
            cursor.execute('SELECT 1 FROM {}'.format(target_table))
        except pymonetdb.exceptions.OperationalError:
            table_exists = False
            pass

        if table_exists:
            if len(records.keys()) > 1:
                raise NotImplementedError('Not supporting delete on multiple keys')

            column = list(records.keys())[0]

            try:
                cursor.execute('DELETE FROM {table} WHERE {column} = %s'.format(table=target_table, column=column),
                               list(records.values()))
            finally:
                cursor.close()

    @rpc
    def truncate(self, target_table):
        table_exists = True

        cursor = self.connection.cursor()

        try:
            cursor.execute('SELECT 1 FROM {}'.format(target_table))
        except pymonetdb.exceptions.OperationalError:
            table_exists = False
            pass

        if table_exists:
            try:
                cursor.execute('DELETE FROM {}'.format(target_table))
            finally:
                cursor.close()

    @rpc
    def update(self, target_table, update_key, updated_records):

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

    @rpc
    def upsert(self, target_table, upsert_key, records, meta):

        cursor = self.connection.cursor()

        try:
            cursor.execute('SELECT 1 FROM {table}'.format(table=target_table))
        except pymonetdb.exceptions.OperationalError:
            self._create_table(target_table, meta)
            pass

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

    @rpc
    def bulk_insert(self, target_table, records, meta, mapping=None):
        cursor = self.connection.cursor()

        try:
            cursor.execute('SELECT 1 FROM {table}'.format(table=target_table))
        except pymonetdb.exceptions.OperationalError:
            self._create_table(target_table, meta)
            pass

        string_records = list()
        n = 0
        for r in self._handle_records(records):
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

    @rpc
    def create_or_replace_view(self, view_name, query, params):
        cursor = self.connection.cursor()
        existed = True
        try:
            cursor.execute('SELECT 1 FROM {} LIMIT 1'.format(view_name))
        except pymonetdb.exceptions.OperationalError:
            existed = False
            pass

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
        cursor = self.connection.cursor()

        is_function_exists = self.check_if_function_exists(name)

        try:
            if is_function_exists:
                cursor.execute('DROP FUNCTION {}'.format(name))

            cursor.execute(script)
        finally:
            cursor.close()
