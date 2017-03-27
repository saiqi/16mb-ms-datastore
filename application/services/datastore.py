import datetime
from random import choice
from string import ascii_lowercase

from nameko.rpc import rpc
import pymonetdb
from bson.json_util import loads
from nameko_mongodb.database import MongoDatabase

from application.dependencies.monetdb import MonetDbConnection


class DatastoreService(object):
    name = 'datastore'

    connection = MonetDbConnection()

    database = MongoDatabase()

    @staticmethod
    def chunks(records, chunksize):
        for i in range(0, len(records), chunksize):
            yield records[i: i + chunksize]

    def _create_table(self, table_name, meta, is_merge_table, partition_keys):
        columns = ','.join('{name} {type}'.format(name=name, type=data_type) for name, data_type in meta)

        try:
            if is_merge_table:
                self.connection.execute('CREATE MERGE TABLE {table} ({columns})'.format(table=table_name,
                                                                                        columns=columns))
            else:
                self.connection.execute('CREATE TABLE {table} ({columns})'.format(table=table_name, columns=columns))
            self.connection.commit()
        except pymonetdb.exceptions.Error:
            self.connection.rollback()
            raise

        self.database.tables.insert_one({'table': table_name, 'created_at': datetime.datetime.now(),
                                         'is_merge_table': is_merge_table, 'keys': partition_keys})

    def _drop_table(self, table_name):
        try:
            self.connection.execute('DROP TABLE {table}'.format(table=table_name))
            self.connection.commit()
        except pymonetdb.exceptions.Error:
            self.connection.rollback()
            raise

        self.database.tables.delete_one({'table': table_name})

    def _add_partition(self, merge_table_name, partition_name, partition_values):
        try:
            self.connection.execute('ALTER TABLE {table} ADD TABLE {partition}'.format(table=merge_table_name,
                                                                                       partition=partition_name))

            self.connection.commit()
        except pymonetdb.exceptions.Error:
            self.connection.rollback()
            raise

        self.database.tables.update_one({'table': merge_table_name},
                                        {'$push': {'partitions': {'name': partition_name, 'values': partition_values}}})

    def _drop_partition(self, merge_table_name, partition_name, partition_values):
        try:
            self.connection.execute('ALTER TABLE {table} DROP TABLE {partition}'.format(table=merge_table_name,
                                                                                        partition=partition_name))
            self.connection.commit()
        except pymonetdb.exceptions.Error:
            self.connection.rollback()
            raise

        self.database.tables.update_one({'table': merge_table_name},
                                        {'$pull': {'partitions': {'name': partition_name, 'values': partition_values}}})

    def _get_partition_name(self, merge_table_name, partition_values):
        meta_table = self.database.tables.find_one({'table': merge_table_name}, {'partitions': 1, 'keys': 1})

        n = len(partition_values.keys())

        if len(meta_table['keys']) != n:
            raise ValueError('Too many or not enough keys to find the partition')

        keys = set(partition_values.keys()).intersection(set(meta_table['keys']))

        if len(keys) != n:
            raise ValueError('Mismatched partition keys')

        if 'partitions' in meta_table:
            for partition in meta_table['partitions']:
                v = [o for o in keys if partition['values'][o] == partition_values[o]]

                if len(v) > 0:
                    return partition['name']

        return None

    def _handle_records(self, records):
        if isinstance(records, str):
            return loads(records)
        return records

    @rpc
    def insert(self, target_table, records, meta, is_merge_table=False, partition_keys=None):
        if is_merge_table and not partition_keys:
            raise ValueError('If is_merge_table is true partition_keys has to be set')

        cursor = self.connection.cursor()

        try:
            cursor.execute('SELECT 1 FROM {table}'.format(table=target_table))
        except pymonetdb.exceptions.OperationalError:
            self.connection.rollback()
            self._create_table(target_table, meta, is_merge_table, partition_keys)
            pass

        partition_cache = {}

        try:
            for row in self._handle_records(records):
                working_table = target_table
                if is_merge_table:
                    partition_values = dict((k, row[k]) for k in partition_keys)
                    fingerprint = ''.join(str(row[k]) for k in partition_keys)

                    if fingerprint not in partition_cache:
                        current_partition_name = self._get_partition_name(target_table, partition_values)

                        if not current_partition_name:
                            current_partition_name = ''.join(choice(ascii_lowercase) for i in range(24))
                            self._create_table(current_partition_name, meta, False, None)
                            self._add_partition(target_table, current_partition_name, partition_values)

                        partition_cache[fingerprint] = current_partition_name

                    working_table = partition_cache[fingerprint]

                cursor.execute(
                    'INSERT INTO {table} ({columns}) VALUES ({records})'.format(table=working_table,
                                                                                columns=','.join(k for k in row),
                                                                                records=','.join(['%s'] * len(row))),
                    list(row.values()))
        except pymonetdb.exceptions.Error:
            self.connection.rollback()
            raise

        self.connection.commit()

    @rpc
    def delete(self, target_table, delete_keys):

        records = self._handle_records(delete_keys)

        table = self.database.tables.find_one({'table': target_table})

        is_merge_table = table['is_merge_table']
        partition_keys = table['keys']

        if is_merge_table:
            if len(set(k for k in records).intersection(set(partition_keys))) != len(partition_keys):
                raise NotImplementedError('Only supporting delete on partition keys')

            current_partition_name = self._get_partition_name(target_table, records)
            self._drop_partition(target_table, current_partition_name, records)
        else:
            cursor = self.connection.cursor()

            if len(records.keys()) > 1:
                raise NotImplementedError('Not supporting delete on multiple keys on non merge table')

            column = list(records.keys())[0]

            try:
                cursor.execute('DELETE FROM {table} WHERE {column} = %s'.format(table=target_table, column=column),
                               list(records.values()))
                self.connection.commit()
            except pymonetdb.exceptions.Error:
                self.connection.rollback()
                raise

    @rpc
    def update(self, target_table, update_key, updated_records):
        table = self.database.tables.find_one({'table': target_table})

        is_merge_table = table['is_merge_table']

        if is_merge_table:
            raise NotImplementedError('Update not supported for merge table please consider a delete/insert')
        else:
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
            except pymonetdb.exceptions.Error:
                self.connection.rollback()
                raise

        self.connection.commit()

    @rpc
    def upsert(self, target_table, upsert_key, records, meta):

        cursor = self.connection.cursor()

        try:
            cursor.execute('SELECT 1 FROM {table}'.format(table=target_table))
        except pymonetdb.exceptions.OperationalError:
            self.connection.rollback()
            self._create_table(target_table, meta, False, None)
            pass

        table = self.database.tables.find_one({'table': target_table})

        is_merge_table = table['is_merge_table']

        if is_merge_table:
            raise NotImplementedError('Upsert not supported for merge table please consider a delete/insert')
        else:
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
            except pymonetdb.exceptions.Error:
                self.connection.rollback()
                raise
            self.connection.commit()

    @rpc
    def bulk_insert(self, target_table, records, meta, is_merge_table=False, partition_keys=None, chunksize=100000):
        pass

    @rpc
    def select(self, query, params):
        cursor = self.connection.cursor()

        try:
            if params is not None:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
        except pymonetdb.exceptions.Error:
            self.connection.rollback()
            raise

        meta = [m for r in cursor.description for i, m in enumerate(r) if i == 0]

        data = cursor.fetchall()

        result = list()

        for r in data:
            result.append(dict(zip(meta, r)))

        return result
