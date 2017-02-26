import datetime
from random import choice
from string import ascii_lowercase

from nameko.rpc import rpc
import pymonetdb
from nameko_mongodb.database import MongoDatabase

from application.dependencies.monetdb import MonetDbConnection

TYPES = ('VARCHAR', 'BOOLEAN', 'INTEGER', 'FLOAT', 'BIGINT', 'DATE', 'TIMESTAMP', 'TIME', 'JSON',)


class DatastoreService(object):
    name = 'datastore_service'

    connection = MonetDbConnection()

    database = MongoDatabase()

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

    @rpc
    def insert(self, target_table, records, meta, is_partitionned=False, partition_keys=None):
        if is_partitionned and not partition_keys:
            raise ValueError('If is_partitionned is true partition_keys has to be set')

        cursor = self.connection.cursor()

        try:
            cursor.execute('SELECT 1 FROM {table}'.format(table=target_table))
        except pymonetdb.exceptions.OperationalError:
            self.connection.rollback()
            self._create_table(target_table, meta, is_partitionned, partition_keys)
            pass

        partition_cache = {}

        try:
            for row in records:
                working_table = target_table
                if is_partitionned:
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

        table = self.database.tables.find_one({'table': target_table})

        is_partitionned = table['is_merge_table']
        partition_keys = table['keys']

        if is_partitionned:
            if len(set(k for k in delete_keys).intersection(set(partition_keys))) != len(partition_keys):
                raise NotImplementedError('Only supporting delete on partition keys')

            current_partition_name = self._get_partition_name(target_table, delete_keys)
            self._drop_partition(target_table, current_partition_name, delete_keys)
        else:
            cursor = self.connection.cursor()

            if len(delete_keys.keys()) > 1:
                raise NotImplementedError('Not supporting delete on multiple keys on non partitionned table')

            column = list(delete_keys.keys())[0]

            try:
                cursor.execute('DELETE FROM {table} WHERE {column} = %s'.format(table=target_table, column=column),
                               list(delete_keys.values()))
                self.connection.commit()
            except pymonetdb.exceptions.Error:
                self.connection.rollback()
                raise

    @rpc
    def update(self, target_table, update_keys, updated_records):
        table = self.database.tables.find_one({'table': target_table})

        is_partitionned = table['is_merge_table']
        partition_keys = table['keys']

        if is_partitionned:
            if len(set(k for k in update_keys).intersection(set(partition_keys))) != len(partition_keys):
                raise NotImplementedError('Only supporting update on partition keys')

            if len(set(r for r in updated_records).intersection(set(partition_keys))) != 0:
                raise NotImplementedError('Can not update partition keys')

            working_table = self._get_partition_name(target_table, update_keys)

            cursor = self.connection.cursor()

            try:
                cursor.execute(
                    'UPDATE {table} SET {columns}'.format(table=working_table,
                                                          columns=','.join(k + '=%s' for k in updated_records)),
                    list(updated_records.values()))
            except pymonetdb.exceptions.Error:
                self.connection.rollback()
                raise
        else:
            if len(update_keys.keys()) > 1:
                raise NotImplementedError('Not supporting update on multiple keys on non partitionned table')

            cursor = self.connection.cursor()

            try:
                params = list(updated_records.values())
                params.append(list(update_keys.values())[0])
                columns = ','.join(k + ' = %s' for k in updated_records)
                key = list(update_keys.keys())[0]
                cursor.execute(
                    'UPDATE {table} SET {columns} WHERE {update_key} = %s'.format(table=target_table,
                                                                                  columns=columns,
                                                                                  update_key=key)
                    , params)
            except pymonetdb.exceptions.Error:
                self.connection.rollback()
                raise

        self.connection.commit()

    @rpc
    def upsert(self, target_table, upsert_keys, records, meta, is_partionned=False, partition_keys=None):
        pass

    @rpc
    def bulk_insert(self, target_table, records, meta, is_partionned=False, partition_keys=None):
        pass
