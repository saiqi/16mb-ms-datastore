import pytest
import pymonetdb
from pymongo import MongoClient
from nameko.testing.services import worker_factory

from application.services.datastore import DatastoreService


@pytest.fixture
def service_database(db_url):
    client = MongoClient(db_url)

    yield client['test_db']

    client.drop_database('test_db')
    client.close()


@pytest.fixture
def connection(host, user, password, database, port):
    _conn = pymonetdb.connect(username=user, hostname=host, password=password, database=database, port=port)

    yield _conn

    def clean_tables():
        cursor = _conn.cursor()

        cursor.execute('SELECT NAME FROM SYS.TABLES WHERE SYSTEM=0')

        has_cleaned = False

        for table in cursor.fetchall():
            has_cleaned = True
            try:
                _conn.execute('DROP TABLE {table}'.format(table=table[0]))
                _conn.commit()
            except pymonetdb.exceptions.Error:
                _conn.rollback()
                continue

        return has_cleaned

    max_retry = 2
    has_cleaned = True
    tries = 0

    while tries < max_retry and has_cleaned:
        has_cleaned = clean_tables()
        tries += 1

    _conn.close()


def test_insert(connection, service_database):
    service = worker_factory(DatastoreService, database=service_database, connection=connection)

    records = [{'ID': 1, 'VALUE': 'toto'}, {'ID': 2, 'VALUE': 'titi'}]
    meta = [('ID', 'INTEGER'), ('VALUE', 'VARCHAR(5)')]

    service.insert('NONPART_INSERT_TABLE', records, meta)

    result = service_database.tables.find_one({'table': 'NONPART_INSERT_TABLE'})

    assert result
    assert result['is_merge_table'] is False

    cursor = connection.cursor()
    cursor.execute('SELECT COUNT(*) FROM NONPART_INSERT_TABLE')

    assert cursor.fetchone()[0] == 2

    service.insert('PART_INSERT_TABLE', records, meta, is_merge_table=True, partition_keys=['ID'])

    result = service_database.tables.find_one({'table': 'PART_INSERT_TABLE'})

    assert result
    assert result['is_merge_table'] is True

    cursor = connection.cursor()
    cursor.execute('SELECT COUNT(*) FROM PART_INSERT_TABLE')

    assert cursor.fetchone()[0] == 2

    for partition in result['partitions']:
        cursor = connection.cursor()
        cursor.execute('SELECT ID FROM {partition}'.format(partition=partition['name']))

        assert partition['values']['ID'] == cursor.fetchone()[0]

    service.insert('PART_INSERT_TABLE', [{'ID': 2, 'VALUE': None}], meta, is_merge_table=True, partition_keys=['ID'])

    result = service_database.tables.find_one({'table': 'PART_INSERT_TABLE'})

    assert len(result['partitions']) == 2

    cursor = connection.cursor()
    cursor.execute('SELECT COUNT(*) FROM PART_INSERT_TABLE')

    assert cursor.fetchone()[0] == 3


def test_delete(connection, service_database):
    service = worker_factory(DatastoreService, database=service_database, connection=connection)

    records = [{'ID': 1, 'VALUE': 'toto'}, {'ID': 2, 'VALUE': 'titi'}]
    meta = [('ID', 'INTEGER'), ('VALUE', 'VARCHAR(5)')]

    service.insert('NONPART_DELETE_TABLE', records, meta)

    service.delete('NONPART_DELETE_TABLE', {'ID': 1})

    cursor = connection.cursor()
    cursor.execute('SELECT COUNT(*) FROM NONPART_DELETE_TABLE')

    assert cursor.fetchone()[0] == 1

    service.insert('PART_DELETE_TABLE', records, meta, True, ['ID'])

    service.delete('PART_DELETE_TABLE', {'ID': 1})

    cursor = connection.cursor()
    cursor.execute('SELECT COUNT(*) FROM PART_DELETE_TABLE')

    assert cursor.fetchone()[0] == 1

    with pytest.raises(NotImplementedError):
        service.delete('PART_DELETE_TABLE', {'VALUE': 'titi'})


def test_update(connection, service_database):
    service = worker_factory(DatastoreService, database=service_database, connection=connection)

    records = [{'ID': 1, 'VALUE': 'toto'}, {'ID': 2, 'VALUE': 'titi'}]
    meta = [('ID', 'INTEGER'), ('VALUE', 'VARCHAR(5)')]

    service.insert('NONPART_UPDATE_TABLE', records, meta)

    service.update('NONPART_UPDATE_TABLE', 'ID', [{'ID': 2, 'VALUE': 'tata'}])

    cursor = connection.cursor()
    cursor.execute('SELECT VALUE FROM NONPART_UPDATE_TABLE WHERE ID = 2')

    assert cursor.fetchone()[0] == 'tata'

    service.insert('PART_UPDATE_TABLE', records, meta, True, ['ID'])

    with pytest.raises(NotImplementedError):
        service.update('PART_UPDATE_TABLE', 'ID', {'ID': 2, 'VALUE': 'tutu'})


def test_upsert(connection, service_database):
    service = worker_factory(DatastoreService, database=service_database, connection=connection)

    records = [{'ID': 1, 'VALUE': 'toto'}, {'ID': 2, 'VALUE': 'titi'}]
    meta = [('ID', 'INTEGER'), ('VALUE', 'VARCHAR(5)')]

    service.insert('NONPART_UPSERT_TABLE', records, meta)

    service.upsert('NONPART_UPSERT_TABLE', 'ID', [{'ID': 2, 'VALUE': 'tata'}, {'ID': 3, 'VALUE': 'tutu'}], meta)

    cursor = connection.cursor()
    cursor.execute('SELECT VALUE FROM NONPART_UPSERT_TABLE WHERE ID = 2')

    assert cursor.fetchone()[0] == 'tata'

    cursor.execute('SELECT VALUE FROM NONPART_UPSERT_TABLE WHERE ID = 3')

    assert cursor.fetchone()[0] == 'tutu'

    service.insert('PART_UPSERT_TABLE', records, meta, True, ['ID'])

    with pytest.raises(NotImplementedError):
        service.upsert('PART_UPSERT_TABLE', 'ID', {'ID': 2, 'VALUE': 'tutu'}, meta)

    service.upsert('NONEXTISTED_UPSERT_TABLE', 'ID', [{'ID': 2, 'VALUE': 'tata'}, {'ID': 3, 'VALUE': 'tutu'}], meta)

    cursor = connection.cursor()
    cursor.execute('SELECT VALUE FROM NONPART_UPSERT_TABLE WHERE ID = 1')

    assert cursor.fetchone()[0] == 'toto'


def test_select(connection, service_database):
    service = worker_factory(DatastoreService, database=service_database, connection=connection)
    result = service.select('SELECT 1 AS V', None)
    assert result
    assert len(result) == 1
    assert result[0]['v'] == 1

    result = service.select('SELECT * FROM (SELECT 1 AS V UNION ALL SELECT 2 AS V) T WHERE V = %s', (2,))
    assert result
    assert len(result) == 1
    assert result[0]['v'] == 2


def test_create_or_replace_view(connection, service_database):
    service = worker_factory(DatastoreService, database=service_database, connection=connection)
    service.create_or_replace_view('MYVIEW', 'SELECT 1 AS V', None)

    cursor = connection.cursor()
    cursor.execute('SELECT * FROM MYVIEW')

    assert cursor.fetchone()[0] == 1

    service.create_or_replace_view('MYVIEW', 'SELECT 1 AS V', None)


def test_insert_from_select(connection, service_database):
    service = worker_factory(DatastoreService, database=service_database, connection=connection)

    query = 'SELECT 0 AS GROUP_ID, 1 AS ID, 15.0 AS VALUE UNION ALL SELECT 0 AS GROUP_ID, 2 AS ID, -5.0 AS VALUE'

    service.insert_from_select('PART_INSERTSELECT_TABLE', query, None, True, 'GROUP_ID', 0)

    cursor = connection.cursor()
    cursor.execute('SELECT VALUE FROM PART_INSERTSELECT_TABLE WHERE ID = 1')

    assert cursor.fetchone()[0] == 15.

    query = 'SELECT 0 AS GROUP_ID, 1 AS ID, 35.0 AS VALUE UNION ALL SELECT 0 AS GROUP_ID, 2 AS ID, -5.0 AS VALUE'

    service.insert_from_select('PART_INSERTSELECT_TABLE', query, None, True, 'GROUP_ID', 0)

    cursor = connection.cursor()
    cursor.execute('SELECT COUNT(*) FROM PART_INSERTSELECT_TABLE')

    assert cursor.fetchone()[0] == 2

    cursor.execute('SELECT VALUE FROM PART_INSERTSELECT_TABLE WHERE ID = 1')

    assert cursor.fetchone()[0] == 35.

    query = 'SELECT 0 AS GROUP_ID, 1 AS ID, 35.0 AS VALUE UNION ALL SELECT 1 AS GROUP_ID, 2 AS ID, -5.0 AS VALUE'
    with pytest.raises(ValueError):
        service.insert_from_select('PART_INSERTSELECT_TABLE', query, None, True, 'GROUP_ID', 0)

    service.insert_from_select('NONPART_INSERTSELECT_TABLE', query, None, False)

    cursor.execute('SELECT VALUE FROM NONPART_INSERTSELECT_TABLE WHERE ID = 2')

    assert cursor.fetchone()[0] == -5.

    query = 'SELECT * FROM (SELECT 0 AS GROUP_ID, 1 AS ID, 35.0 AS VALUE) T WHERE ID = %s'

    service.insert_from_select('NONPART_INSERTSELECTP_TABLE', query, [1], False)

    cursor.execute('SELECT VALUE FROM NONPART_INSERTSELECTP_TABLE WHERE ID = 1')

    assert cursor.fetchone()[0] == 35.
