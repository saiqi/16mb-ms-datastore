import pytest
import pymonetdb
import pymonetdb.exceptions
from nameko.testing.services import worker_factory

from application.services.datastore import DatastoreService


@pytest.fixture
def connection(host, user, password, database, port):
    _conn = pymonetdb.connect(username=user, hostname=host, password=password, database=database, port=port,
                              autocommit=True)

    yield _conn

    def clean_tables():
        cursor = _conn.cursor()

        cursor.execute('SELECT NAME FROM SYS.TABLES WHERE SYSTEM=0')

        has_cleaned = False

        for table in cursor.fetchall():
            has_cleaned = True
            try:
                _conn.execute('DROP TABLE {table}'.format(table=table[0]))
            except pymonetdb.exceptions.Error:
                continue

        cursor.close()

        return has_cleaned

    def clean_functions():
        cursor = _conn.cursor()

        cursor.execute('SELECT NAME, TYPE FROM SYS.FUNCTIONS WHERE LANGUAGE = 6')

        for func in cursor.fetchall():
            _conn.execute('DROP AGGREGATE {}'.format(func[0])) if func[1] == 3\
                else _conn.execute('DROP FUNCTION {}'.format(func[0]))

        cursor.close()

    max_retry = 2
    has_cleaned = True
    tries = 0

    while tries < max_retry and has_cleaned:
        has_cleaned = clean_tables()
        tries += 1

    clean_functions()

    _conn.close()


def test_insert(connection):
    service = worker_factory(DatastoreService, connection=connection)

    records = [{'ID': 1, 'VALUE': 'toto'}, {'ID': 2, 'VALUE': 'titi'}]
    meta = [('ID', 'INTEGER'), ('VALUE', 'VARCHAR(5)')]

    service.insert('NONPART_INSERT_TABLE', records, meta)

    cursor = connection.cursor()
    cursor.execute('SELECT COUNT(*) FROM NONPART_INSERT_TABLE')

    assert cursor.fetchone()[0] == 2


def test_truncate(connection):
    service = worker_factory(DatastoreService, connection=connection)

    records = [{'ID': 1, 'VALUE': 'toto'}, {'ID': 2, 'VALUE': 'titi'}]
    meta = [('ID', 'INTEGER'), ('VALUE', 'VARCHAR(5)')]

    service.insert('NONPART_TRUNCATE_TABLE', records, meta)

    service.truncate('NONPART_TRUNCATE_TABLE')

    cursor = connection.cursor()
    cursor.execute('SELECT COUNT(*) FROM NONPART_TRUNCATE_TABLE')

    assert cursor.fetchone()[0] == 0


def test_delete(connection):
    service = worker_factory(DatastoreService, connection=connection)

    records = [{'ID': 1, 'VALUE': 'toto'}, {'ID': 2, 'VALUE': 'titi'}]
    meta = [('ID', 'INTEGER'), ('VALUE', 'VARCHAR(5)')]

    service.insert('NONPART_DELETE_TABLE', records, meta)

    service.delete('NONPART_DELETE_TABLE', {'ID': 1})

    cursor = connection.cursor()
    cursor.execute('SELECT COUNT(*) FROM NONPART_DELETE_TABLE')

    assert cursor.fetchone()[0] == 1


def test_update(connection):
    service = worker_factory(DatastoreService, connection=connection)

    records = [{'ID': 1, 'VALUE': 'toto'}, {'ID': 2, 'VALUE': 'titi'}]
    meta = [('ID', 'INTEGER'), ('VALUE', 'VARCHAR(5)')]

    service.insert('NONPART_UPDATE_TABLE', records, meta)

    service.update('NONPART_UPDATE_TABLE', 'ID', [{'ID': 2, 'VALUE': 'tata'}])

    cursor = connection.cursor()
    cursor.execute('SELECT VALUE FROM NONPART_UPDATE_TABLE WHERE ID = 2')

    assert cursor.fetchone()[0] == 'tata'


def test_upsert(connection):
    service = worker_factory(DatastoreService, connection=connection)

    records = [{'ID': 1, 'VALUE': 'toto'}, {'ID': 2, 'VALUE': 'titi'}]
    meta = [('ID', 'INTEGER'), ('VALUE', 'VARCHAR(5)')]

    service.insert('NONPART_UPSERT_TABLE', records, meta)

    service.upsert('NONPART_UPSERT_TABLE', 'ID', [{'ID': 2, 'VALUE': 'tata'}, {'ID': 3, 'VALUE': 'tutu'}], meta)

    cursor = connection.cursor()
    cursor.execute('SELECT VALUE FROM NONPART_UPSERT_TABLE WHERE ID = 2')

    assert cursor.fetchone()[0] == 'tata'

    cursor.execute('SELECT VALUE FROM NONPART_UPSERT_TABLE WHERE ID = 3')

    assert cursor.fetchone()[0] == 'tutu'

    service.upsert('NONEXTISTED_UPSERT_TABLE', 'ID', [{'ID': 2, 'VALUE': 'tata'}, {'ID': 3, 'VALUE': 'tutu'}], meta)

    cursor = connection.cursor()
    cursor.execute('SELECT VALUE FROM NONPART_UPSERT_TABLE WHERE ID = 1')

    assert cursor.fetchone()[0] == 'toto'


def test_bulk_insert(connection):
    service = worker_factory(DatastoreService, connection=connection)

    records = [{'id': 1, 'value': 'toto'}, {'value': 'titi', 'id': 2}]
    meta = [('ID', 'INTEGER'), ('VALUE', 'VARCHAR(5)')]

    service.bulk_insert('NONPART_BULK_TABLE', records, meta, {'ID': 'id', 'VALUE': 'value'})

    cursor = connection.cursor()
    cursor.execute('SELECT VALUE FROM NONPART_BULK_TABLE WHERE ID = 2')
    assert cursor.fetchone()[0] == 'titi'

    records = [{'ID': 3, 'VALUE': 'tutu'}, {'ID': 4, 'VALUE': 'tata'}]
    service.bulk_insert('NONPART_BULK_TABLE', records, meta)
    cursor.execute('SELECT VALUE FROM NONPART_BULK_TABLE WHERE ID = 4')
    assert cursor.fetchone()[0] == 'tata'

    wrong_records = [{'ID': 'wrong', 'VALUE': 'titi'}]
    with pytest.raises(pymonetdb.exceptions.OperationalError):
        service.bulk_insert('NONPART_BULK_TABLE', wrong_records, meta)

    cursor.execute('DELETE FROM NONPART_BULK_TABLE')
    records = [
        {'id': 1, 'value': 'toto'},
        {'value': 'titi', 'id': 2},
        {'id': 3, 'value': 'tutu'},
        {'id': 4, 'value': 'tata'}
    ]
    service.bulk_insert('NONPART_BULK_TABLE', records, meta, mapping={'ID': 'id', 'VALUE': 'value'}, chunk_size=3)
    cursor.execute('SELECT COUNT(*) FROM NONPART_BULK_TABLE')
    assert cursor.fetchone()[0] == 4


def test_create_or_replace_view(connection):
    service = worker_factory(DatastoreService, connection=connection)
    service.create_or_replace_view('MYVIEW', 'SELECT 1 AS V', None)

    cursor = connection.cursor()
    cursor.execute('SELECT * FROM MYVIEW')

    assert cursor.fetchone()[0] == 1

    service.create_or_replace_view('MYVIEW', 'SELECT 1 AS V', None)


def test_insert_from_select(connection):
    service = worker_factory(DatastoreService, connection=connection)

    cursor = connection.cursor()
    query = 'SELECT 0 AS GROUP_ID, 1 AS ID, 35.0 AS VALUE UNION ALL SELECT 1 AS GROUP_ID, 2 AS ID, -5.0 AS VALUE'

    service.insert_from_select('NONPART_INSERTSELECT_TABLE', query, None)

    cursor.execute('SELECT VALUE FROM NONPART_INSERTSELECT_TABLE WHERE ID = 2')

    assert cursor.fetchone()[0] == -5.

    query = 'SELECT * FROM (SELECT 0 AS GROUP_ID, 1 AS ID, 35.0 AS VALUE) T WHERE ID = %s'

    service.insert_from_select('NONPART_INSERTSELECTP_TABLE', query, [1])

    cursor.execute('SELECT VALUE FROM NONPART_INSERTSELECTP_TABLE WHERE ID = 1')

    assert cursor.fetchone()[0] == 35.


def test_check_if_function_exists(connection):
    service = worker_factory(DatastoreService, connection=connection)

    script = '''
    CREATE FUNCTION kwnown_function(i INTEGER) RETURNS INTEGER LANGUAGE PYTHON {
        return i * 2
    };
    '''

    connection.execute(script)

    exists = service.check_if_function_exists('kwnown_function')

    assert exists is True

    exists = service.check_if_function_exists('unknown_function')

    assert exists is False


def test_create_or_replace_python_function(connection):
    service = worker_factory(DatastoreService, connection=connection)

    script = '''
    CREATE FUNCTION python_times_two(i INTEGER) RETURNS INTEGER LANGUAGE PYTHON {
        return i * 2
    };
    '''

    service.create_or_replace_python_function('python_times_two', script)

    cursor = connection.cursor()
    cursor.execute('SELECT python_times_two(2) as result')

    assert cursor.fetchone()[0] == 4

    service.create_or_replace_python_function('python_times_two', script)

    cursor.execute('SELECT python_times_two(2) as result')

    assert cursor.fetchone()[0] == 4

def test_create_or_replace_aggregate(connection):
    service = worker_factory(DatastoreService, connection=connection)

    script = '''
    CREATE AGGREGATE python_aggregate(val INTEGER) 
    RETURNS INTEGER 
    LANGUAGE PYTHON {
        try:
            unique = numpy.unique(aggr_group)
            x = numpy.zeros(shape=(unique.size))
            for i in range(0, unique.size):
                x[i] = numpy.sum(val[aggr_group==unique[i]])
        except NameError:
            # aggr_group doesn't exist. no groups, aggregate on all data
            x = numpy.sum(val)
        return(x)
    };
    '''

    service.create_or_replace_python_function('python_aggregate', script)
    service.create_or_replace_python_function('python_aggregate', script)


def test_add_partition(connection):
    service = worker_factory(DatastoreService, connection=connection)

    connection.execute('CREATE TABLE T1 (ID INTEGER)')
    connection.execute('INSERT INTO T1 VALUES (1)')

    service.add_partition('T1', 'MT1', [('ID', 'INTEGER')])

    cursor = connection.cursor()
    cursor.execute('SELECT ID FROM MT1')

    assert cursor.fetchone()[0] == 1


def test_drop_paritition(connection):
    service = worker_factory(DatastoreService, connection=connection)

    connection.execute('CREATE TABLE T2 (ID INTEGER)')
    connection.execute('INSERT INTO T2 VALUES (1)')

    connection.execute('CREATE TABLE T3 (ID INTEGER)')
    connection.execute('INSERT INTO T3 VALUES (2)')

    connection.execute('CREATE MERGE TABLE MT2 (ID INTEGER)')
    connection.execute('ALTER TABLE MT2 ADD TABLE T2')
    connection.execute('ALTER TABLE MT2 ADD TABLE T3')

    service.drop_partition('T2', 'MT2')

    cursor = connection.cursor()
    cursor.execute('SELECT ID FROM MT2')

    assert cursor.fetchone()[0] == 2
