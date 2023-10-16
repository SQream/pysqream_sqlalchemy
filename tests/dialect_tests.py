''' Testing the SQream SQLAlchemy dialect. See also tests for the SQream
    DB-API connector 
'''

import os, sys
sys.path.append(os.path.abspath(__file__).rsplit('tests/', 1)[0] + '/pysqream_sqlalchemy/')
from sqlalchemy import orm, create_engine, MetaData, inspect, Table, Column, select, insert, cast
from sqlalchemy.schema import CreateTable   # Print ORM table DDLs
from base import TestBase, TestBaseWithoutBeforeAfter, Logger, TestBaseTI
from alembic.runtime.migration import MigrationContext
from alembic.operations import Operations

import sqlalchemy as sa, pandas as pd
# import pandas as pd
from time import time
from datetime import datetime, date, timezone as tz
from decimal import Decimal
import pytest

# try:
#     import pudb as pdb
# except:
#     import pdb


## Registering dialect
sa.dialects.registry.register("pysqream.dialect", "dialect", "SqreamDialect")


def find_diff(df1: pd.DataFrame, df2: pd.DataFrame):
    """
    Find the differance between two dataframes
    """

    if len(df1.index) != len(df2.index):
        msg = f"Row count does not match\nSQream returned {len(df1.index)}\nremote returned: {len(df2.index)}"
        return (False, msg)

    res = df1.compare(df2, keep_equal=True)
    return (True, "") if res.empty else (False, str(res))


class TestSqlalchemy(TestBase):

    def test_sqlalchemy(self):

        Logger().info('SQLAlchemy direct query tests')
        # Test 0 - as bestowed upon me by Yuval. Using the URL object directly instead of a connection string
        sa.dialects.registry.register("pysqream.dialect", "dialect", "SqreamDialect")
        manual_conn_str = sa.engine.url.URL(
            'pysqream+dialect', username='sqream', password='sqream',
            host=f'{self.ip}', port=5000, database='master')
        engine2 = create_engine(manual_conn_str)
        res = engine2.execute('select 1')
        assert(all(row[0] == 1 for row in res))

        # Simple direct Engine query - this passes the queries to the underlying DB-API
        res = self.engine.execute('create or replace table "kOko" ("iNts fosho" int not null)')
        res = self.engine.execute('insert into "kOko" values (1),(2),(3),(4),(5)')
        res = self.engine.execute('select * from "kOko"')
        # Using the underlying DB-API fetch() functions
        assert(res.fetchone() == (1,))
        assert(res.fetchmany(2) == [(2,), (3,)])
        assert(res.fetchall() == [(4,), (5,)])

        # Reflection test
        inspector = inspect(self.engine)
        inspected_cols = inspector.get_columns('kOko')
        assert (inspected_cols[0]['name'] == 'iNts fosho')

        self.metadata.reflect(bind=self.engine)
        assert(repr(self.metadata.tables["kOko"]) == f"Table('kOko', MetaData(bind=Engine(pysqream+dialect://sqream:***@{self.ip}:5000/master)), Column('iNts fosho', Integer(), table=<kOko>, nullable=False), schema=None)")

        Logger().info('SQLAlchemy ORM tests')
        # ORM queries - test that correct SQream queries (SQL text strings) are
        # created (that are then passed to the DB-API)

        # Create table via ORM
        orm_table = Table(
            'orm_table', self.metadata,
            Column('bools', sa.Boolean),
            Column('ubytes', sa.Tinyint),
            Column('shorts', sa.SmallInteger),
            Column('iNts', sa.Integer),
            Column('bigints', sa.BigInteger),
            Column('floats', sa.REAL),
            Column('doubles', sa.Float),
            Column('dates', sa.Date),
            Column('datetimes', sa.DateTime),
            Column('varchars', sa.String(10)),
            Column('nvarchars', sa.UnicodeText),
            Column('numerics', sa.Numeric(38, 10)),
            extend_existing = True
        )
        if self.engine.has_table(orm_table.name):
            orm_table.drop()

        orm_table.create()

        # Insert into table
        values = [(True, 77, 777, 7777, 77777, 7.0, 7.77777777, date(2012, 11, 23), datetime(2012, 11, 23, 16, 34, 56),
                   'bla', 'בלה', Decimal("1.1")),] * 2

        orm_table.insert().values(values).execute()

        # Validate results
        res = self.engine.execute(orm_table.select()).fetchall()
        assert(values == res)

        # Run a simple join query
        t2 = orm_table.alias()
        joined = orm_table.join(t2, orm_table.columns.iNts == t2.columns.iNts, isouter=False)
        # orm_table.select().select_from(joined).execute()
        res = joined.select().execute().fetchall()
        assert(len(res) == 2 * len(values))


## Pandas tests
#  ------------


class TestPandas(TestBase):

    def test_pandas(self):

        # Creating a SQream table from a Pandas DataFrame
        Logger().info('Pandas tests')
        df = pd.DataFrame({
            'bools': [True,False],
            'ubytes': [10,11],
            'shorts': [110,111],
            'ints': [1110,1111],
            'bigints': [1111110,11111111],
            'floats': [10.0,11.0],
            'doubles': [10.1111111,11.1111111],
            'dates': [date(2012, 11, 23),date(2012, 11, 23)],
            'datetimes':  [datetime(2012, 11, 23, 16, 34, 56),datetime(2012, 11, 23, 16, 34, 56)],
            'varchars':  ['koko','koko2'],
            'nvarchars':  ['shoko','shoko2'],
            'numerics': [Decimal("1.1"), Decimal("-1.1")]
        })

        dtype={
            'bools': sa.Boolean,
            'ubytes': sa.Tinyint,
            'shorts': sa.SmallInteger,
            'ints': sa.Integer,
            'bigints': sa.BigInteger,
            'floats': sa.REAL,
            'doubles': sa.Float,
            'dates': sa.Date,
            'datetimes': sa.DateTime,
            'varchars': sa.String(10),
            'nvarchars': sa.UnicodeText,
            'numerics': sa.Numeric(38, 10)
        }

        # Drop, create and insert
        df.to_sql('kOko3', self.engine, if_exists='replace', index=False, dtype=dtype)

        res = pd.read_sql('select * from "kOko3"', self.conn_str)
        res2 = pd.read_sql_table('kOko3', con=self.engine)

        assert ((res == df).eq(True).all()[0])
        assert ((res2 == df).eq(True).all()[0])


## Alembic tests
#  -------------

class TestAlembic(TestBase):

    def test_alembic(self):
        Logger().info('Alembic tests')
        conn = self.engine.connect()
        ctx = MigrationContext.configure(conn)
        op = Operations(ctx)

        try:
            op.drop_table('waste')
        except:
            pass

        t = op.create_table('waste',
            Column('bools', sa.Boolean),
            Column('ubytes', sa.Tinyint),
            Column('shorts', sa.SmallInteger),
            Column('ints', sa.Integer),
            Column('bigints', sa.BigInteger),
            Column('floats', sa.REAL),
            Column('doubles', sa.Float),
            Column('dates', sa.Date),
            Column('datetimes', sa.DateTime),
            Column('varchars', sa.String(10)),
            Column('nvarchars', sa.UnicodeText),
            Column('numerics', sa.Numeric(38, 10)),
        )

        data = [
            {
            'bools': True,
            'ubytes':5,
            'shorts': 55,
            'ints': 555,
            'bigints': 5555,
            'floats': 5.0,
            'doubles': 5.5555555,
            'dates': date(2012, 11, 23),
            'datetimes': datetime(2012, 11, 23, 16, 34, 56),
            'varchars': 'bla',
            'nvarchars': 'bla2',
            'numerics': Decimal("1.1")
            },
            {'bools': False,
            'ubytes':6,
            'shorts': 66,
            'ints': 666,
            'bigints': 6666,
            'floats': 6.0,
            'doubles': 6.6666666,
            'dates': date(2012, 11, 24),
            'datetimes': datetime(2012, 11, 24, 16, 34, 57),
            'varchars': 'bla',
            'nvarchars': 'bla2',
            'numerics': Decimal("-1.1")
            }
        ]

        op.bulk_insert(t, data)

        res = self.engine.execute('select * from waste').fetchall()
        assert(res == [tuple(dikt.values()) for dikt in data])


class TestTI(TestBaseTI):

    @pytest.fixture()
    def path(self):
        return "data/LBC9_PLV_affinity_matrix_send.csv"

    @pytest.fixture()
    def insert_data(self, path):
        return pd.read_csv(path).to_dict('records')

    def test_ti_1(self, path, insert_data):

        ins = self.testware_affinity_matrix.insert(insert_data)
        self.engine.execute(ins)
        res = self.engine.execute(self.testware_affinity_matrix.select()).fetchall()
        res_df = pd.DataFrame(res, columns=['technology', 'criteria', 'category', 'component',
                                            'svn', 'parm_name', 'lpt', 'tech', 'severity'])
        expected_df = pd.read_csv(path)
        is_equal, msg_results = find_diff(expected_df, res_df)
        assert is_equal, msg_results

    def test_ti_2(self, path, insert_data):

        ins = self.testware_affinity_matrix.insert()
        self.engine.execute(ins, insert_data)
        res = self.engine.execute(self.testware_affinity_matrix.select()).fetchall()
        res_df = pd.DataFrame(res, columns=['technology', 'criteria', 'category', 'component',
                                            'svn', 'parm_name', 'lpt', 'tech', 'severity'])
        expected_df = pd.read_csv(path)
        is_equal, msg_results = find_diff(expected_df, res_df)
        assert is_equal, msg_results

