'''
    Testing the SQream SQLAlchemy dialect. See also tests for the SQream
    DB-API connector
'''

import os
import sys

sys.path.append(os.path.abspath(__file__).rsplit('tests/', 1)[0] + '/pysqream_sqlalchemy/')
sys.path.append(os.path.abspath(__file__).rsplit('tests/', 1)[0] + '/tests/')
import pytest
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, select, Table, Column, insert, text, DDL, orm
from test_base import TestBase, Logger, TestBaseTI
from alembic.runtime.migration import MigrationContext
from alembic.operations import Operations
from datetime import datetime, date
from decimal import Decimal

# Registering dialect
sa.dialects.registry.register("pysqream.dialect", "dialect", "SqreamDialect")


def find_diff(df1: pd.DataFrame, df2: pd.DataFrame):
    """
    Find the differance between two dataframes
    """
    if len(df1.index) != len(df2.index):
        msg = f"Row count does not match\nSQream returned {len(df1.index)}\nremote returned: {len(df2.index)}"
        return False, msg

    res = df1.compare(df2, keep_equal=True)
    return (True, "") if res.empty else (False, str(res))


class TestSqlalchemy(TestBase):
    def test_sqlalchemy(self):
        Logger().info('SQLAlchemy direct query tests')
        # Test 0 - as bestowed upon me by Yuval. Using the URL object directly instead of a connection string
        sa.dialects.registry.register("pysqream.dialect", "dialect", "SqreamDialect")
        manual_conn_str = sa.engine.url.URL.create(drivername='pysqream+dialect',
                                                   username='sqream',
                                                   password='sqream',
                                                   host=f'{self.ip}',
                                                   port=self.port,
                                                   database='master')
        engine2 = create_engine(manual_conn_str)
        session2 = orm.sessionmaker(bind=engine2)()
        res = session2.execute(DDL('select 1'))
        assert (all(row[0] == 1 for row in res))

        # Simple direct Engine query - this passes the queries to the underlying DB-API
        self.session.execute(DDL('create or replace table "kOko" ("iNts fosho" int not null)'))
        self.session.execute(DDL('insert into "kOko" values (1),(2),(3),(4),(5)'))
        res = self.session.execute(DDL('select * from "kOko"'))

        # Using the underlying DB-API fetch() functions
        assert (res.fetchone() == (1,))
        assert (res.fetchmany(2) == [(2,), (3,)])
        assert (res.fetchall() == [(4,), (5,)])

        # Reflection test
        inspected_cols = self.insp.get_columns('kOko')
        assert (inspected_cols[0]['name'] == 'iNts fosho')

        self.metadata.reflect(bind=self.engine, only={'kOko'})
        assert (repr(self.metadata.tables["kOko"]) == "Table('kOko', MetaData(), Column('iNts fosho', Integer(), table=<kOko>, nullable=False), schema=None)")

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
            Column('numerics', sa.Numeric(38, 1)),
            extend_existing=True
        )
        if self.insp.has_table(orm_table.name):
            orm_table.drop(bind=self.engine)

        orm_table.create(bind=self.engine)

        # Insert into table
        values = [(True, 77, 777, 7777, 77777, 7.0, 7.77777777, date(2012, 11, 23), datetime(2012, 11, 23, 16, 34, 56),
                   'test', 'test_text', Decimal('7.7')), ] * 2

        stmt = orm_table.insert().values(values)
        self.session.execute(stmt)

        # Validate results
        res = self.session.execute(orm_table.select()).fetchall()
        assert values == res

        # Run a simple join query
        t2 = orm_table.alias()
        joined = orm_table.join(t2, orm_table.columns.iNts == t2.columns.iNts, isouter=False)
        stmt = joined.select()
        res = self.session.execute(stmt).fetchall()
        assert len(res) == len(values) * 2


# Pandas tests
class TestPandas(TestBase):
    def test_pandas(self):
        # Creating a SQream table from a Pandas DataFrame
        Logger().info('Pandas tests')
        df = pd.DataFrame({
            'bools': [True, False],
            'ubytes': [10, 11],
            'shorts': [110, 111],
            'ints': [1110, 1111],
            'bigints': [1111110, 11111111],
            'floats': [10.0, 11.0],
            'doubles': [10.1111111, 11.1111111],
            'dates': [date(2012, 11, 23), date(2012, 11, 23)],
            'datetimes': [datetime(2012, 11, 23, 16, 34, 56), datetime(2012, 11, 23, 16, 34, 56)],
            'varchars': ['koko', 'koko2'],
            'nvarchars': ['shoko', 'shoko2'],
            'numerics': [Decimal("1.1"), Decimal("-1.1")]
        })

        dtype = {
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

        res = pd.read_sql('select * from "kOko3"', self.engine)
        res2 = pd.read_sql_table('kOko3', self.engine)

        assert ((res == df).eq(True).all().iloc[0])
        assert ((res2 == df).eq(True).all().iloc[0])


# Alembic tests
class TestAlembic(TestBase):
    def test_alembic(self):
        Logger().info('Alembic tests')
        session = self.session.connection()
        ctx = MigrationContext.configure(session)
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
                'ubytes': 5,
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
             'ubytes': 6,
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

        res = self.session.execute(text('select * from waste')).fetchall()
        assert (res == [tuple(d.values()) for d in data])


class TestTI(TestBaseTI):

    @pytest.fixture()
    def path(self):
        return f"{os.getcwd()}/tests/data/LBC9_PLV_affinity_matrix_send.csv"

    @pytest.fixture()
    def insert_data(self, path):
        return pd.read_csv(path).to_dict('records')

    @pytest.mark.parametrize("case", (1, 2))
    def test_ti(self, path, insert_data, case):
        if case == 1:
            ins = insert(self.testware_affinity_matrix).values(insert_data)
            self.session.execute(ins)
        elif case == 2:
            ins = self.testware_affinity_matrix.insert()
            self.session.execute(ins, insert_data)

        res = self.session.execute(self.testware_affinity_matrix.select()).fetchall()
        res_df = pd.DataFrame(res, columns=['technology', 'criteria', 'category', 'component',
                                            'svn', 'parm_name', 'lpt', 'tech', 'severity'])
        expected_df = pd.read_csv(path)
        is_equal, msg_results = find_diff(expected_df, res_df)
        assert is_equal, msg_results


class TestNew(TestBase):
    def test_1(self):
        table1 = Table(
            'table1', self.metadata,
            Column("id", sa.Integer), Column("name", sa.UnicodeText), Column("value2", sa.Integer)
        )
        if self.insp.has_table(table1.name):
            table1.drop(bind=self.engine)

        table1.create(bind=self.engine)

        # Insert into table
        values = [(1, 'test', 2)]

        ins = insert(table1).values(values)
        self.session.execute(ins)

        stmt = select(table1).where(text("id=1"))
        res = self.session.execute(stmt).fetchall()
        assert res == values, res
