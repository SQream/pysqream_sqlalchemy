''' Testing the SQream SQLAlchemy dialect. See also tests for the SQream
    DB-API connector 
'''

import os, sys
sys.path.append(os.path.abspath(__file__).rsplit('tests/', 1)[0] + '/pysqream/')

from sqlalchemy import orm, create_engine, MetaData, inspect, Table, Column, select, insert, cast
from sqlalchemy.schema import CreateTable   # Print ORM table DDLs

from alembic.runtime.migration import MigrationContext
from alembic.operations import Operations

import sqlalchemy as sa, pandas as pd
from time import time
from datetime import datetime, date, timezone as tz

try:
    import pudb as pdb
except:
    import pdb


## Registering dialect
#  -------------------

# In-process registering, doesn't require a dialect package
sa.dialects.registry.register("pysqream.dialect", "dialect", "SqreamDialect") 
conn_str = "pysqream+dialect://sqream:sqream@localhost:5001/master?use_ssl=True"                                                  
print_echo = False
engine = create_engine(conn_str, echo = print_echo) 
sa.Tinyint = engine.dialect.Tinyint
session = orm.sessionmaker(bind=engine)()

metadata = MetaData()
metadata.bind = engine

## SQLAlchemy tests
#  ----------------

def sqlalchemy_tests():

    print (f'SQLAlchemy direct query tests')
    # Test 0 - as bestowed upon me by Yuval. Using the URL object directly instead of a connection string
    manual_conn_str = sa.engine.url.URL(
        'pysqream+dialect', username='sqream', password='sqream', 
        host='localhost', port=5001, database='master', 
        query={'use_ssl': True})
    engine2 = create_engine(manual_conn_str)
    res = engine2.execute('select 1')
    assert(all(row[0] == 1 for row in res))

    # Simple direct Engine query - this passes the queries to the underlying DB-API
    res = engine.execute('create or replace table "kOko" ("iNts" int)')
    res = engine.execute('insert into "kOko" values (1),(2),(3),(4),(5)')
    res = engine.execute('select * from kOko')
    # Using the underlying DB-API fetch() functions
    assert (res.fetchone() == (1,))
    assert (res.fetchmany(2) == [(2,), (3,)])
    assert (res.fetchall() == [(4,), (5,)])

    # Reflection test
    inspector = inspect(engine)
    inspected_cols = inspector.get_columns('kOko')
    assert (inspected_cols[0]['name'] == 'iNts')

    print (f'SQLAlchemy ORM tests')
    # ORM queries - test that correct SQream queries (SQL text strings) are
    # creaed (that are then passed to the DB-API)
    
    # Create table via ORM
    orm_table = Table(
        'orm_table', metadata, 
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
    )
    if engine.has_table(orm_table.name): 
        orm_table.drop()
    
    orm_table.create()

    # Insert into table
    values = [(True, 77, 777, 7777, 77777, 7.0, 7.77777777, date(2012, 11, 23), datetime(2012, 11, 23, 16, 34, 56), 'bla', 'בלה'),] * 2 
    orm_table.insert().values(values).execute()
    
    # Validate results
    res = engine.execute(orm_table.select()).fetchall()
    assert(values == res)

    # Run a simple join query
    t2 = orm_table.alias()
    joined = orm_table.join(t2, orm_table.columns.iNts == t2.columns.iNts, isouter = False)
    # orm_table.select().select_from(joined).execute()
    res = joined.select().execute().fetchall()
    assert(len(res) == 2 * len(values))


## Pandas tests
#  ------------

def pandas_tests():

    # Creating a SQream table from a Pandas DataFrame
    print (f'Pandas tests')
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
        'nvarchars':  ['shoko','shoko2']
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
        'nvarchars': sa.UnicodeText
    }

    # Drop, create and insert
    df.to_sql('kOko3', engine, if_exists='replace', index=False, dtype=dtype)  
    
    res = pd.read_sql('select * from "kOko3"', conn_str)

    assert ((res == df).eq(True).all()[0])


## Alembic tests
#  -------------

def alembic_tests():

    print (f'Alembic tests')
    conn = engine.connect()
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
        'dates':date(2012, 11, 23),
        'datetimes': datetime(2012, 11, 23, 16, 34, 56),
        'varchars': 'bla',
        'nvarchars': 'bla2'
        },
        {'bools': False,
        'ubytes':6,
        'shorts': 66,
        'ints': 666,
        'bigints': 6666,
        'floats': 6.0,
        'doubles': 6.6666666,
        'dates':date(2012, 11, 24),
        'datetimes': datetime(2012, 11, 24, 16, 34, 57),
        'varchars': 'bla',
        'nvarchars': 'bla2'
        }
    ]

    op.bulk_insert(t, data)
    
    res = engine.execute('select * from waste').fetchall()
    assert(res == [tuple(dikt.values()) for dikt in data])


if __name__ == '__main__':
    
    args = sys.argv

    sqlalchemy_tests()
    pandas_tests()
    alembic_tests()
