===== 
SQream SQLAlchemy Dialect
===== 


Simple Usage sample:
----------

.. code-block:: python
              
    # Direct usage with In-process registering, if pip install is undesirable
    sa.dialects.registry.register("pysqream-sqlalchemy.dialect", "dialect", "SqreamDialect") 
    conn_str = ""pysqream-sqlalchemy+dialect://sqream:sqream@localhost:5001/master?use_ssl=True"                                                  
    engine = create_engine(conn_str, echo = print_echo) 

    metadata = MetaData()
    metadata.bind = engine

    res = engine.execute('create or replace table test (ints int)')
    res = engine.execute('insert into test values (5)')
    res = engine.execute('select * from test')
    assert(all(row[0] == 5 for row in res))


Pandas Usage Sample:
----------

.. code-block:: python
              
    # Direct usage with In-process registering, if pip install is undesirable
    sa.dialects.registry.register("pysqream-sqlalchemy.dialect", "dialect", "SqreamDialect") 
    conn_str = ""pysqream-sqlalchemy+dialect://sqream:sqream@localhost:5001/master?use_ssl=True"                                                  
    engine = create_engine(conn_str, echo = print_echo) 

    metadata = MetaData()
    metadata.bind = engine

    res = engine.execute('create or replace table test (ints int)')
    res = engine.execute('insert into test values (5)')
    res = engine.execute('select * from test')
    assert(all(row[0] == 5 for row in res))
