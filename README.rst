===== 
SQream SQLAlchemy Dialect
===== 

Requirements:
----------
- Python 3.6+ - Use Python 3.8.1+ for best performance
- SQLAlchemy 1.3.17+ - Tested against 1.3.17 - ``pip3 install --upgrade sqlalchemy``
- SQream DB-API Connector 3.0.2+ - ``pip3 install`` or put in the same folder as the dialect
- Cython - an optional dependency for SQream DB-API Connector - ``pip3 install --upgrade cython``


Simple Usage Sample:
----------

.. code-block:: python

    import sqlalchemy as sa
              
    # Direct usage of the dialect with In-process registering, if not pip installed
    sa.dialects.registry.register("pysqream-sqlalchemy.dialect", "dialect", "SqreamDialect") 
    
    conn_str = "sqream://sqream:sqream@localhost:5001/master?use_ssl=True"                                                  
    engine = create_engine(conn_str, echo = print_echo) 

    metadata = MetaData()
    metadata.bind = engine

    res = engine.execute('create or replace table test (ints int)')
    res = engine.execute('insert into test values (5), (6)')
    res = engine.execute('select * from test')
