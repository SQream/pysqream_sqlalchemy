===== 
SQream SQLAlchemy Dialect
===== 

Requirements:
----------
- Python 3.6+ (Some boost in performance with Python 3.8.1+)
- SQLAlchemy 1.3.15+ (tested against 1.3.15)
- SQream DB-API Connector 3.0.1+ - `pip install` or put in the same folder as the dialect

Simple Usage Sample:
----------

.. code-block:: python
              
    # Direct usage of the dialect with In-process registering, if not pip installed
    sa.dialects.registry.register("pysqream-sqlalchemy.dialect", "dialect", "SqreamDialect") 
    
    conn_str = ""pysqream-sqlalchemy+dialect://sqream:sqream@localhost:5001/master?use_ssl=True"                                                  
    engine = create_engine(conn_str, echo = print_echo) 

    metadata = MetaData()
    metadata.bind = engine

    res = engine.execute('create or replace table test (ints int)')
    res = engine.execute('insert into test values (5), (6)')
    res = engine.execute('select * from test')
