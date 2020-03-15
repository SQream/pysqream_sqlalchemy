===== 
SQream SQLAlchemy Dialect
===== 


Simple Usage example:
----------

.. code-block:: python
              
    # Direct usage with In-process registering, doesn't require pip install
    sa.dialects.registry.register("pysqream.dialect", "dialect", "SqreamDialect") 
    conn_str = "pysqream+dialect://sqream:sqream@localhost:5001/master?use_ssl=True"                                                  
    engine = create_engine(conn_str, echo = print_echo) 

    metadata = MetaData()
    metadata.bind = engine

    res = engine.execute('create or replace table kOko (ints int)')
    res = engine.execute('insert into kOko values (5)')
    res = engine.execute('select * from kOko')
    assert(all(row[0] == 5 for row in res))

