**********************************
SQLAlchemy Dialect for SQream DB
**********************************

Requirements:
=====================

* Python > 3.6. Python 3.8.1+ recommended
* SQLAlchemy > 1.3.18
* SQream DB-API Connector > 3.0.3
* Cython (optional - improves performance)


Simple Usage Sample:
===============================

.. code-block:: python

    import sqlalchemy as sa
                  
    conn_str = "sqream://sqream:sqream@localhost:5001/master?use_ssl=True"                                                  
    engine = create_engine(conn_str, echo = print_echo) 

    metadata = MetaData()
    metadata.bind = engine

    res = engine.execute('create or replace table test (ints int)')
    res = engine.execute('insert into test values (5), (6)')
    res = engine.execute('select * from test')
