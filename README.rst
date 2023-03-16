**********************************
SQLAlchemy Dialect for SQream DB
**********************************

Requirements:
=====================

* Python > 3.9+
* SQLAlchemy = 1.4.46
* SQream DB-API Connector >= 3.2.5
* Cython (optional - improves performance)


Simple Usage Sample:
===============================

.. code-block:: python

    import sqlalchemy as sa
    import pandas as pd
                  
    conn_str = "sqream://sqream:sqream@localhost:3108/master"                                                  
    engine = sa.create_engine(conn_str, connect_args={"clustered": True}) 

    engine.execute('create or replace table test (ints int)')
    engine.execute('insert into test values (5), (6)')
    df = pd.read_sql('select * from test', engine)
    print(df)
