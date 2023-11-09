**********************************
SQLAlchemy Dialect for SQream DB
**********************************

Prerequisites
================

* Python > 3.9+
* SQLAlchemy = 1.4.46
* SQream DB-API Connector = 3.2.5
* Cython (optional - improves performance)

Installing SQream SQLAlchemy
=============================

.. code-block:: shell

    pip3.9 install pysqream-sqlalchemy -U

Verifying Installation
------------------------

.. code-block:: python

    import sqlalchemy as sa
    import pandas as pd
                  
    conn_str = "sqream://sqream:sqream@localhost:3108/master"                                                  
    engine = sa.create_engine(conn_str, connect_args={"clustered": True}) 

    engine.execute('create or replace table test (ints int)')
    engine.execute('insert into test values (5), (6)')
    df = pd.read_sql('select * from test', engine)
    print(df)

Connection String 
=====================

.. code-block:: shell

    sqream://<user_login_name>:<password>@<host>:<port>/<db_name>

Parameters
------------

.. list-table:: 
   :widths: auto
   :header-rows: 1
   
   * - Parameter
     - Description
   * - ``username``
     - Username of a role to use for connection
   * - ``password``
     - Specifies the password of the selected role
   * - ``host``
     - Specifies the hostname
   * - ``port``
     - Specifies the port number
   * - ``port_ssl``
     - An optional parameter
   * - ``database``
     - Specifies the database name 
   * - ``clustered``
     - Establishing a multi-clustered connection. Input values: ``True``, ``False``. Default is ``False``
   * - ``service``
     - Specifies service queue to use

Example
=========

Pulling a Table into Pandas
---------------------------

The following example shows how to pull a table in Pandas. This example uses the URL method to create the connection string:

.. code-block:: python

   import sqlalchemy as sa
   
   from sqlalchemy.engine.url import URL
   
   import pandas as pd


	engine_url = URL('sqream'
		, username='sqream'
		, password='12345'
		, host='127.0.0.1'
		, port=3108
		, database='master')
	engine = sa.create_engine(engine_url,connect_args={"clustered": True, "service": "admin"})
	   
	table_df = pd.read_sql("select * from nba", con=engine)

Limitations
=============

Parameterized Queries
-----------------------

SQream SQLAlchemy supports only the ``BULK INSERT`` statement.

















