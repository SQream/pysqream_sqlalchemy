'''
SQLAlchemy refers to SQL variants as dialects. An SQLAlchemy Dialect object
contains information about specific behaviors of the backend, keywords etc.
It also references to the default underlying DB-API implementation (aka Driver) in use.

Usage:
- Pop a Python shell from sqream_dialect.py's folder, or add it to Python's import path

# Usage snippet - type in in shell or editor
# ------------------------------------------

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry

# In-process registering, installing the package not required
registry.register("sqream.sqream_dialect", "sqream_dialect", "SqreamDialect")

engine = create_engine("sqream+sqream_dialect://sqream:sqream@localhost:5000/master")

# Check 1, 2
res = engine.execute('select 1')

for row in res:
    print row

'''
import pysqream.utils
# from __future__ import annotations
# from importlib import import_module, resources    # for importing and returning the module
from sqlalchemy.engine.default import DefaultDialect, DefaultExecutionContext
from sqlalchemy.types import Boolean, LargeBinary, SmallInteger, Integer, BigInteger, Float, Date, DateTime, String, Unicode, UnicodeText, Numeric
from .base import SqreamSQLCompiler, SqreamTypeCompiler, TINYINT, SqreamDDLCompiler
from sqlalchemy.dialects import registry
from sqlalchemy.sql import compiler, crud

try:
    from alembic.ddl.impl import DefaultImpl
except ImportError:
    pass
else:
    class SQreamImpl(DefaultImpl):
        ''' Allows Alembic tool to recognize the dialect if installed '''

        __dialect__ = 'sqream'


registry.register("pysqream", "dialect", "SqreamDialect")



sqream_to_alchemy_types = {
    'bool':      Boolean,
    'boolean':   Boolean,
    'ubyte':     TINYINT,
    'tinyint':   TINYINT,
    'smallint':  SmallInteger,
    'int':       Integer,
    'integer':   Integer,
    'bigint':    BigInteger,
    'float':     Float,
    'double':    Float,
    'real':      Float,
    'date':      Date,
    'datetime':  DateTime,
    'timestamp': DateTime,
    'varchar':   String,
    'nvarchar':  Unicode,
    'text':      Unicode,
    'numeric':   Numeric,
}


def printdbg(message, dbg=False):

    if dbg:
        print(message)


class SqreamDialect(DefaultDialect):
    ''' dbapi() classmethod, get_table_names() and get_columns() seem to be the
        important ones for Apache Superset. get_pk_constraint() returning an empty
        sequence also needs to be in place  '''

    name = 'sqream'
    driver = 'sqream'
    default_paramstyle = 'qmark'
    supports_native_boolean = True
    supports_multivalues_insert = True
    supports_native_decimal = True
    supports_statement_cache = True
    supports_identity_columns = True
    supports_sequences = False

    type_compiler = SqreamTypeCompiler
    statement_compiler = SqreamSQLCompiler
    ddl_compiler = SqreamDDLCompiler
    Tinyint = TINYINT

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def dbapi(cls):
        ''' The minimal reqruirement to get an engine.connect() going'''
        # return dbapi

        # return __import__("sqream_dbapi", fromlist="sqream")

        try:
            from pysqream import pysqream as pysqream
        except ImportError:
            import pysqream

        return pysqream

    def initialize(self, connection):
        self.default_schema_name = 'public'

    def get_table_names(self, connection, schema=None, **kw):
        ''' Allows showing table names when connecting database to Apache Superset'''

        query = "select * from sqream_catalog.tables"
        tables = connection.execute(query).fetchall()
        query = "select * from sqream_catalog.external_tables"
        external_tables = connection.execute(query).fetchall()
        return [table_spec[3] for table_spec in tables + external_tables]

    def get_schema_names(self, connection, schema=None, **kw):
        ''' Return schema names '''

        query = "select get_schemas()"
        return [schema for schema, database in connection.execute(query).fetchall()]

    def has_schema(self, connection, schema):
        query = "select get_schemas()"
        schemas = [schema for schema, database in connection.execute(query).fetchall()]
        return schema in schemas

    def get_view_names(self, connection, schema='public', **kw):
        
        # 0,public.fuzz
        return [schema_view.split(".", 1)[1] for idx, schema_view in connection.execute("select get_views()").fetchall() if schema_view.split(".", 1)[0] == schema]

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_table_names(connection, schema)

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        ''' Used by SQLAlchemy's Table() which is called by Superset's get_table()
            when trying to add a new table to the sources'''

        query = f'select get_ddl(\'"{table_name}"\')'
        res = connection.execute(query).fetchall()
        table_ddl = ''.join(tup[0] for tup in res).splitlines()

        schema = table_ddl[0].split()[2].split('.')[0]
        columns_meta = []

        # 1st (0) entry is "create table", last 4 are closing parantheses and other jib
        for col in table_ddl[1:-4]:
            if col == ')':
                break
            col_meta = col.split('"')
            col_name = col_meta[1]
            try:
                type_key = col_meta[2].split()[0].split('(')[0]
                col_type = sqream_to_alchemy_types[type_key]
            except KeyError as e:
                raise Exception(f'key {type_key} not found. Perhaps get_ddl() implementation change? ** col meta {col_meta}')

            col_nullable = col_meta[2] == 'null' 
            c = {
                'name': col_name,
                'type': col_type,
                'nullable': col_nullable,
                'default': None
                }
            columns_meta.append(c)       # add default extraction if exists in sqream

        return columns_meta

    def do_execute(self, cursor, statement, parameters, context=None):

        if statement.lower().startswith('insert') and '?' in statement: # and type(parameters[0] not in (tuple, list)):
            cursor.executemany(statement, parameters, data_as='alchemy_flat_list')
        else:
            cursor.execute(statement, parameters)

    def _get_server_version_info(self, connection):

        query = 'select get_sqream_server_version()'
        sqream_version = connection.execute(query).fetchall()[0][0]

        return sqream_version

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return {}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    # TODO - add to readme - commit not supported
    def do_commit(self, connection):
        connection.commit()
        # raise NotSupportedException("commit are not supported by SQream")

    # TODO - add to readme - rollback not supported
    def do_rollback(self, connection):
        connection.rollback()
        # raise NotSupportedException("rollback are not supported by SQream")


class NotSupportedException(Exception):
    pass