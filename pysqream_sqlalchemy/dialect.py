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
# from __future__ import annotations
# from importlib import import_module, resources    # for importing and returning the module
from sqlalchemy.engine.default import DefaultDialect, DefaultExecutionContext
from sqlalchemy.types import Boolean, LargeBinary, SmallInteger, Integer, BigInteger, Float, Date, DateTime, String, Unicode, UnicodeText, Numeric
from sqlalchemy.dialects.mysql import TINYINT
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


class TINYINT(TINYINT):
    ''' Allows describing tables via the ORM mechanism. Complemented in
        SqreamTypeCompiler '''

    pass


class SqreamTypeCompiler(compiler.GenericTypeCompiler):
    ''' Get the SQream string names for SQLAlchemy types, useful for ORM
        generated Create queries '''

    def visit_BOOLEAN(self, type_, **kw):

        return "BOOL"

    def visit_TINYINT(self, type_, **kw):

        return "TINYINT"


class SqreamSQLCompiler(compiler.SQLCompiler):
    ''' Overriding visit_insert behavior of generating SQL with multiple
       (?,?) clauses for ORM inserts with parameters  '''

    def visit_insert(self, insert_stmt, asfrom=False, **kw):
        toplevel = not self.stack

        self.stack.append(
            {
                "correlate_froms": set(),
                "asfrom_froms": set(),
                "selectable": insert_stmt,
            }
        )

        crud_params = crud._setup_crud_params(
            self, insert_stmt, crud.ISINSERT, **kw
        )

        if (not crud_params and
                not self.dialect.supports_default_values and
                not self.dialect.supports_empty_insert):
            raise exc.CompileError(
                "The '%s' dialect with current database "
                "version settings does not support empty "
                "inserts." % self.dialect.name
            )

        if insert_stmt._has_multi_parameters:
            if not self.dialect.supports_multivalues_insert:
                raise exc.CompileError(
                    "The '%s' dialect with current database "
                    "version settings does not support "
                    "in-place multirow inserts." % self.dialect.name
                )
            crud_params_single = crud_params[0]
        else:
            crud_params_single = crud_params

        preparer = self.preparer
        supports_default_values = self.dialect.supports_default_values

        text = "INSERT "

        if insert_stmt._prefixes:
            text += self._generate_prefixes(
                insert_stmt, insert_stmt._prefixes, **kw
            )

        text += "INTO "
        table_text = preparer.format_table(insert_stmt.table)

        if insert_stmt._hints:
            _, table_text = self._setup_crud_hints(insert_stmt, table_text)

        text += table_text

        if crud_params_single or not supports_default_values:
            text += " (%s)" % ", ".join(
                [preparer.format_column(c[0]) for c in crud_params_single]
            )

        if self.returning or insert_stmt._returning:
            returning_clause = self.returning_clause(
                insert_stmt, self.returning or insert_stmt._returning
            )

            if self.returning_precedes_values:
                text += " " + returning_clause
        else:
            returning_clause = None

        if insert_stmt.select is not None:
            select_text = self.process(self._insert_from_select, **kw)

            if self.ctes and toplevel and self.dialect.cte_follows_insert:
                text += " %s%s" % (self._render_cte_clause(), select_text)
            else:
                text += " %s" % select_text
        elif not crud_params and supports_default_values:
            text += " DEFAULT VALUES"

        # This part would originally generate an insert statement such as
        # `insert into table test values (?,?), (?,?), (?,?)` which sqream
        # does not support
        # <Overriding part> - money is in crud_params[0]
        elif insert_stmt._has_multi_parameters:
            insert_single_values_expr = ", ".join([c[1] for c in crud_params[0]])
            text += " VALUES (%s)" % insert_single_values_expr
            if toplevel:
                self.insert_single_values_expr = insert_single_values_expr
        # </Overriding part>
        else:
            insert_single_values_expr = ", ".join([c[1] for c in crud_params])
            text += " VALUES (%s)" % insert_single_values_expr
            if toplevel:
                self.insert_single_values_expr = insert_single_values_expr

        if insert_stmt._post_values_clause is not None:
            post_values_clause = self.process(
                insert_stmt._post_values_clause, **kw
            )
            if post_values_clause:
                text += " " + post_values_clause

        if returning_clause and not self.returning_precedes_values:
            text += " " + returning_clause

        if self.ctes and toplevel and not self.dialect.cte_follows_insert:
            text = self._render_cte_clause() + text

        self.stack.pop(-1)

        if asfrom:
            return "(" + text + ")"
        else:
            return text


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

    type_compiler = SqreamTypeCompiler
    statement_compiler = SqreamSQLCompiler
    Tinyint = TINYINT

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def dbapi(cls):
        ''' The minimal reqruirement to get an engine.connect() going'''
        # return dbapi

        # return __import__("sqream_dbapi", fromlist="sqream")

        try:
            from pysqream import dbapi as dbapi
        except ImportError:
            import dbapi

        return dbapi

    def initialize(self, connection):
        self.default_schema_name = 'public'

    def get_table_names(self, connection, schema=None, **kw):
        ''' Allows showing table names when connecting database to Apache Superset'''

        query = "select * from sqream_catalog.tables"
        return [table_spec[3] for table_spec in connection.execute(query).fetchall()]

    def get_schema_names(self, connection, schema=None, **kw):
        ''' Return schema names '''

        query = "select get_schemas()"
        return [schema for schema, database in connection.execute(query).fetchall()]
        

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

    def do_commit(self, connection):
        connection.commit()

    def do_rollback(self, connection):
        connection.rollback()
