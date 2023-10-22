
from sqlalchemy.sql import compiler, crud
from sqlalchemy import exc, util
from sqlalchemy.dialects.mysql import TINYINT


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

    def visit_insert(self, insert_stmt, **kw):

        compile_state = insert_stmt._compile_state_factory(
            insert_stmt, self, **kw
        )
        insert_stmt = compile_state.statement

        toplevel = not self.stack

        if toplevel:
            self.isinsert = True
            if not self.dml_compile_state:
                self.dml_compile_state = compile_state
            if not self.compile_state:
                self.compile_state = compile_state

        self.stack.append(
            {
                "correlate_froms": set(),
                "asfrom_froms": set(),
                "selectable": insert_stmt,
            }
        )

        crud_params = crud._get_crud_params(
            self, insert_stmt, compile_state, **kw
        )

        if (
            not crud_params
            and not self.dialect.supports_default_values
            and not self.dialect.supports_default_metavalue
            and not self.dialect.supports_empty_insert
        ):
            raise exc.CompileError(
                "The '%s' dialect with current database "
                "version settings does not support empty "
                "inserts." % self.dialect.name
            )

        if compile_state._has_multi_parameters:
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

        if insert_stmt._independent_ctes:
            for cte in insert_stmt._independent_ctes:
                cte._compiler_dispatch(self, **kw)

        text += table_text

        if crud_params_single or not supports_default_values:
            text += " (%s)" % ", ".join(
                [expr for c, expr, value in crud_params_single]
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
            # placed here by crud.py
            select_text = self.process(
                self.stack[-1]["insert_from_select"], insert_into=True, **kw
            )

            if self.ctes and self.dialect.cte_follows_insert:
                nesting_level = len(self.stack) if not toplevel else None
                text += " %s%s" % (
                    self._render_cte_clause(
                        nesting_level=nesting_level,
                        include_following_stack=True,
                        visiting_cte=kw.get("visiting_cte"),
                    ),
                    select_text,
                )
            else:
                text += " %s" % select_text
        elif not crud_params and supports_default_values:
            text += " DEFAULT VALUES"
        # This part would originally generate an insert statement such as
        # `insert into table test values (?,?), (?,?), (?,?)` which sqream
        # does not support
        # <Overriding part> - money is in crud_params[0]
        elif compile_state._has_multi_parameters:
            insert_single_values_expr = ", ".join([c[2] for c in crud_params[0]])
            text += " VALUES (%s)" % insert_single_values_expr
            if toplevel:
                self.insert_single_values_expr = insert_single_values_expr
        # </Overriding part>
        else:
            insert_single_values_expr = ", ".join(
                [value for c, expr, value in crud_params]
            )
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

        if self.ctes and not self.dialect.cte_follows_insert:
            nesting_level = len(self.stack) if not toplevel else None
            text = (
                self._render_cte_clause(
                    nesting_level=nesting_level,
                    include_following_stack=True,
                    visiting_cte=kw.get("visiting_cte"),
                )
                + text
            )

        self.stack.pop(-1)

        return text


class SqreamDDLCompiler(compiler.DDLCompiler):

    def visit_identity_column(self, identity, **kw):
        self.check_identity_options(identity)
        text = " IDENTITY"
        if identity.start is not None or identity.increment is not None:
            start = 1 if identity.start is None else identity.start
            increment = 1 if identity.increment is None else identity.increment
            text += f"({start},{increment})"
        return text

    def visit_primary_key_constraint(self, constraint, **kw):
        # primary key constraints are not supported by SQream
        if constraint.columns_autoinc_first is not None and constraint.columns_autoinc_first[0].identity is None:
            raise NotSupportedException("primary key constraints are not supported by SQream")

    def visit_foreign_key_constraint(self, constraint, **kw):
        # foreign key constraints are not supported by SQream
        raise NotSupportedException("foreign key constraints are not supported by SQream")

    def visit_create_sequence(self, create, prefix=None, **kw):
        # create sequence are not supported by SQream
        raise NotSupportedException("create sequence are not supported by SQream")

    def visit_drop_sequence(self, drop, **kw):
        # drop sequence are not supported by SQream
        raise NotSupportedException("drop sequence are not supported by SQream")

    def check_identity_options(self, identity_options):
        if identity_options.minvalue is not None:
            raise NotSupportedException("min value of identity key constraints are not supported by SQream")
        if identity_options.maxvalue is not None:
            raise NotSupportedException("max value of identity key constraints are not supported by SQream")
        if identity_options.nominvalue is not None:
            raise NotSupportedException("no minvalue of identity key constraints are not supported by SQream")
        if identity_options.nomaxvalue is not None:
            raise NotSupportedException("no maxvalue of identity key constraints are not supported by SQream")
        if identity_options.cache is not None:
            raise NotSupportedException("cache of identity key constraints are not supported by SQream")
        if identity_options.order is not None:
            raise NotSupportedException("order of identity key constraints are not supported by SQream")
        if identity_options.cycle is not None:
            raise NotSupportedException("cycle of identity key constraints are not supported by SQream")


class NotSupportedException(Exception):
    pass

