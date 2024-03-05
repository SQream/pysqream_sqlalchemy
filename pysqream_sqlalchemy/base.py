from sqlalchemy.sql import compiler, crud, elements
from sqlalchemy import exc
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.sql.compiler import FUNCTIONS, OPERATORS


NOT_SUPPORTED_OPERATORS = ['NULLS FIRST', 'NULLS LAST']
NOT_SUPPORTED_FUNCTIONS = ['aggregate_strings', 'cube', 'current_time', 'current_user', 'grouping_sets', 'localtime',
                           'next_value', 'random', 'rollup', 'session_user', 'user']
NOT_SUPPORTED_PARAMETERIZED_FUNCTIONS = ['char_length', 'coalesce', 'concat', 'percentile_cont', 'percentile_disc',
                                         'nth_value', 'ntile']


class TINYINT(TINYINT):
    """
        Allows describing tables via the ORM mechanism.
        Complemented in SqreamTypeCompiler
    """

    pass


class SqreamTypeCompiler(compiler.GenericTypeCompiler):
    """ Get the SQream string names for SQLAlchemy types, useful for ORM
        generated Create queries """

    def visit_BOOLEAN(self, type_, **kw):
        return "BOOL"

    def visit_TINYINT(self, type_, **kw):
        return "TINYINT"


class SqreamSQLCompiler(compiler.SQLCompiler):
    def visit_select(
        self,
        select_stmt,
        asfrom=False,
        insert_into=False,
        fromhints=None,
        compound_index=None,
        select_wraps_for=None,
        lateral=False,
        from_linter=None,
        **kwargs
    ):
        assert select_wraps_for is None, (
            "SQLAlchemy 1.4 requires use of "
            "the translate_select_structure hook for structural "
            "translations of SELECT objects"
        )

        # initial setup of SELECT.  the compile_state_factory may now
        # be creating a totally different SELECT from the one that was
        # passed in.  for ORM use this will convert from an ORM-state
        # SELECT to a regular "Core" SELECT.  other composed operations
        # such as computation of joins will be performed.

        kwargs["within_columns_clause"] = False

        compile_state = select_stmt._compile_state_factory(
            select_stmt, self, **kwargs
        )
        kwargs["ambiguous_table_name_map"] = (
            compile_state._ambiguous_table_name_map
        )

        select_stmt = compile_state.statement

        if select_stmt.whereclause is not None and \
                (hasattr(select_stmt.whereclause, "left") and hasattr(select_stmt.whereclause, "right")) and (
                (hasattr(select_stmt.whereclause.left, "value")) or (hasattr(select_stmt.whereclause.right, "value"))):
            raise NotSupportedException("Where clause of parameterized query not supported on SQream")

        elif select_stmt.whereclause is not None and hasattr(select_stmt.whereclause, "whens"):
            raise NotSupportedException("Where clause of parameterized query not supported on SQream")

        toplevel = not self.stack

        if toplevel and not self.compile_state:
            self.compile_state = compile_state

        is_embedded_select = compound_index is not None or insert_into

        # translate step for Oracle, SQL Server which often need to
        # restructure the SELECT to allow for LIMIT/OFFSET and possibly
        # other conditions
        if self.translate_select_structure:
            new_select_stmt = self.translate_select_structure(
                select_stmt, asfrom=asfrom, **kwargs
            )

            # if SELECT was restructured, maintain a link to the originals
            # and assemble a new compile state
            if new_select_stmt is not select_stmt:
                compile_state_wraps_for = compile_state
                select_wraps_for = select_stmt
                select_stmt = new_select_stmt

                compile_state = select_stmt._compile_state_factory(
                    select_stmt, self, **kwargs
                )
                select_stmt = compile_state.statement

        entry = self._default_stack_entry if toplevel else self.stack[-1]

        populate_result_map = need_column_expressions = (
            toplevel
            or entry.get("need_result_map_for_compound", False)
            or entry.get("need_result_map_for_nested", False)
        )

        # indicates there is a CompoundSelect in play and we are not the
        # first select
        if compound_index:
            populate_result_map = False

        # this was first proposed as part of #3372; however, it is not
        # reached in current tests and could possibly be an assertion
        # instead.
        if not populate_result_map and "add_to_result_map" in kwargs:
            del kwargs["add_to_result_map"]

        froms = self._setup_select_stack(
            select_stmt, compile_state, entry, asfrom, lateral, compound_index
        )

        column_clause_args = kwargs.copy()
        column_clause_args.update(
            {"within_label_clause": False, "within_columns_clause": False}
        )

        text = "SELECT "  # we're off to a good start !

        if select_stmt._hints:
            hint_text, byfrom = self._setup_select_hints(select_stmt)
            if hint_text:
                text += hint_text + " "
        else:
            byfrom = None

        if select_stmt._independent_ctes:
            self._dispatch_independent_ctes(select_stmt, kwargs)

        if select_stmt._prefixes:
            text += self._generate_prefixes(
                select_stmt, select_stmt._prefixes, **kwargs
            )

        text += self.get_select_precolumns(select_stmt, **kwargs)
        # the actual list of columns to print in the SELECT column list.
        inner_columns = [
            c
            for c in [
                self._label_select_column(
                    select_stmt,
                    column,
                    populate_result_map,
                    asfrom,
                    column_clause_args,
                    name=name,
                    proxy_name=proxy_name,
                    fallback_label_name=fallback_label_name,
                    column_is_repeated=repeated,
                    need_column_expressions=need_column_expressions,
                )
                for (
                    name,
                    proxy_name,
                    fallback_label_name,
                    column,
                    repeated,
                ) in compile_state.columns_plus_names
            ]
            if c is not None
        ]

        if populate_result_map and select_wraps_for is not None:
            # if this select was generated from translate_select,
            # rewrite the targeted columns in the result map

            translate = dict(
                zip(
                    [
                        name
                        for (
                            key,
                            proxy_name,
                            fallback_label_name,
                            name,
                            repeated,
                        ) in compile_state.columns_plus_names
                    ],
                    [
                        name
                        for (
                            key,
                            proxy_name,
                            fallback_label_name,
                            name,
                            repeated,
                        ) in compile_state_wraps_for.columns_plus_names
                    ],
                )
            )

            self._result_columns = [
                compiler.ResultColumnsEntry(
                    key, name, tuple(translate.get(o, o) for o in obj), type_
                )
                for key, name, obj, type_ in self._result_columns
            ]

        text = self._compose_select_body(
            text,
            select_stmt,
            compile_state,
            inner_columns,
            froms,
            byfrom,
            toplevel,
            kwargs,
        )

        if select_stmt._statement_hints:
            per_dialect = [
                ht
                for (dialect_name, ht) in select_stmt._statement_hints
                if dialect_name in ("*", self.dialect.name)
            ]
            if per_dialect:
                text += " " + self.get_statement_hint_text(per_dialect)

        # In compound query, CTEs are shared at the compound level
        if self.ctes and (not is_embedded_select or toplevel):
            nesting_level = len(self.stack) if not toplevel else None
            text = self._render_cte_clause(nesting_level=nesting_level) + text

        if select_stmt._suffixes:
            text += " " + self._generate_prefixes(
                select_stmt, select_stmt._suffixes, **kwargs
            )

        self.stack.pop(-1)

        return text

    def visit_unary(
        self, unary, add_to_result_map=None, result_map_targets=(), **kw
    ):

        if add_to_result_map is not None:
            result_map_targets += (unary,)
            kw["add_to_result_map"] = add_to_result_map
            kw["result_map_targets"] = result_map_targets

        if unary.operator:
            if unary.modifier:
                raise exc.CompileError(
                    "Unary expression does not support operator "
                    "and modifier simultaneously"
                )

            disp = self._get_operator_dispatch(
                unary.operator, "unary", "operator"
            )
            if disp:
                return disp(unary, unary.operator, **kw)
            else:
                return self._generate_generic_unary_operator(
                    unary, OPERATORS[unary.operator], **kw
                )
        elif unary.modifier:
            if str(OPERATORS[unary.modifier]).strip() in NOT_SUPPORTED_OPERATORS:
                raise NotSupportedException(f"{str(OPERATORS[unary.modifier]).strip()} not supported on SQream")

            disp = self._get_operator_dispatch(
                unary.modifier, "unary", "modifier"
            )
            if disp:
                return disp(unary, unary.modifier, **kw)
            else:
                return self._generate_generic_unary_modifier(
                    unary, OPERATORS[unary.modifier], **kw
                )
        else:
            raise exc.CompileError(
                "Unary expression has no operator or modifier"
            )

    def visit_delete(self, delete_stmt, visiting_cte=None, **kw):
        compile_state = delete_stmt._compile_state_factory(
            delete_stmt, self, **kw
        )
        delete_stmt = compile_state.statement
        if delete_stmt.whereclause is not None and hasattr(delete_stmt.whereclause, "clauses"):
            for cla in delete_stmt.whereclause.clauses:
                if (hasattr(cla, "left") and hasattr(cla, "right")) and \
                        (hasattr(cla.left, "value") or (hasattr(cla.right, "value"))):
                    raise NotSupportedException("Where clause of parameterized query not supported on SQream")

        elif delete_stmt.whereclause is not None and \
                (hasattr(delete_stmt.whereclause, "left") and hasattr(delete_stmt.whereclause, "right")) and (
                (hasattr(delete_stmt.whereclause.left, "value")) or (hasattr(delete_stmt.whereclause.right, "value"))):
            raise NotSupportedException("Where clause of parameterized query not supported on SQream")

        if visiting_cte is not None:
            kw["visiting_cte"] = visiting_cte
            toplevel = False
        else:
            toplevel = not self.stack

        if toplevel:
            self.isdelete = True
            if not self.dml_compile_state:
                self.dml_compile_state = compile_state
            if not self.compile_state:
                self.compile_state = compile_state

        if self.linting & compiler.COLLECT_CARTESIAN_PRODUCTS:
            from_linter = compiler.FromLinter({}, set())
            warn_linting = self.linting & compiler.WARN_LINTING
            if toplevel:
                self.from_linter = from_linter
        else:
            from_linter = None
            warn_linting = False

        extra_froms = compile_state._extra_fromss

        correlate_froms = {delete_stmt.table}.union(extra_froms)
        self.stack.append(
            {
                "correlate_froms": correlate_froms,
                "asfrom_froms": correlate_froms,
                "selectable": delete_stmt,
            }
        )

        text = "DELETE "

        if delete_stmt._prefixes:
            text += self._generate_prefixes(
                delete_stmt, delete_stmt._prefixes, **kw
            )

        text += "FROM "

        try:
            table_text = self.delete_table_clause(
                delete_stmt,
                delete_stmt.table,
                extra_froms,
                from_linter=from_linter,
            )
        except TypeError:
            # anticipate 3rd party dialects that don't include **kw
            # TODO: remove in 2.1
            table_text = self.delete_table_clause(
                delete_stmt, delete_stmt.table, extra_froms
            )
            if from_linter:
                _ = self.process(delete_stmt.table, from_linter=from_linter)

        crud._get_crud_params(self, delete_stmt, compile_state, toplevel, **kw)

        if delete_stmt._hints:
            dialect_hints, table_text = self._setup_crud_hints(
                delete_stmt, table_text
            )
        else:
            dialect_hints = None

        if delete_stmt._independent_ctes:
            self._dispatch_independent_ctes(delete_stmt, kw)

        text += table_text

        if (self.implicit_returning or delete_stmt._returning) and self.returning_precedes_values:
            text += " " + self.returning_clause(
                delete_stmt,
                self.implicit_returning or delete_stmt._returning,
                populate_result_map=toplevel,
            )

        if extra_froms:
            extra_from_text = self.delete_extra_from_clause(
                delete_stmt,
                delete_stmt.table,
                extra_froms,
                dialect_hints,
                from_linter=from_linter,
                **kw,
            )
            if extra_from_text:
                text += " " + extra_from_text

        if delete_stmt._where_criteria:
            t = self._generate_delimited_and_list(
                delete_stmt._where_criteria, **kw
            )
            if t:
                text += " WHERE " + t

        if (self.implicit_returning or delete_stmt._returning) and not self.returning_precedes_values:
            text += " " + self.returning_clause(
                delete_stmt,
                self.implicit_returning or delete_stmt._returning,
                populate_result_map=toplevel,
            )

        if self.ctes:
            nesting_level = len(self.stack) if not toplevel else None
            text = self._render_cte_clause(nesting_level=nesting_level) + text

        if warn_linting:
            assert from_linter is not None
            from_linter.warn(stmt_type="DELETE")

        self.stack.pop(-1)

        return text

    def visit_update(self, update_stmt, visiting_cte=None, **kw):
        compile_state = update_stmt._compile_state_factory(
            update_stmt, self, **kw
        )

        update_stmt = compile_state.statement

        if visiting_cte is not None:
            kw["visiting_cte"] = visiting_cte
            toplevel = False
        else:
            toplevel = not self.stack

        if update_stmt.whereclause is not None and \
                (hasattr(update_stmt.whereclause, "left") and hasattr(update_stmt.whereclause, "right")) and (
                (hasattr(update_stmt.whereclause.left, "value")) or (hasattr(update_stmt.whereclause.right, "value"))):
            raise NotSupportedException("Where clause of parameterized query not supported on SQream")

        if toplevel:
            self.isupdate = True
            if not self.dml_compile_state:
                self.dml_compile_state = compile_state
            if not self.compile_state:
                self.compile_state = compile_state

        if self.linting & compiler.COLLECT_CARTESIAN_PRODUCTS:
            from_linter = compiler.FromLinter({}, set())
            warn_linting = self.linting & compiler.WARN_LINTING
            if toplevel:
                self.from_linter = from_linter
        else:
            from_linter = None
            warn_linting = False

        extra_froms = compile_state._extra_froms
        is_multitable = bool(extra_froms)

        if is_multitable:
            # main table might be a JOIN
            main_froms = set(compiler._from_objects(update_stmt.table))
            render_extra_froms = [
                f for f in extra_froms if f not in main_froms
            ]
            correlate_froms = main_froms.union(extra_froms)
        else:
            render_extra_froms = []
            correlate_froms = {update_stmt.table}

        self.stack.append(
            {
                "correlate_froms": correlate_froms,
                "asfrom_froms": correlate_froms,
                "selectable": update_stmt,
            }
        )

        text = "UPDATE "

        if update_stmt._prefixes:
            text += self._generate_prefixes(
                update_stmt, update_stmt._prefixes, **kw
            )

        table_text = self.update_tables_clause(
            update_stmt,
            update_stmt.table,
            render_extra_froms,
            from_linter=from_linter,
            **kw,
        )

        crud_params_struct = crud._get_crud_params(
            self, update_stmt, compile_state, toplevel, **kw
        )
        crud_params = crud_params_struct.single_params

        if update_stmt._hints:
            dialect_hints, table_text = self._setup_crud_hints(
                update_stmt, table_text
            )
        else:
            dialect_hints = None

        if update_stmt._independent_ctes:
            for cte in update_stmt._independent_ctes:
                cte._compiler_dispatch(self, **kw)

        text += table_text

        text += " SET "
        text += ", ".join(
            expr + "=" + value
            for _, expr, value, _ in compiler.cast(
                "List[Tuple[Any, str, str, Any]]", crud_params
            )
        )

        if self.implicit_returning or update_stmt._returning:
            if self.returning_precedes_values:
                text += " " + self.returning_clause(
                    update_stmt,
                    self.implicit_returning or update_stmt._returning,
                    populate_result_map=toplevel,
                )

        if extra_froms:
            extra_from_text = self.update_from_clause(
                update_stmt,
                update_stmt.table,
                render_extra_froms,
                dialect_hints,
                from_linter=from_linter,
                **kw,
            )
            if extra_from_text:
                text += " " + extra_from_text

        if update_stmt._where_criteria:
            t = self._generate_delimited_and_list(
                update_stmt._where_criteria, from_linter=from_linter, **kw
            )
            if t:
                text += " WHERE " + t

        limit_clause = self.update_limit_clause(update_stmt)
        if limit_clause:
            text += " " + limit_clause

        if (self.implicit_returning or update_stmt._returning) and not self.returning_precedes_values:
            text += " " + self.returning_clause(
                update_stmt,
                self.implicit_returning or update_stmt._returning,
                populate_result_map=toplevel,
            )

        if self.ctes:
            nesting_level = len(self.stack) if not toplevel else None
            text = self._render_cte_clause(nesting_level=nesting_level) + text

        if warn_linting:
            assert from_linter is not None
            from_linter.warn(stmt_type="UPDATE")

        self.stack.pop(-1)

        return text

    def visit_function(self, func, add_to_result_map=None, **kwargs):
        if func.name in NOT_SUPPORTED_FUNCTIONS:
            raise NotSupportedException(f"{func.name} function not supported on SQream")

        elif func.name in NOT_SUPPORTED_PARAMETERIZED_FUNCTIONS:
            raise NotSupportedException(f"{func.name} function with parameterized value is not supported on SQream")

        if add_to_result_map is not None:
            add_to_result_map(func.name, func.name, (), func.type)

        disp = getattr(self, "visit_%s_func" % func.name.lower(), None)
        text: str

        if disp:
            text = disp(func, **kwargs)
        else:
            name = FUNCTIONS.get(func._deannotate().__class__, None)
            if name:
                if func._has_args:
                    name += "%(expr)s"
            else:
                name = func.name
                name = (
                    self.preparer.quote(name)
                    if self.preparer._requires_quotes_illegal_chars(name)
                    or isinstance(name, elements.quoted_name)
                    else name
                )
                name = name + "%(expr)s"
            text = ".".join(
                [
                    (
                        self.preparer.quote(tok)
                        if self.preparer._requires_quotes_illegal_chars(tok)
                        or isinstance(name, elements.quoted_name)
                        else tok
                    )
                    for tok in func.packagenames
                ]
                + [name]
            ) % {"expr": self.function_argspec(func, **kwargs)}

        if func._with_ordinality:
            text += " WITH ORDINALITY"
        return text

    def visit_concat_func(self, concat, **kw):
        concat_str = " || "
        question_mark = []
        for cla in concat.expression.clauses:
            question_mark.append("?")
        return concat_str.join(question_mark)

    def visit_now_func(self, now, **kw):
        return "CURRENT_TIMESTAMP"

    def visit_localtimestamp_func(self, localtimestamp, **kw):
        return "CURRENT_TIMESTAMP"

    def visit_contains_op_binary(self, binary, operator, **kw):
        raise NotSupportedException("Like contains clause of parameterized query not supported on SQream")

    def visit_not_contains_op_binary(self, binary, operator, **kw):
        raise NotSupportedException("Not Like contains clause of parameterized query not supported on SQream")

    def visit_startswith_op_binary(self, binary, operator, **kw):
        raise NotSupportedException("Like startswith clause of parameterized query not supported on SQream")

    def visit_not_startswith_op_binary(self, binary, operator, **kw):
        raise NotSupportedException("Not Like startswith clause of parameterized query not supported on SQream")

    def visit_endswith_op_binary(self, binary, operator, **kw):
        raise NotSupportedException("Like endswith clause of parameterized query not supported on SQream")

    def visit_not_endswith_op_binary(self, binary, operator, **kw):
        raise NotSupportedException("Not Like endswith clause of parameterized query not supported on SQream")

    def visit_like_op_binary(self, binary, operator, **kw):
        raise NotSupportedException("Like clause of parameterized query not supported on SQream")

    def visit_not_like_op_binary(self, binary, operator, **kw):
        raise NotSupportedException("Not Like clause of parameterized query not supported on SQream")

    def visit_ilike_op_binary(self, binary, operator, **kw):
        raise NotSupportedException("ILike clause of parameterized query not supported on SQream")

    def visit_not_ilike_op_binary(self, binary, operator, **kw):
        raise NotSupportedException("Not ILike clause of parameterized query not supported on SQream")

    def visit_between_op_binary(self, binary, operator, **kw):
        raise NotSupportedException("Between clause of parameterized query not supported on SQream")

    def visit_not_between_op_binary(self, binary, operator, **kw):
        raise NotSupportedException("Not Between clause of parameterized query not supported on SQream")

    def visit_in_op_binary(self, binary, operator, **kw):
        raise NotSupportedException("In clause of parameterized query not supported on SQream")

    def visit_not_in_op_binary(self, binary, operator, **kw):
        raise NotSupportedException("Not In clause of parameterized query not supported on SQream")


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

