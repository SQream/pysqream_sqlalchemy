import sys
sys.path.insert(0, 'pysqream_sqlalchemy')
sys.path.insert(0, 'tests')
from datetime import datetime, date
from decimal import Decimal
from typing import Union

import pytest
from sqlalchemy import text, Table, Column, dialects, Integer, Text, select, Connection, insert
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.engine.row import Row

from test_base import TestBaseCRUD


dialects.registry.register("pysqream.dialect", "dialect", "SqreamDialect")


class TestCreate(TestBaseCRUD):

    def recreate_all_via_metadata(self):
        self.metadata.drop_all(bind=self.engine)
        self.metadata.create_all(bind=self.engine)

    def test_create_database(self):
        if self.database_name in self.get_databases(self.session):
            self.session.execute(text(f"drop database {self.database_name}"))

        self.session.execute(text(f"create database {self.database_name}"))

        databases_amount_old = len(self.get_databases(self.session))
        assert self.database_name in self.get_databases(self.session)

        self.session.execute(text(f"drop database {self.database_name}"))

        databases_amount_new = len(self.get_databases(self.session))
        assert self.database_name not in self.get_databases(self.session)
        assert databases_amount_new == databases_amount_old - 1

    def test_create_database_with_engine_context_manager(self):
        with self.engine.connect() as connection:
            if self.database_name in self.get_databases(self.session):
                connection.execute(text(f"drop database {self.database_name}"))

            connection.execute(text(f"create database {self.database_name}"))
            connection.commit()

            databases_amount_old = len(self.get_databases(self.session))
            assert self.database_name in self.get_databases(self.session)

            connection.execute(text(f"drop database {self.database_name}"))

            databases_amount_new = len(self.get_databases(self.session))
            assert self.database_name not in self.get_databases(self.session)
            assert databases_amount_new == databases_amount_old - 1

    def test_create_database_with_session_context_manager(self):
        new_session = sessionmaker(self.engine)
        with new_session.begin() as session:
            if self.database_name in self.get_databases(self.session):
                session.execute(text(f"drop database {self.database_name}"))

            session.execute(text(f"create database {self.database_name}"))

            databases_amount_old = len(self.get_databases(self.session))
            assert self.database_name in self.get_databases(self.session)

            session.execute(text(f"drop database {self.database_name}"))

            databases_amount_new = len(self.get_databases(self.session))
            assert self.database_name not in self.get_databases(self.session)
            assert databases_amount_new == databases_amount_old - 1

    def test_create_schema(self):
        if self.insp.has_schema(self.schema_name):
            self.session.execute(text(f"drop schema {self.schema_name}"))

        self.session.execute(text(f"create schema {self.schema_name}"))

        assert len(self.insp.get_schema_names()) == 2
        assert self.schema_name in self.insp.get_schema_names()

        self.session.execute(text(f"drop schema {self.schema_name}"))

        assert len(self.insp.get_schema_names()) == 1
        assert self.schema_name not in self.insp.get_schema_names()

    def test_create_schema_with_engine_context_manager(self):
        with self.engine.connect() as connection:
            if self.insp.has_schema(self.schema_name):
                connection.execute(text(f"drop schema {self.schema_name}"))

            connection.execute(text(f"create schema {self.schema_name}"))

            assert len(self.insp.get_schema_names()) == 2
            assert self.schema_name in self.insp.get_schema_names()

            connection.execute(text(f"drop schema {self.schema_name}"))

            assert len(self.insp.get_schema_names()) == 1
            assert self.schema_name not in self.insp.get_schema_names()

    def test_create_schema_with_session_context_manager(self):
        new_session = sessionmaker(self.engine)
        with new_session.begin() as session:
            if self.insp.has_schema(self.schema_name):
                session.execute(text(f"drop schema {self.schema_name}"))

            session.execute(text(f"create schema {self.schema_name}"))

            assert len(self.insp.get_schema_names()) == 2
            assert self.schema_name in self.insp.get_schema_names()

            session.execute(text(f"drop schema {self.schema_name}"))

            assert len(self.insp.get_schema_names()) == 1
            assert self.schema_name not in self.insp.get_schema_names()

    def test_create_table(self, crud_table):
        self.recreate_all_via_metadata()

        table_names_with_crud = self.insp.get_table_names()
        assert self.table_name in table_names_with_crud

        crud_table.drop(bind=self.engine)

        table_names_without_crud = self.insp.get_table_names()
        assert len(table_names_without_crud) == len(table_names_with_crud) - 1
        assert self.table_name not in table_names_without_crud

    def test_create_table_with_engine_context_manager(self, crud_table):
        with self.engine.connect() as connection:
            self.recreate_all_via_metadata()

            table_names_with_crud = self.insp.get_table_names()
            assert self.table_name in table_names_with_crud

            crud_table.drop(bind=connection)

            table_names_without_crud = self.insp.get_table_names()
            assert len(table_names_without_crud) == len(table_names_with_crud) - 1
            assert self.table_name not in table_names_without_crud

    def test_create_table_with_session_context_manager(self, crud_table):
        new_session = sessionmaker(self.engine)
        with new_session.begin() as session:
            self.recreate_all_via_metadata()

            table_names_with_crud = self.insp.get_table_names()
            assert self.table_name in table_names_with_crud

            crud_table.drop(bind=session.connection())

            table_names_without_crud = self.insp.get_table_names()
            assert len(table_names_without_crud) == len(table_names_with_crud) - 1
            assert self.table_name not in table_names_without_crud

    def test_create_view(self, crud_table):
        self.recreate_all_via_metadata()

        self.session.execute(text(f"create view {self.view_name} as select * from {self.table_name}"))

        views_old = self.insp.get_view_names()
        assert self.view_name in views_old

        self.session.execute(text(f"drop view {self.view_name}"))
        views_new = self.insp.get_view_names()

        assert len(views_new) == len(views_old) - 1
        assert self.view_name not in views_new

    def test_create_view_with_engine_context_manager(self, crud_table):
        with self.engine.connect() as connection:
            self.recreate_all_via_metadata()

            connection.execute(text(f"create view {self.view_name} as select * from {self.table_name}"))

            views_old = self.insp.get_view_names()
            assert self.view_name in views_old

            connection.execute(text(f"drop view {self.view_name}"))
            views_new = self.insp.get_view_names()

            assert len(views_new) == len(views_old) - 1
            assert self.view_name not in views_new

    # @pytest.mark.skip(reason="Class 'sqlalchemy.sql.schema.Table' is not mapped: session.add(obj) isn't working")
    def test_create_view_with_session_context_manager(self, crud_table):
        new_session = sessionmaker(self.engine)
        with new_session.begin() as session:
            self.recreate_all_via_metadata()

            session.execute(text(f"create view {self.view_name} as select * from {self.table_name}"))

            views_old = self.insp.get_view_names()
            assert self.view_name in views_old

            session.execute(text(f"drop view {self.view_name}"))
            views_new = self.insp.get_view_names()

            assert len(views_new) == len(views_old) - 1
            assert self.view_name not in views_new


class TestRead(TestBaseCRUD):

    def insert_values_into_table(self,
                                 table_object: Table,
                                 rows_amount: int = 1,
                                 executor: Union[Session, Connection] = None,
                                 values: list[tuple] = None) -> None:
        if not values:
            values = [self.get_random_row_values_for_crud_table(i) for i in range(1, rows_amount + 1)]

        insert_statement = table_object.insert().values(values)
        if not executor:
            self.session.execute(insert_statement)
        else:
            executor.execute(insert_statement)

    @pytest.mark.parametrize(
        ("statement", "expected_result"),
        (
            ("select(crud_table)", 100),
            ("select(crud_table).where(text('i>50'))", 50),
        )
    )
    def test_read_from_table(self, crud_table, statement, expected_result):
        self.drop_crud_table_or_view_if_exists(crud_table)

        crud_table.create(bind=self.engine)

        self.insert_values_into_table(crud_table, rows_amount=100)

        select_statement = eval(statement)
        result1 = self.session.execute(select_statement).fetchall()
        result2 = self.session.execute(select_statement).scalars().all()

        assert len(result1) == expected_result == len(result2)

    @pytest.mark.parametrize(
        ("statement", "expected_result"),
        (
            ("select(crud_table)", 100),
            ("select(crud_table).where(text('i>50'))", 50),
        )
    )
    def test_read_from_table_with_engine_context_manager(self, crud_table, statement, expected_result):
        with self.engine.connect() as connection:
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=connection)

            crud_table.create(bind=connection)

            self.insert_values_into_table(crud_table, executor=connection, rows_amount=100)

            select_statement = eval(statement)
            result1 = self.session.execute(select_statement).fetchall()
            result2 = self.session.execute(select_statement).scalars().all()

            assert len(result1) == expected_result == len(result2)

    @pytest.mark.parametrize(
        ("statement", "expected_result"),
        (
            ("select(crud_table)", 100),
            ("select(crud_table).where(text('i>50'))", 50),
        )
    )
    def test_read_from_table_with_session_context_manager(self, crud_table, statement, expected_result):
        new_session = sessionmaker(self.engine)
        with new_session.begin() as session:
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=session.connection())

            crud_table.create(bind=session.connection())

            self.insert_values_into_table(crud_table, executor=session, rows_amount=100)

            select_statement = eval(statement)
            result1 = self.session.execute(select_statement).fetchall()
            result2 = self.session.execute(select_statement).scalars().all()

            assert len(result1) == expected_result == len(result2)

    def test_read_from_view(self, crud_table):
        self.drop_crud_table_or_view_if_exists(crud_table, drop_view=True)

        crud_table.create(bind=self.engine)

        self.session.execute(text(f"create view {self.view_name} as select * from {self.table_name}"))

        self.insert_values_into_table(crud_table, rows_amount=10)

        select_statement = select(crud_table)
        result1 = self.session.execute(select_statement).fetchall()
        result2 = self.session.execute(select_statement).scalars().all()

        assert len(result1) == 10 == len(result2)

    def test_read_from_view_with_engine_context_manager(self, crud_table):
        with self.engine.connect() as connection:
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=connection)
            if self.view_name in self.insp.get_view_names():
                connection.execute(text(f"drop view {self.view_name}"))

            crud_table.create(bind=connection)

            self.session.execute(text(f"create view {self.view_name} as select * from {self.table_name}"))

            self.insert_values_into_table(crud_table, rows_amount=10, executor=connection)

            select_statement = select(crud_table)
            result1 = connection.execute(select_statement).fetchall()
            result2 = connection.execute(select_statement).scalars().all()

            assert len(result1) == 10 == len(result2)

    def test_read_from_view_with_session_context_manager(self, crud_table):
        new_session = sessionmaker(self.engine)
        with new_session.begin() as session:
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=session.connection())
            if self.view_name in self.insp.get_view_names():
                session.execute(text(f"drop view {self.view_name}"))

            crud_table.create(bind=session.connection())

            self.session.execute(text(f"create view {self.view_name} as select * from {self.table_name}"))

            self.insert_values_into_table(crud_table, rows_amount=10, executor=session)

            select_statement = select(crud_table)
            result1 = session.execute(select_statement).fetchall()
            result2 = session.execute(select_statement).scalars().all()

            assert len(result1) == 10 == len(result2)

    @pytest.mark.parametrize(
        ('query', "entity_name"),
        (
            ("select * from sqream_catalog.tables where table_name = '{}'", TestBaseCRUD.table_name),
            ("select * from sqream_catalog.views where view_name = '{}'", TestBaseCRUD.view_name),
            ("select * from sqream_catalog.chunks join sqream_catalog.tables on "
             "sqream_catalog.tables.table_id = sqream_catalog.chunks.table_id where table_name = '{}'",
             TestBaseCRUD.table_name),
        )
    )
    def test_read_from_sqream_catalog(self, crud_table, query, entity_name):
        self.drop_crud_table_or_view_if_exists(crud_table, drop_view=True)

        crud_table.create(bind=self.engine)
        self.session.execute(text(f"create view {self.view_name} as select * from {self.table_name}"))

        self.insert_values_into_table(crud_table, rows_amount=10)

        statement = text(query.format(entity_name))
        result1 = self.session.execute(statement).fetchall()
        result2 = self.session.execute(statement).scalars().all()

        assert isinstance(result1, list)
        assert isinstance(result2, list)
        assert len(result1) == 1 == len(result2)

    @pytest.mark.parametrize(
        ('query', "entity_name"),
        (
            ("select * from sqream_catalog.tables where table_name = '{}'", TestBaseCRUD.table_name),
            ("select * from sqream_catalog.views where view_name = '{}'", TestBaseCRUD.view_name),
            ("select * from sqream_catalog.chunks join sqream_catalog.tables on "
             "sqream_catalog.tables.table_id = sqream_catalog.chunks.table_id where table_name = '{}'",
             TestBaseCRUD.table_name),
        )
    )
    def test_read_from_sqream_catalog_with_engine_context_manager(self, crud_table, query, entity_name):
        with self.engine.connect() as connection:
            if self.view_name in self.insp.get_view_names():
                connection.execute(text(f"drop view {self.view_name}"))
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=connection)

            crud_table.create(bind=connection)
            connection.execute(text(f"create view {self.view_name} as select * from {self.table_name}"))

            self.insert_values_into_table(crud_table, rows_amount=10, executor=connection)

            statement = text(query.format(entity_name))
            result1 = connection.execute(statement).fetchall()
            result2 = connection.execute(statement).scalars().all()

            assert isinstance(result1, list)
            assert isinstance(result2, list)
            assert len(result1) == 1 == len(result2)

    @pytest.mark.parametrize(
        ('query', "entity_name"),
        (
            ("select * from sqream_catalog.tables where table_name = '{}'", TestBaseCRUD.table_name),
            ("select * from sqream_catalog.views where view_name = '{}'", TestBaseCRUD.view_name),
            ("select * from sqream_catalog.chunks join sqream_catalog.tables on "
             "sqream_catalog.tables.table_id = sqream_catalog.chunks.table_id where table_name = '{}'",
             TestBaseCRUD.table_name),
        )
    )
    def test_read_from_sqream_catalog_with_session_context_manager(self, crud_table, query, entity_name):
        new_session = sessionmaker(self.engine)
        with new_session.begin() as session:
            if self.view_name in self.insp.get_view_names():
                session.execute(text(f"drop view {self.view_name}"))
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=session.connection())

            crud_table.create(bind=session.connection())
            session.execute(text(f"create view {self.view_name} as select * from {self.table_name}"))

            self.insert_values_into_table(crud_table, rows_amount=10, executor=session)

            statement = text(query.format(entity_name))
            result1 = session.execute(statement).fetchall()
            result2 = session.execute(statement).scalars().all()

            assert isinstance(result1, list)
            assert isinstance(result2, list)
            assert len(result1) == 1 == len(result2)

    def test_read_with_join(self, crud_table):
        table1 = Table(
            'table1',
            self.metadata,
            Column("i", Integer),
            Column("t", Text),
        )

        self.drop_crud_table_or_view_if_exists(crud_table)
        if self.insp.has_table(table1.name):
            table1.drop(bind=self.engine)

        crud_table.create(bind=self.engine)
        table1.create(bind=self.engine)

        values1 = [(i, 't' * i) for i in range(1, 11)]
        values2 = [self.get_random_row_values_for_crud_table(i) for i in range(1, 11)]
        self.insert_values_into_table(crud_table, rows_amount=10, values=values2)
        self.insert_values_into_table(table1, rows_amount=10, values=values1)

        res1 = self.session.execute(select(crud_table)).fetchall()
        res2 = self.session.execute(select(crud_table)).scalars().all()
        assert res1 == values2
        assert len(res1) == len(res2)

        joined = crud_table.join(table1, crud_table.c.i == table1.c.i, isouter=False)
        statement = select(joined)
        joined_res = self.session.execute(statement).fetchall()
        assert len(joined_res) == len(values1)

    def test_read_with_join_with_engine_context_manager(self, crud_table):
        table1 = Table(
            'table1',
            self.metadata,
            Column("i", Integer),
            Column("t", Text),
        )
        with self.engine.connect() as connection:
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=connection)
            if self.insp.has_table(table1.name):
                table1.drop(bind=connection)

            crud_table.create(bind=connection)
            table1.create(bind=connection)

            values1 = [(i, 't' * i) for i in range(1, 11)]
            values2 = [self.get_random_row_values_for_crud_table(i) for i in range(1, 11)]
            self.insert_values_into_table(crud_table, rows_amount=10, values=values2, executor=connection)
            self.insert_values_into_table(table1, rows_amount=10, values=values1, executor=connection)

            res1 = connection.execute(select(crud_table)).fetchall()
            res2 = connection.execute(select(crud_table)).scalars().all()
            assert res1 == values2
            assert len(res1) == len(res2)

            joined = crud_table.join(table1, crud_table.c.i == table1.c.i, isouter=False)
            statement = select(joined)
            joined_res = connection.execute(statement).fetchall()
            assert len(joined_res) == len(values1)

    def test_read_with_join_with_session_context_manager(self, crud_table):
        new_session = sessionmaker(self.engine)
        table1 = Table(
            'table1',
            self.metadata,
            Column("i", Integer),
            Column("t", Text),
        )
        with new_session.begin() as session:
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=session.connection())
            if self.insp.has_table(table1.name):
                table1.drop(bind=session.connection())

            crud_table.create(bind=session.connection())
            table1.create(bind=session.connection())

            values1 = [(i, 't' * i) for i in range(1, 11)]
            values2 = [self.get_random_row_values_for_crud_table(i) for i in range(1, 11)]
            self.insert_values_into_table(crud_table, rows_amount=10, values=values2, executor=session)
            self.insert_values_into_table(table1, rows_amount=10, values=values1, executor=session)

            res1 = session.execute(select(crud_table)).fetchall()
            res2 = session.execute(select(crud_table)).scalars().all()
            assert res1 == values2
            assert len(res1) == len(res2)

            joined = crud_table.join(table1, crud_table.c.i == table1.c.i, isouter=False)
            statement = select(joined)
            joined_res = session.execute(statement).fetchall()
            assert len(joined_res) == len(values1)

    def test_read_with_filter(self, crud_table):
        self.drop_crud_table_or_view_if_exists(crud_table)

        crud_table.create(bind=self.engine)

        self.insert_values_into_table(crud_table, rows_amount=10)

        row_i_5 = self.session.execute(select(crud_table).filter(text("i=5"))).scalar_one()
        assert isinstance(row_i_5, int)
        assert row_i_5 == 5

        row = self.session.execute(select(crud_table).filter(text("d>'1900-01-10'"))).first()
        assert isinstance(row, Row)
        assert len(row) == len(crud_table.c)

    def test_read_with_filter_with_engine_context_manager(self, crud_table):
        with self.engine.connect() as connection:
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=connection)

            crud_table.create(bind=connection)

            self.insert_values_into_table(crud_table, rows_amount=10, executor=connection)

            row_i_5 = connection.execute(select(crud_table).filter(text("i=5"))).scalar_one()
            assert isinstance(row_i_5, int)
            assert row_i_5 == 5

            row = connection.execute(select(crud_table).filter(text("d>'1900-01-10'"))).first()
            assert isinstance(row, Row)
            assert len(row) == len(crud_table.c)

    def test_read_with_filter_with_session_context_manager(self, crud_table):
        new_session = sessionmaker(self.engine)
        with new_session.begin() as session:
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=session.connection())

            crud_table.create(bind=session.connection())

            self.insert_values_into_table(crud_table, rows_amount=10, executor=session)

            row_i_5 = session.execute(select(crud_table).filter(text("i=5"))).scalar_one()
            assert isinstance(row_i_5, int)
            assert row_i_5 == 5

            row = session.execute(select(crud_table).filter(text("d>'1900-01-10'"))).first()
            assert isinstance(row, Row)
            assert len(row) == len(crud_table.c)


class TestUpdate(TestBaseCRUD):

    def test_session_add_delete(self, crud_table_row):
        self.Base.metadata.drop_all(bind=self.engine)
        self.Base.metadata.create_all(bind=self.engine)

        row = crud_table_row(i=1,
                             b=True,
                             d=date(2024, 1, 10),
                             dt=datetime.now(),
                             n=Decimal("123456789.123456"),
                             t="TEXT")
        self.session.add(row)
        ids = self.session.scalars(select(crud_table_row)).all()
        assert len(ids) == 1

    @pytest.mark.parametrize(
        "insert_statement",
        (
            "crud_table.insert().values(values)",
            "insert(crud_table).values(values)",
        )
    )
    def test_insert_into_table(self, crud_table, insert_statement):
        self.drop_crud_table_or_view_if_exists(crud_table)
        rows_amount = 100

        crud_table.create(bind=self.engine)
        values = [self.get_random_row_values_for_crud_table(i) for i in range(1, rows_amount + 1)]

        self.session.execute(eval(insert_statement))

        result1 = self.session.execute(select(crud_table)).fetchall()
        result2 = self.session.execute(select(crud_table)).scalars().all()

        assert len(result1) == rows_amount == len(result2)

    @pytest.mark.parametrize(
        "insert_statement",
        (
            "crud_table.insert().values(values)",
            "insert(crud_table).values(values)",
        )
    )
    def test_insert_into_table_with_engine_context_manager(self, crud_table, insert_statement):
        with (self.engine.connect() as connection):
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=connection)
            rows_amount = 100

            crud_table.create(bind=connection)
            values = [self.get_random_row_values_for_crud_table(i) for i in range(1, rows_amount + 1)]

            connection.execute(eval(insert_statement))

            result1 = connection.execute(select(crud_table)).fetchall()
            result2 = connection.execute(select(crud_table)).scalars().all()

            assert len(result1) == rows_amount == len(result2)

    @pytest.mark.parametrize(
        "insert_statement",
        (
            "crud_table.insert().values(values)",
            "insert(crud_table).values(values)",
        )
    )
    def test_insert_into_table_with_session_context_manager(self, crud_table, insert_statement):
        new_session = sessionmaker(self.engine)
        with new_session.begin() as session:
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=session.connection())
            rows_amount = 100

            crud_table.create(bind=session.connection())
            values = [self.get_random_row_values_for_crud_table(i) for i in range(1, rows_amount + 1)]

            session.execute(eval(insert_statement))

            result1 = session.execute(select(crud_table)).fetchall()
            result2 = session.execute(select(crud_table)).scalars().all()

            assert len(result1) == rows_amount == len(result2)

    def test_update_table(self, crud_table):
        self.drop_crud_table_or_view_if_exists(crud_table)

        crud_table.create(bind=self.engine)
        insert_statement = crud_table.insert().values([self.get_random_row_values_for_crud_table(1)])
        self.session.execute(insert_statement)

        self.session.execute(text(f"update {self.table_name} set i = 404 where i = 1"))

        row_i = self.session.execute(select(crud_table)).scalars().first()
        assert isinstance(row_i, int)
        assert row_i == 404

    def test_update_table_with_engine_context_manager(self, crud_table):
        with self.engine.connect() as connection:
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=connection)

            crud_table.create(bind=connection)
            insert_statement = crud_table.insert().values([self.get_random_row_values_for_crud_table(1)])
            connection.execute(insert_statement)

            connection.execute(text(f"update {self.table_name} set i = 404 where i = 1"))

            row_i = connection.execute(select(crud_table)).scalars().first()
            assert isinstance(row_i, int)
            assert row_i == 404

    def test_update_table_with_session_context_manager(self, crud_table):
        new_session = sessionmaker(self.engine)
        with new_session.begin() as session:
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=session.connection())

            crud_table.create(bind=session.connection())
            insert_statement = crud_table.insert().values([self.get_random_row_values_for_crud_table(1)])
            session.execute(insert_statement)

            session.execute(text(f"update {self.table_name} set i = 404 where i = 1"))

            row_i = session.execute(select(crud_table)).scalars().first()
            assert isinstance(row_i, int)
            assert row_i == 404


class TestDelete(TestBaseCRUD):

    def test_delete_database(self):
        if self.database_name not in self.get_databases(self.session):
            self.session.execute(text(f"create database {self.database_name}"))

        databases_amount_old = len(self.get_databases(self.session))
        self.session.execute(text(f"drop database {self.database_name}"))

        databases_amount_new = len(self.get_databases(self.session))
        assert self.database_name not in self.get_databases(self.session)
        assert databases_amount_new == databases_amount_old - 1

    def test_delete_database_with_engine_context_manager(self):
        with self.engine.connect() as connection:
            if self.database_name not in self.get_databases(self.session):
                connection.execute(text(f"create database {self.database_name}"))

            databases_amount_old = len(self.get_databases(self.session))
            connection.execute(text(f"drop database {self.database_name}"))

            databases_amount_new = len(self.get_databases(self.session))
            assert self.database_name not in self.get_databases(self.session)
            assert databases_amount_new == databases_amount_old - 1

    def test_delete_database_with_session_context_manager(self):
        new_session = sessionmaker(self.engine)
        with new_session.begin() as session:
            if self.database_name not in self.get_databases(self.session):
                session.execute(text(f"create database {self.database_name}"))

            databases_amount_old = len(self.get_databases(self.session))
            session.execute(text(f"drop database {self.database_name}"))

            databases_amount_new = len(self.get_databases(self.session))
            assert self.database_name not in self.get_databases(self.session)
            assert databases_amount_new == databases_amount_old - 1

    def test_delete_schema(self):
        if not self.insp.has_schema(self.schema_name):
            self.session.execute(text(f"create schema {self.schema_name}"))

        self.session.execute(text(f"drop schema {self.schema_name}"))

        assert len(self.insp.get_schema_names()) == 1
        assert self.schema_name not in self.insp.get_schema_names()

    def test_delete_schema_with_engine_context_manager(self):
        with self.engine.connect() as connection:
            if not self.insp.has_schema(self.schema_name):
                connection.execute(text(f"create schema {self.schema_name}"))

            connection.execute(text(f"drop schema {self.schema_name}"))

            assert len(self.insp.get_schema_names()) == 1
            assert self.schema_name not in self.insp.get_schema_names()

    def test_delete_schema_with_session_context_manager(self):
        new_session = sessionmaker(self.engine)
        with new_session.begin() as session:
            if not self.insp.has_schema(self.schema_name):
                session.execute(text(f"create schema {self.schema_name}"))

            session.execute(text(f"drop schema {self.schema_name}"))

            assert len(self.insp.get_schema_names()) == 1
            assert self.schema_name not in self.insp.get_schema_names()

    def test_delete_table(self, crud_table):
        if not self.insp.has_table(self.table_name):
            crud_table.create(bind=self.engine)

        table_names_with_crud = self.insp.get_table_names()
        crud_table.drop(bind=self.engine)

        table_names_without_crud = self.insp.get_table_names()
        assert len(table_names_without_crud) == len(table_names_with_crud) - 1
        assert self.table_name not in table_names_without_crud

    def test_delete_table_with_engine_context_manager(self, crud_table):
        with self.engine.connect() as connection:
            if not self.insp.has_table(self.table_name):
                crud_table.create(bind=connection)

            table_names_with_crud = self.insp.get_table_names()
            crud_table.drop(bind=connection)

            table_names_without_crud = self.insp.get_table_names()
            assert len(table_names_without_crud) == len(table_names_with_crud) - 1
            assert self.table_name not in table_names_without_crud

    def test_delete_table_with_session_context_manager(self, crud_table):
        new_session = sessionmaker(self.engine)
        with new_session.begin() as session:
            if not self.insp.has_table(self.table_name):
                crud_table.create(bind=session.connection())

            table_names_with_crud = self.insp.get_table_names()
            crud_table.drop(bind=session.connection())

            table_names_without_crud = self.insp.get_table_names()
            assert len(table_names_without_crud) == len(table_names_with_crud) - 1
            assert self.table_name not in table_names_without_crud

    def test_delete_view(self, crud_table):
        if self.table_name not in self.insp.get_table_names():
            crud_table.create(bind=self.engine)
        if self.view_name not in self.insp.get_view_names():
            self.session.execute(text(f"create view {self.view_name} as select * from {self.table_name}"))

        views_old = self.insp.get_view_names()
        self.session.execute(text(f"drop view {self.view_name}"))
        crud_table.drop(bind=self.engine)

        views_new = self.insp.get_view_names()

        assert len(views_new) == len(views_old) - 1
        assert self.view_name not in views_new

    def test_delete_view_with_engine_context_manager(self, crud_table):
        with self.engine.connect() as connection:
            if self.table_name not in self.insp.get_table_names():
                crud_table.create(bind=connection)
            if self.view_name not in self.insp.get_view_names():
                connection.execute(text(f"create view {self.view_name} as select * from {self.table_name}"))

            views_old = self.insp.get_view_names()
            connection.execute(text(f"drop view {self.view_name}"))
            crud_table.drop(bind=connection)

            views_new = self.insp.get_view_names()

            assert len(views_new) == len(views_old) - 1
            assert self.view_name not in views_new

    def test_delete_view_with_session_context_manager(self, crud_table):
        new_session = sessionmaker(self.engine)
        with new_session.begin() as session:
            if self.table_name not in self.insp.get_table_names():
                crud_table.create(bind=session.connection())
            if self.view_name not in self.insp.get_view_names():
                session.execute(text(f"create view {self.view_name} as select * from {self.table_name}"))

            views_old = self.insp.get_view_names()
            session.execute(text(f"drop view {self.view_name}"))
            crud_table.drop(bind=session.connection())

            views_new = self.insp.get_view_names()

            assert len(views_new) == len(views_old) - 1
            assert self.view_name not in views_new


class TestUtilityFunctions(TestBaseCRUD):
    """
    I took all utility functions below from:
    https://docs.sqream.com/en/latest/search.html?q=utility&check_keywords=yes&area=default
    """

    @pytest.mark.parametrize(
        "query",
        (
                "select show_node_info(0)",
                "select show_locks()",
                "select show_server_status()",
                "select show_connections()",
                f"select get_data_metrics('daily', '{datetime.now()}', '{datetime.now()}')",
                "select get_gpu_info()",
                "select show_last_node_info()",
        )
    )
    def test_most_used_utility_functions(self, query):
        result = self.session.execute(text(query)).fetchall()
        assert isinstance(result, list)
        assert len(result) >= 0

    @pytest.mark.parametrize("query", ("select get_ddl('{}')", "select get_statement_permissions('select * from {}')"))
    def test_get_ddl_and_statement_permissions(self, crud_table, query):
        self.drop_crud_table_or_view_if_exists(crud_table)
        crud_table.create(bind=self.engine)
        result = self.session.execute(text(query.format(self.table_name))).first()
        assert isinstance(result, Row)
        assert len(result) in (1, 3)

    @pytest.mark.parametrize("query", ("select get_ddl('{}')", "select get_statement_permissions('select * from {}')"))
    def test_get_ddl_and_statement_permissions_with_engine_context_manager(self, crud_table, query):
        with self.engine.connect() as connection:
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=connection)

            crud_table.create(bind=connection)
            result = connection.execute(text(query.format(self.table_name))).first()
            assert isinstance(result, Row)
            assert len(result) in (1, 3)

    @pytest.mark.parametrize("query", ("select get_ddl('{}')", "select get_statement_permissions('select * from {}')"))
    def test_get_ddl_and_statement_permissions_with_session_context_manager(self, crud_table, query):
        new_session = sessionmaker(self.engine)
        with new_session.begin() as session:
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=session.connection())

            crud_table.create(bind=session.connection())
            result = session.execute(text(query.format(self.table_name))).first()
            assert isinstance(result, Row)
            assert len(result) in (1, 3)

    def test_negative_utility_functions(self):
        with pytest.raises(Exception) as error:
            self.session.execute(text("select stop_statement(0)")).fetchall()
        assert "stop_statement could not find a statement with id 0" in str(error)

    def test_negative_utility_functions_with_engine_context_manager(self):
        with pytest.raises(Exception) as error:
            with self.engine.connect() as connection:
                connection.execute(text("select stop_statement(0)")).fetchall()
        assert "stop_statement could not find a statement with id 0" in str(error)

    def test_negative_utility_functions_with_session_context_manager(self):
        new_session = sessionmaker(self.engine)
        with pytest.raises(Exception) as error:
            with new_session.begin() as session:
                session.execute(text("select stop_statement(0)")).fetchall()
        assert "stop_statement could not find a statement with id 0" in str(error)
