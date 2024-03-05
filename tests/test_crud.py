import sys
sys.path.insert(0, 'pysqream_sqlalchemy')
sys.path.insert(0, 'tests')
from datetime import datetime, date
from decimal import Decimal
from random import choice, randint, choices

import pytest
from sqlalchemy import text, Table, Column, dialects, orm, Integer, Boolean, Date, DateTime, Numeric, Text, select, update, insert
from sqlalchemy.engine.row import Row

from test_base import TestBase


dialects.registry.register("pysqream.dialect", "dialect", "SqreamDialect")


class TestBaseCRUD(TestBase):
    database_name = schema_name = table_name = 'crud'
    view_name = "view_for_crud"

    @staticmethod
    def get_databases(connection: orm.Session) -> list[str]:
        query = text("select database_name from sqream_catalog.databases")
        databases = connection.execute(query).fetchall()
        if databases:
            return [d[0] for d in databases]
        return []

    @pytest.fixture
    def crud_table(self):
        return Table(
            self.table_name,
            self.metadata,
            Column('i', Integer),
            Column('b', Boolean),
            Column('d', Date),
            Column('dt', DateTime),
            Column('n', Numeric(15, 6)),
            Column('t', Text),
            # Column('iar', ARRAY(Integer)),
            # Column('bar', ARRAY(Boolean)),
            # Column('dar', ARRAY(Date)),
            # Column('dtar',ARRAY(DateTime)),
            # Column('nar', ARRAY(Numeric(15, 6))),
            # Column('tar', ARRAY(Text)),
            extend_existing=True
        )

    @staticmethod
    def get_random_row_values_for_crud_table(row_number: int):
        return (
            row_number,
            choice((True, False)),
            date(year=randint(2000, 2024), month=randint(1, 12), day=randint(1, 28)),
            datetime(year=randint(2000, 2024),
                     month=randint(1, 12),
                     day=randint(1, 28),
                     hour=randint(1, 23),
                     minute=randint(1, 59),
                     second=randint(1, 59)),
            Decimal(f"{randint(int(1e8), int(9e8))}.{randint(int(1e5), int(9e5))}"),
            "".join(choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=randint(5, 50)))
        )

    def drop_crud_table_or_view_if_exists(self, crud_table, drop_table: bool = True, drop_view: bool = False) -> None:
        if drop_view:
            if self.view_name in self.insp.get_view_names():
                self.session.execute(text(f"drop view {self.view_name}"))
        if drop_table:
            if self.insp.has_table(self.table_name):
                crud_table.drop(bind=self.session.connection())


class TestCreate(TestBaseCRUD):

    def test_create_database(self):
        if self.database_name in self.get_databases(self.session):
            query = f"drop database {self.database_name}"
            self.session.execute(text(query))

        self.session.execute(text(f"create database {self.database_name}"))

        databases_amount_old = len(self.get_databases(self.session))
        assert self.database_name in self.get_databases(self.session)

        self.session.execute(text(f"drop database {self.database_name}"))

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

    def test_create_table(self, crud_table):
        self.drop_crud_table_or_view_if_exists(crud_table)

        crud_table.create(bind=self.session.connection())

        table_names_with_crud = self.insp.get_table_names()
        assert self.table_name in table_names_with_crud

        crud_table.drop(bind=self.session.connection())

        table_names_without_crud = self.insp.get_table_names()
        assert len(table_names_without_crud) == len(table_names_with_crud) - 1
        assert self.table_name not in table_names_without_crud

    def test_create_view(self, crud_table):
        self.drop_crud_table_or_view_if_exists(crud_table, drop_view=True)
        crud_table.create(bind=self.session.connection())

        self.session.execute(text(f"create view {self.view_name} as select * from {self.table_name}"))

        views_old = self.insp.get_view_names()
        assert self.view_name in views_old

        self.session.execute(text(f"drop view {self.view_name}"))
        views_new = self.insp.get_view_names()

        assert len(views_new) == len(views_old) - 1
        assert self.view_name not in views_new


class TestRead(TestBaseCRUD):

    @pytest.mark.parametrize(
        ("statement", "expected_result"),
        (
            ("select(crud_table)", 100),
            ("select(crud_table).where(text('i>50'))", 50),
        )
    )
    def test_read_from_table(self, crud_table, statement, expected_result):
        self.drop_crud_table_or_view_if_exists(crud_table)
        rows_amount = 100

        crud_table.create(bind=self.session.connection())
        values = [self.get_random_row_values_for_crud_table(i) for i in range(1, rows_amount + 1)]

        insert_statement = crud_table.insert().values(values)
        self.session.execute(insert_statement)

        select_statement = eval(statement)
        result = self.session.execute(select_statement).fetchall()

        assert len(result) == expected_result

        crud_table.drop(bind=self.session.connection())

    def test_read_from_view(self, crud_table):
        self.drop_crud_table_or_view_if_exists(crud_table, drop_view=True)
        crud_table.create(bind=self.session.connection())

        self.session.execute(text(f"create view {self.view_name} as select * from {self.table_name}"))

        values = [self.get_random_row_values_for_crud_table(i) for i in range(1, 11)]
        insert_statement = crud_table.insert().values(values)
        self.session.execute(insert_statement)

        select_statement = select(crud_table)
        result = self.session.execute(select_statement).fetchall()

        assert len(result) == 10
        crud_table.drop(bind=self.session.connection())
        self.session.execute(text(f"drop view {self.view_name}"))

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
        crud_table.create(bind=self.session.connection())
        self.session.execute(text(f"create view {self.view_name} as select * from {self.table_name}"))

        values = [self.get_random_row_values_for_crud_table(i) for i in range(1, 11)]
        insert_statement = crud_table.insert().values(values)
        self.session.execute(insert_statement)

        statement = text(query.format(entity_name))
        result = self.session.execute(statement).fetchall()
        assert isinstance(result, list)
        assert len(result) == 1

        crud_table.drop(bind=self.session.connection())
        self.session.execute(text(f"drop view {self.view_name}"))

    def test_read_with_join(self, crud_table):
        table1 = Table(
            'table1',
            self.metadata,
            Column("i", Integer),
            Column("t", Text),
        )

        self.drop_crud_table_or_view_if_exists(crud_table)
        if self.insp.has_table(table1.name):
            table1.drop(bind=self.session.connection())

        crud_table.create(bind=self.session.connection())
        table1.create(bind=self.session.connection())

        values1 = [(i, 't' * i) for i in range(1, 11)]
        values2 = [self.get_random_row_values_for_crud_table(i) for i in range(1, 11)]
        insert_statement1 = insert(table1).values(values1)
        insert_statement2 = insert(crud_table).values(values2)
        self.session.execute(insert_statement1)
        self.session.execute(insert_statement2)

        res = self.session.execute(select(crud_table)).fetchall()
        assert res == values2

        joined = crud_table.join(table1, crud_table.c.i == table1.c.i, isouter=False)
        statement = select(joined)
        joined_res = self.session.execute(statement).fetchall()
        assert len(joined_res) == len(values1)


class TestUpdate(TestBaseCRUD):

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

        crud_table.create(bind=self.session.connection())
        values = [self.get_random_row_values_for_crud_table(i) for i in range(1, rows_amount + 1)]

        self.session.execute(eval(insert_statement))

        result = self.session.execute(select(crud_table)).fetchall()

        assert len(result) == rows_amount

        crud_table.drop(bind=self.session.connection())

    @pytest.mark.skip(reason="Where clause of parameterized query not supported on SQream")
    def test_update_table(self, crud_table):
        self.drop_crud_table_or_view_if_exists(crud_table)

        crud_table.create(bind=self.session.connection())
        insert_statement = crud_table.insert().values([self.get_random_row_values_for_crud_table(1)])
        self.session.execute(insert_statement)

        # statement = update(crud_table).where(text("i=1")).values(t="new_text")
        # statement = update(crud_table).values(t="new_text")
        statement = crud_table.update().values(t="new_text")
        # statement = select(crud_table).where(text("i=1"))
        self.session.execute(statement)

        row = self.session.execute(select(crud_table)).first()
        print(row)

        crud_table.drop(bind=self.session.connection())


class TestDelete(TestBaseCRUD):

    def test_delete_database(self):
        if self.database_name not in self.get_databases(self.session):
            self.session.execute(text(f"create database {self.database_name}"))

        databases_amount_old = len(self.get_databases(self.session))
        self.session.execute(text(f"drop database {self.database_name}"))

        databases_amount_new = len(self.get_databases(self.session))
        assert self.database_name not in self.get_databases(self.session)
        assert databases_amount_new == databases_amount_old - 1

    def test_delete_schema(self):
        if not self.insp.has_schema(self.schema_name):
            self.session.execute(text(f"create schema {self.schema_name}"))

        self.session.execute(text(f"drop schema {self.schema_name}"))

        assert len(self.insp.get_schema_names()) == 1
        assert self.schema_name not in self.insp.get_schema_names()

    def test_delete_table(self, crud_table):
        if not self.insp.has_table(self.table_name):
            crud_table.create(bind=self.session.connection())

        table_names_with_crud = self.insp.get_table_names()
        crud_table.drop(bind=self.session.connection())

        table_names_without_crud = self.insp.get_table_names()
        assert len(table_names_without_crud) == len(table_names_with_crud) - 1
        assert self.table_name not in table_names_without_crud

    def test_delete_view(self, crud_table):
        if self.view_name not in self.insp.get_view_names():
            crud_table.create(bind=self.session.connection())
            self.session.execute(text(f"create view {self.view_name} as select * from {self.table_name}"))

        views_old = self.insp.get_view_names()
        self.session.execute(text(f"drop view {self.view_name}"))
        crud_table.drop(bind=self.session.connection())

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
        crud_table.create(bind=self.session.connection())
        result = self.session.execute(text(query.format(self.table_name))).first()
        assert isinstance(result, Row)
        assert len(result) in (1, 3)
        crud_table.drop(bind=self.session.connection())

    def test_negative_utility_functions(self):
        with pytest.raises(Exception) as error:
            self.session.execute(text("select stop_statement(0)")).fetchall()
        assert "stop_statement could not find a statement with id 0" in str(error)
