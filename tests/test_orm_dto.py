

import os, sys
sys.path.append(os.path.abspath(__file__).rsplit('tests/', 1)[0] + '/pysqream_sqlalchemy/')
sys.path.append(os.path.abspath(__file__).rsplit('tests/', 1)[0] + '/tests/')
from test_base import TestBaseOrm
from sqlalchemy import select, dialects, Table, Column, union_all
from sqlalchemy.sql import exists
from sqlalchemy.orm import aliased, Session
import sqlalchemy as sa
import pytest


dialects.registry.register("pysqream.dialect", "dialect", "SqreamDialect")


class TestOrmDto(TestBaseOrm):

    def test_select(self):

        expected_stmt = f"SELECT {self.table1.c.id}, {self.table1.c.name}, {self.table1.c.value} \nFROM {self.table1.name}"
        stmt = self.table1.select()
        assert expected_stmt == str(stmt)

        stmt = select(self.table1)
        assert expected_stmt == str(stmt)

    def test_select_one_col(self):

        expected_stmt = f"SELECT table1.id \nFROM {self.table1.name}"
        stmt = select([self.table1.c.id])
        assert expected_stmt == str(stmt)

    # Select Where not supported
    def test_select_where_not_supported(self):

        with pytest.raises(Exception) as e_info:
            print(self.table1.select().where(self.table1.c.id == '1'))

        assert "Where clause of parameterized query not supported on SQream" in str(e_info.value)

    def test_select_order_by(self):

        expected_stmt = f"SELECT {self.table1.c.id}, {self.table1.c.name}, {self.table1.c.value} \nFROM " \
                        f"{self.table1.name} ORDER BY {self.table1.c.id}"
        stmt = select(self.table1).order_by(self.table1.c.id)
        assert expected_stmt == str(stmt)

        stmt = self.table1.select().order_by(self.table1.c.id)
        assert expected_stmt == str(stmt)

    def test_select_multiple_order_by(self):

        expected_stmt = f"SELECT {self.table1.c.id}, {self.table1.c.name}, {self.table1.c.value} \nFROM " \
                        f"{self.table1.name} ORDER BY {self.table1.c.id}, {self.table1.c.name}"
        stmt = select(self.table1).order_by(self.table1.c.id, self.table1.c.name)
        assert expected_stmt == str(stmt)

        stmt = self.table1.select().order_by(self.table1.c.id, self.table1.c.name)
        assert expected_stmt == str(stmt)

    def test_select_join(self):

        expected_stmt = f"SELECT {self.table1.c.id}, {self.table1.c.name}, {self.table1.c.value} \nFROM {self.table1.name} " \
                        f"JOIN {self.table2.name} ON {self.table1.c.id} = {self.table2.c.id}"
        join_stmt = self.table1.join(self.table2, self.table1.c.id == self.table2.c.id)
        stmt = select([self.table1]).select_from(join_stmt)
        assert expected_stmt == str(stmt)

    # Select Join Where not supported
    def test_select_join_where_not_supported(self):

        join_stmt = self.table1.join(self.table2, self.table1.c.id == self.table2.c.id)
        with pytest.raises(Exception) as e_info:
            print(select([self.table1]).select_from(join_stmt).where(self.table1.c.id == '1'))
        assert "Where clause of parameterized query not supported on SQream" in str(e_info.value)

    def test_select_join_order_by(self):

        expected_stmt = f"SELECT {self.table1.c.id}, {self.table1.c.name}, {self.table1.c.value} \nFROM {self.table1.name} " \
                        f"JOIN {self.table2.name} ON {self.table1.c.id} = {self.table2.c.id} ORDER BY {self.table1.c.id}"
        join_stmt = self.table1.join(self.table2, self.table1.c.id == self.table2.c.id)
        stmt = select([self.table1]).select_from(join_stmt).order_by(self.table1.c.id)
        assert expected_stmt == str(stmt)

    def test_select_multiple_join(self):

        table3 = Table(
            'table3', self.metadata,
            Column("id", sa.Integer), Column("name", sa.UnicodeText), Column("value", sa.Integer)
        )

        table4 = Table(
            'table4', self.metadata,
            Column("id", sa.Integer), Column("name", sa.UnicodeText), Column("value", sa.Integer)
        )

        expected_stmt = f"SELECT {self.table1.c.id}, {self.table1.c.name}, {self.table1.c.value}, " \
                        f"{self.table2.c.id} AS id_1, {self.table2.c.name} AS name_1, {self.table2.c.value} AS value_1 " \
                        f"\nFROM {self.table1.name} " \
                        f"JOIN {self.table2.name} ON {self.table1.c.id} = {self.table2.c.id} " \
                        f"JOIN {table3.name} ON {self.table1.c.id} = {table3.c.id} " \
                        f"JOIN {table4.name} ON {self.table1.c.id} = {table4.c.id}"

        stmt = select(self.table1, self.table2).join_from(
            self.table1, self.table2, self.table1.c.id == self.table2.c.id
        ).join_from(self.table1, table3, self.table1.c.id == table3.c.id)\
            .join_from(self.table1, table4, self.table1.c.id == table4.c.id)
        assert expected_stmt == str(stmt)

    def test_aliased(self):

        table_aliased = aliased(self.table1, name="table_aliased")
        expected_stmt = f"SELECT {table_aliased.c.id}, {table_aliased.c.name}, {table_aliased.c.value} \n" \
                        f"FROM {self.table1.name} AS {table_aliased.name}"
        stmt = select([table_aliased])
        assert expected_stmt == str(stmt)

    def test_select_subquery(self):

        expected_stmt = f"SELECT {self.table2.c.id}, {self.table2.c.name}, {self.table2.c.value} \n" \
                        f"FROM {self.table2.name} JOIN (SELECT {self.table1.c.id} AS id, " \
                        f"{self.table1.c.name} AS name, {self.table1.c.value} AS value \n" \
                        f"FROM {self.table1.name}) AS anon_1 ON {self.table2.c.id} = anon_1.id"
        subq = select(self.table1).subquery()
        stmt = select(self.table2).join(subq, self.table2.c.id == subq.c.id)
        assert expected_stmt == str(stmt)

    def test_union_all(self):

        expected_stmt = f"SELECT {self.table1.c.id}, {self.table1.c.name}, {self.table1.c.value} " \
                        f"\nFROM {self.table1.name} UNION ALL SELECT {self.table2.c.id}, {self.table2.c.name}, " \
                        f"{self.table2.c.value} \nFROM {self.table2.name}"
        stmt = union_all(select(self.table1), select(self.table2))
        assert expected_stmt == str(stmt)

    def test_exists(self):

        self.Base.metadata.drop_all(bind=self.engine)
        self.Base.metadata.create_all(self.engine)

        stmt = exists().where(self.user.id == self.address.id)
        with Session(self.engine) as session:
            spongebob = self.user(id=1,
                                  name="spongebob",
                                  fullname="Spongebob Squarepants")
            spongebob_email = self.address(id=1, email_address="spongebob@gmail.com", user_id=1)
            session.add_all([spongebob, spongebob_email])
            session.flush()

            for (name,) in session.query(self.user.name).filter(stmt):
                assert name == "spongebob", f"Exist is not return the correct value, " \
                                          f"expected to get spongebob, bot got {name}"

    # Exists like not supported
    def test_exists_with_like_not_supported(self):

        self.Base.metadata.drop_all(bind=self.engine)
        self.Base.metadata.create_all(self.engine)

        with Session(self.engine) as session:
            spongebob = self.user(id=1,
                                  name="spongebob",
                                  fullname="Spongebob Squarepants")
            spongebob_email = self.address(id=1, email_address="spongebob@gmail.com", user_id=1)
            session.add_all([spongebob, spongebob_email])
            session.flush()

            with pytest.raises(Exception) as e_info:
                for (name,) in session.query(self.user.name).filter(self.address.email_address.like("%gmail%")):
                    print(name)

        assert "Where clause of parameterized query not supported on SQream" in str(e_info.value)