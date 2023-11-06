import os, sys
sys.path.append(os.path.abspath(__file__).rsplit('tests/', 1)[0] + '/pysqream_sqlalchemy/')
sys.path.append(os.path.abspath(__file__).rsplit('tests/', 1)[0] + '/tests/')
from test_base import TestBaseOrm
from sqlalchemy import select, dialects, Table, Column, union_all, Identity, ForeignKey, Sequence, schema, update, delete
from sqlalchemy.orm import aliased, Session
import sqlalchemy as sa
import pytest


dialects.registry.register("pysqream.dialect", "dialect", "SqreamDialect")


class TestOrmDao(TestBaseOrm):

    def test_drop_all(self):
        self.Base.metadata.drop_all(bind=self.engine)

    def test_create_all(self):
        self.Base.metadata.drop_all(bind=self.engine)
        self.Base.metadata.create_all(self.engine)

    def test_create_table_with_identity(self):
        table3 = Table(
            'table3', self.metadata,
            Column("id", sa.Integer, Identity(start=0)), Column("name", sa.UnicodeText), Column("value", sa.Integer)
        )
        if self.engine.has_table(table3.name):
            table3.drop()
        table3.create()

    def test_create_table_with_identity_minvalue_not_supported(self):
        table3 = Table(
            'table3', self.metadata,
            Column("id", sa.Integer, Identity(start=1, minvalue=1)), Column("name", sa.UnicodeText), Column("value", sa.Integer)
        )
        if self.engine.has_table(table3.name):
            table3.drop()

        with pytest.raises(Exception) as e_info:
            table3.create()

        assert "min value of identity key constraints are not supported by SQream" in str(e_info.value)

    def test_create_table_with_identity_maxvalue_not_supported(self):
        table3 = Table(
            'table3', self.metadata,
            Column("id", sa.Integer, Identity(start=1, maxvalue=10)), Column("name", sa.UnicodeText), Column("value", sa.Integer)
        )
        if self.engine.has_table(table3.name):
            table3.drop()

        with pytest.raises(Exception) as e_info:
            table3.create()

        assert "max value of identity key constraints are not supported by SQream" in str(e_info.value)

    def test_create_table_with_identity_nomaxvalue_not_supported(self):
        table3 = Table(
            'table3', self.metadata,
            Column("id", sa.Integer, Identity(start=1, nomaxvalue=10)), Column("name", sa.UnicodeText), Column("value", sa.Integer)
        )
        if self.engine.has_table(table3.name):
            table3.drop()

        with pytest.raises(Exception) as e_info:
            table3.create()

        assert "no maxvalue of identity key constraints are not supported by SQream" in str(e_info.value)

    def test_create_table_with_identity_nominvalue_not_supported(self):
        table3 = Table(
            'table3', self.metadata,
            Column("id", sa.Integer, Identity(start=1, nominvalue=10)), Column("name", sa.UnicodeText), Column("value", sa.Integer)
        )
        if self.engine.has_table(table3.name):
            table3.drop()

        with pytest.raises(Exception) as e_info:
            table3.create()

        assert "no minvalue of identity key constraints are not supported by SQream" in str(e_info.value)

    def test_create_table_with_identity_cache_not_supported(self):
        table3 = Table(
            'table3', self.metadata,
            Column("id", sa.Integer, Identity(start=1, cache=5)), Column("name", sa.UnicodeText), Column("value", sa.Integer)
        )
        if self.engine.has_table(table3.name):
            table3.drop()

        with pytest.raises(Exception) as e_info:
            table3.create()

        assert "cache of identity key constraints are not supported by SQream" in str(e_info.value)

    def test_create_table_with_identity_order_not_supported(self):
        table3 = Table(
            'table3', self.metadata,
            Column("id", sa.Integer, Identity(start=1, order=True)), Column("name", sa.UnicodeText), Column("value", sa.Integer)
        )
        if self.engine.has_table(table3.name):
            table3.drop()

        with pytest.raises(Exception) as e_info:
            table3.create()

        assert "order of identity key constraints are not supported by SQream" in str(e_info.value)

    def test_create_table_with_identity_cycle_not_supported(self):
        table3 = Table(
            'table3', self.metadata,
            Column("id", sa.Integer, Identity(start=1, cycle=6)), Column("name", sa.UnicodeText), Column("value", sa.Integer)
        )
        if self.engine.has_table(table3.name):
            table3.drop()

        with pytest.raises(Exception) as e_info:
            table3.create()

        assert "cycle of identity key constraints are not supported by SQream" in str(e_info.value)

    def test_foreign_key_not_supported(self):
        table3 = Table(
            'table3', self.metadata,
            Column("id", sa.Integer, ForeignKey("table1.id")), Column("name", sa.UnicodeText), Column("value", sa.Integer)
        )
        if self.engine.has_table(table3.name):
            table3.drop()

        with pytest.raises(Exception) as e_info:
            table3.create()

        assert "foreign key constraints are not supported by SQream" in str(e_info.value)

    def test_primary_key_not_supported(self):
        table3 = Table(
            'table3', self.metadata,
            Column("id", sa.Integer, primary_key=True), Column("name", sa.UnicodeText), Column("value", sa.Integer)
        )
        if self.engine.has_table(table3.name):
            table3.drop()

        with pytest.raises(Exception) as e_info:
            table3.create()

        assert "primary key constraints are not supported by SQream" in str(e_info.value)

    def test_add_all(self):
        self.Base.metadata.drop_all(bind=self.engine)
        self.Base.metadata.create_all(self.engine)

        with Session(self.engine) as session:
            spongebob = self.user(id=1,
                                  name="spongebob",
                                  fullname="Spongebob Squarepants")
            sandy = self.user(id=2,
                              name="sandy",
                              fullname="Sandy Cheeks")
            patrick = self.user(id=3,
                                name="patrick",
                                fullname="Patrick Star")
            session.add_all([spongebob, sandy, patrick])
            session.flush()
            session.commit() # not doing nothing

        res = self.engine.execute(select(self.user)).fetchall()
        assert len(res) == 3, "Row count after insert is not correct"

    # Delete Where not supported
    def test_delete_where_not_supported_1(self):
        self.Base.metadata.drop_all(bind=self.engine)
        self.Base.metadata.create_all(self.engine)

        with Session(self.engine) as session:
            patrick = self.user(id=3,
                                name="patrick",
                                fullname="Patrick Star")
            session.add_all([patrick])
            session.flush()
            # session.commit() # not doing nothing

        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.delete(patrick)
                session.flush()
                # session.commit() # not doing nothing

        assert "Where clause of parameterized query not supported on SQream" in str(e_info.value)

    def test_delete_where_not_supported_2(self):

        stmt = delete(self.user).where(self.user.id == "1")
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "Where clause of parameterized query not supported on SQream" in str(e_info.value)

    def test_delete(self):
        expected_stmt = f"DELETE FROM {self.user.__tablename__}"
        stmt = delete(self.user)
        assert expected_stmt == str(stmt)

    # Update Where not supported
    def test_update_not_supported(self):

        stmt = update(self.user).values(name="daniel").where(self.user.id == "1")
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "Where clause of parameterized query not supported on SQream" in str(e_info.value)

    def test_table_description(self):
        pass