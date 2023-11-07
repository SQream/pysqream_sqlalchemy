

import os, sys
sys.path.append(os.path.abspath(__file__).rsplit('tests/', 1)[0] + '/pysqream_sqlalchemy/')
sys.path.append(os.path.abspath(__file__).rsplit('tests/', 1)[0] + '/tests/')
from test_base import TestBaseOrm
from sqlalchemy import select, dialects, Table, Column, union_all, func, distinct, case, cast, Numeric, \
    extract, nulls_first, desc, nulls_last, asc, true, false
from sqlalchemy.sql import exists
from sqlalchemy.orm import aliased, Session
import sqlalchemy as sa
import pytest
from datetime import datetime


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

    def test_select_group_by(self):

        expected_stmt = f"SELECT {self.table1.c.value} \nFROM {self.table1.name} GROUP BY {self.table1.c.value}"
        stmt = select(self.table1.c.value).group_by(self.table1.c.value)
        assert expected_stmt == str(stmt)

    def test_grouping_sets(self):

        stmt = select(
            func.sum(self.user.id), self.user.name, self.user.fullname
        ).group_by(func.grouping_sets(self.user.name, self.user.fullname))
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "grouping_sets function not supported on SQream" in str(e_info.value)

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

    # Case is not supported
    def test_case_not_supported(self):

        self.Base.metadata.drop_all(bind=self.engine)
        self.Base.metadata.create_all(self.engine)

        with Session(self.engine) as session:
            spongebob = self.user(id=1,
                                  name="spongebob",
                                  fullname="Spongebob Squarepants")
            spongebob_email = self.address(id=1, email_address="spongebob@gmail.com", user_id=1)
            session.add_all([spongebob, spongebob_email])
            session.flush()

        stmt = select(self.user). \
            where(
            case(
                (self.user.name == 'spongebob', 'S'),
                (self.user.name == 'jack', 'J'),
                else_='E'
            )
        )

        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "Where clause of parameterized query not supported on SQream" in str(e_info.value)

    def test_cast(self):

        expected_stmt = f"SELECT CAST({self.user.__tablename__}.id AS NUMERIC(10, 4)) AS id \nFROM {self.user.__tablename__}"
        stmt = select(cast(self.user.id, Numeric(10, 4)))
        assert expected_stmt == str(stmt)

    def test_func_sum(self):

        expected_stmt = f"SELECT sum({self.table1.c.value}) AS sum_1 \nFROM {self.table1}"
        stmt = select(func.sum(self.table1.c.value))
        assert expected_stmt == str(stmt)

    def test_func_count(self):
        expected_stmt = f"SELECT count({self.table1.c.value}) AS count_1 \nFROM {self.table1}"
        stmt = select(func.count(self.table1.c.value))
        assert expected_stmt == str(stmt)

    def test_func_max(self):
        expected_stmt = f"SELECT max({self.table1.c.value}) AS max_1 \nFROM {self.table1}"
        stmt = select(func.max(self.table1.c.value))
        assert expected_stmt == str(stmt)

    def test_func_min(self):
        expected_stmt = f"SELECT min({self.table1.c.value}) AS min_1 \nFROM {self.table1}"
        stmt = select(func.min(self.table1.c.value))
        assert expected_stmt == str(stmt)

    def test_func_current_timestamp(self):

        expected_stmt = "SELECT CURRENT_TIMESTAMP AS current_timestamp_1"
        stmt = select(func.current_timestamp())
        assert expected_stmt == str(stmt)

    def test_func_now(self):

        stmt = select(func.now())
        with Session(self.engine) as session:
            res = session.execute(stmt)

        assert res.fetchall() != None, "Excepted to results"

    def test_func_localtimestamp(self):

        stmt = select(func.localtimestamp())
        with Session(self.engine) as session:
            res = session.execute(stmt)

        assert res.fetchall() != None, "Excepted to results"

    def test_func_current_date(self):

        expected_stmt = "SELECT CURRENT_DATE AS current_date_1"
        stmt = select(func.current_date())
        assert expected_stmt == str(stmt)

    def test_func_current_time_not_supported(self):

        stmt = select(func.current_time())
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "current_time function not supported on SQream" in str(e_info.value)

    def test_func_localtime_not_supported(self):

        stmt = select(func.localtime())
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "localtime function not supported on SQream" in str(e_info.value)

    def test_sysdate(self):
        expected_stmt = "SELECT sysdate AS sysdate_1"
        stmt = select(func.sysdate())
        assert expected_stmt == str(stmt)

    def test_aggregate_strings_not_supported(self):

        stmt = select(func.aggregate_strings(self.user.name, "."))
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "aggregate_strings function not supported on SQream" in str(e_info.value)

    def test_array_agg(self):

        expected_stmt = f"SELECT array_agg({self.user.__tablename__}.id) AS array_agg_1 \nFROM {self.user.__tablename__}"
        stmt = select(func.array_agg(self.user.id))
        assert expected_stmt == str(stmt)

    def test_char_length_not_supported(self):

        stmt = select(func.char_length('daniel'))
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "char_length function with parameterized value is not supported on SQream" in str(e_info.value)

    def test_coalesce_not_supported(self):

        stmt = select(func.coalesce(None, None, None, 1))
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "coalesce function with parameterized value is not supported on SQream" in str(e_info.value)

    def test_cube_not_supported(self):

        stmt = select(func.sum(self.user.id), self.user.name, self.user.fullname
                ).group_by(func.cube(self.user.name, self.user.fullname))
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)
        assert "cube function not supported on SQream" in str(e_info.value)

    def test_current_user_not_supported(self):

        stmt = select(func.current_user())
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "current_user function not supported on SQream" in str(e_info.value)

    def test_concat_not_supported(self):

        stmt = select(func.concat('a', 'b'))
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "concat function with parameterized value is not supported on SQream" in str(e_info.value)

    def test_session_user_not_supported(self):

        stmt = select(func.session_user())
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "session_user function not supported on SQream" in str(e_info.value)

    def test_user_not_supported(self):

        stmt = select(func.user())
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "user function not supported on SQream" in str(e_info.value)

    def test_distinct(self):

        expected_stmt = f"SELECT DISTINCT {self.table1.c.id} \nFROM {self.table1}"
        stmt = select(distinct(self.table1.c.id))
        assert expected_stmt == str(stmt)

    def test_cte(self):

        self.Base.metadata.drop_all(bind=self.engine)
        self.Base.metadata.create_all(self.engine)

        with Session(self.engine) as session:
            patrick = self.user(id=3,
                                name="patrick",
                                fullname="Patrick Star")
            session.add_all([patrick])
            session.flush()

        test_cte = select(
            self.user.name,
            func.sum(self.user.id).label('total_ids')
        ).group_by(self.user.name).cte("test_cte")

        stmt = select(test_cte)

        with Session(self.engine) as session:
            result = session.execute(stmt).all()

        assert ('patrick', 3) == result[0], f"expected to get {('patrick', 3)}, got {result[0]}"

    # sql.expression
    def test_true(self):

        expected_stmt = f"SELECT {self.table1.c.id}, {self.table1.c.name}, {self.table1.c.value} " \
                        f"\nFROM {self.table1} \nWHERE true"
        stmt = select(self.table1).where(true())
        assert expected_stmt == str(stmt)

    # sql.expression
    def test_false(self):

        expected_stmt = f"SELECT {self.table1.c.id}, {self.table1.c.name}, {self.table1.c.value} " \
                        f"\nFROM {self.table1} \nWHERE false"
        stmt = select(self.table1).where(false())
        assert expected_stmt == str(stmt)

    def test_asc(self):

        expected_stmt = f"SELECT {self.table1.c.id}, {self.table1.c.name}, {self.table1.c.value} \nFROM {self.table1} " \
                        f"ORDER BY {self.table1.c.name} ASC"
        stmt = select(self.table1).order_by(asc(self.table1.c.name))
        assert expected_stmt == str(stmt)

    def test_desc(self):

        expected_stmt = f"SELECT {self.table1.c.id}, {self.table1.c.name}, {self.table1.c.value} \nFROM {self.table1} " \
                        f"ORDER BY {self.table1.c.name} DESC"
        stmt = select(self.table1).order_by(desc(self.table1.c.name))
        assert expected_stmt == str(stmt)

    def test_nulls_first_not_supported(self):

        stmt = select(self.user).order_by(nulls_first(desc(self.user.name)))
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "NULLS FIRST not supported on SQream" in str(e_info.value)

    def test_nulls_last_not_supported(self):

        stmt = select(self.user).order_by(nulls_last(desc(self.user.name)))
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "NULLS LAST not supported on SQream" in str(e_info.value)

    def test_extract(self):

        self.Base.metadata.drop_all(bind=self.engine)
        self.Base.metadata.create_all(self.engine)

        with Session(self.engine) as session:
            date_one = self.dates(id=1, dates=datetime.now())
            date_two = self.dates(id=2, dates=datetime.now())
            session.add_all([date_one, date_two])
            session.flush()

        stmt = select(extract("YEAR", self.dates.dates))
        with Session(self.engine) as session:
            result = session.execute(stmt).all()

        year = datetime.today().year
        for row in result:
            assert int(row[0]) == year, f"Wrong result expected to {year} got {int(row[0])}"

    def test_where_extract_1(self):

        stmt = select(extract("YEAR", self.dates.dates)).where(self.dates.id == 1)
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)
        assert "Where clause of parameterized query not supported on SQream" in str(e_info.value)

    def test_where_extract_2(self):

        stmt = select(self.dates.id).where(extract("YEAR", self.dates.dates) == datetime.today().year)
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)
        assert "Where clause of parameterized query not supported on SQream" in str(e_info.value)

    #TODO - add test while supporting feature
    # def test_in(self):
    #     pass

    #TODO - add test while supporting feature
    # def test_not_in(self):
    #     pass

    #TODO - add test while supporting feature
    # def test_endswith(self):
    #     pass

    #TODO - add test while supporting feature
    # def test_not_endswith(self):
    #     pass

    #TODO - add test while supporting feature
    # def test_startswith(self):
    #     pass

    #TODO - add test while supporting feature
    # def test_not_startswith(self):
    #     pass

    #TODO - add test while supporting feature
    # def test_like(self):
    #     pass

    #TODO - add test while supporting feature
    # def test_not_like(self):
    #     pass

    #TODO - add test while supporting feature
    # def test_ilike(self):
    #     pass

    #TODO - add test while supporting feature
    # def test_not_ilike(self):
    #     pass

    #TODO - add test while supporting feature
    # def test_between(self):
    #     pass

    #TODO - add test while supporting feature
    # def test_not_between(self):
    #     pass

    #TODO - add test while supporting feature
    # def test_contains(self):
    #     pass

    #TODO - add test while supporting feature
    # def test_not_contains(self):
    #     pass

    # window_function tests
    def test_lag(self):

        expected_stmt = f"SELECT lag(user_account.id) OVER (PARTITION BY user_account.name ORDER BY user_account.id) AS anon_1 " \
                        f"\nFROM {self.user.__tablename__}"
        stmt = select(func.lag(self.user.id).over(partition_by=self.user.name, order_by=self.user.id))
        assert expected_stmt == str(stmt)

    def test_lead(self):

        expected_stmt = f"SELECT lead(user_account.id) OVER (PARTITION BY user_account.name ORDER BY user_account.id) AS anon_1 " \
                        f"\nFROM {self.user.__tablename__}"
        stmt = select(func.lead(self.user.id).over(partition_by=self.user.name, order_by=self.user.id))
        assert expected_stmt == str(stmt)

    def test_max(self):

        expected_stmt = f"SELECT max(user_account.id) OVER (PARTITION BY user_account.name ORDER BY user_account.id) AS anon_1 " \
                        f"\nFROM {self.user.__tablename__}"
        stmt = select(func.max(self.user.id).over(partition_by=self.user.name, order_by=self.user.id))
        assert expected_stmt == str(stmt)

    def test_min(self):

        expected_stmt = f"SELECT min(user_account.id) OVER (PARTITION BY user_account.name ORDER BY user_account.id) AS anon_1 " \
                        f"\nFROM {self.user.__tablename__}"
        stmt = select(func.min(self.user.id).over(partition_by=self.user.name, order_by=self.user.id))
        assert expected_stmt == str(stmt)

    def test_sum(self):

        expected_stmt = f"SELECT sum(user_account.id) OVER (PARTITION BY user_account.name ORDER BY user_account.id) AS anon_1 " \
                        f"\nFROM {self.user.__tablename__}"
        stmt = select(func.sum(self.user.id).over(partition_by=self.user.name, order_by=self.user.id))
        assert expected_stmt == str(stmt)

    def test_first_value(self):

        expected_stmt = f"SELECT first_value(user_account.id) OVER (ORDER BY user_account.id) AS anon_1 " \
                        f"\nFROM {self.user.__tablename__}"
        stmt = select(func.first_value(self.user.id).over(order_by=self.user.id))
        assert expected_stmt == str(stmt)

    def test_last_value(self):

        expected_stmt = f"SELECT last_value(user_account.id) OVER (ORDER BY user_account.id) AS anon_1 " \
                        f"\nFROM {self.user.__tablename__}"
        stmt = select(func.last_value(self.user.id).over(order_by=self.user.id))
        assert expected_stmt == str(stmt)

    def test_row_number(self):
        expected_stmt = f"SELECT row_number() OVER (PARTITION BY user_account.name " \
                        f"ORDER BY user_account.id) AS anon_1 " \
                        f"\nFROM {self.user.__tablename__}"
        stmt = select(func.row_number().over(partition_by=self.user.name, order_by=self.user.id))
        assert expected_stmt == str(stmt)

    def test_nth_value_not_supported(self):

        stmt = select(func.nth_value(self.user.id, 1).over(order_by=self.user.id))
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "nth_value function with parameterized value is not supported on SQream" in str(e_info.value)

    def test_ntile_not_supported(self):
        stmt = select(func.ntile(1).over(order_by=self.user.id))
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "ntile function with parameterized value is not supported on SQream" in str(e_info.value)

    def test_cume_dist(self):

        expected_stmt = f"SELECT {self.table1.c.id}, {self.table1.c.name}, {self.table1.c.value}, cume_dist() OVER " \
                        f"(PARTITION BY {self.table1.c.name} ORDER BY {self.table1.c.value}) AS anon_1 " \
                        f"\nFROM {self.table1.name}"
        stmt = select(self.table1.c.id, self.table1.c.name, self.table1.c.value, func.cume_dist().over(
            partition_by=self.table1.c.name, order_by=self.table1.c.value))
        assert expected_stmt == str(stmt)

    def test_dense_rank(self):

        expected_stmt = f"SELECT {self.table1.c.id}, {self.table1.c.name}, {self.table1.c.value}, dense_rank() OVER " \
                        f"(PARTITION BY {self.table1.c.name} ORDER BY {self.table1.c.value}) AS anon_1 " \
                        f"\nFROM {self.table1.name}"
        stmt = select(self.table1.c.id, self.table1.c.name, self.table1.c.value, func.dense_rank().over(
            partition_by=self.table1.c.name, order_by=self.table1.c.value))
        assert expected_stmt == str(stmt)

    def test_mode(self):

        expected_stmt = f"SELECT mode() WITHIN GROUP (ORDER BY {self.table1.c.value}) AS anon_1 " \
                        f"\nFROM {self.table1.name}"
        stmt = select(func.mode().within_group(self.table1.c.value))
        assert expected_stmt == str(stmt)

    def test_percent_rank(self):

        expected_stmt = f"SELECT {self.table1.c.id}, {self.table1.c.name}, {self.table1.c.value}, percent_rank() OVER " \
                        f"(PARTITION BY {self.table1.c.name} ORDER BY {self.table1.c.value}) AS anon_1 " \
                        f"\nFROM {self.table1.name}"
        stmt = select(self.table1.c.id, self.table1.c.name, self.table1.c.value, func.percent_rank().over(
            partition_by=self.table1.c.name, order_by=self.table1.c.value))
        assert expected_stmt == str(stmt)

    def test_percentile_cont_not_supported(self):

        stmt = select(func.percentile_cont(0.18).within_group(self.table1.c.value))
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "percentile_cont function with parameterized value is not supported on SQream" in str(e_info.value)

    def test_percentile_disc_not_supported(self):

        stmt = select(func.percentile_disc(0.18).within_group(self.table1.c.value))
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "percentile_disc function with parameterized value is not supported on SQream" in str(e_info.value)

    def test_random_not_supported(self):

        stmt = select(func.random())
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "random function not supported on SQream" in str(e_info.value)

    def test_rank(self):

        expected_stmt = f"SELECT {self.table1.c.id}, {self.table1.c.name}, {self.table1.c.value}, rank() OVER " \
                        f"(PARTITION BY {self.table1.c.name} ORDER BY {self.table1.c.value}) AS anon_1 " \
                        f"\nFROM {self.table1.name}"
        stmt = select(self.table1.c.id, self.table1.c.name, self.table1.c.value, func.rank().over(
            partition_by=self.table1.c.name, order_by=self.table1.c.value))
        assert expected_stmt == str(stmt)

    def test_rollup_not_supported(self):

        stmt = select(func.rollup())
        with pytest.raises(Exception) as e_info:
            with Session(self.engine) as session:
                session.execute(stmt)

        assert "rollup function not supported on SQream" in str(e_info.value)


