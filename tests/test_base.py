import socket
from datetime import datetime, date
from decimal import Decimal
from random import choice, randint, choices
from typing import Union

import pytest
import sqlalchemy as sa
from sqlalchemy import (text,
                        Table,
                        Column,
                        orm,
                        Integer,
                        Boolean,
                        Date,
                        DateTime,
                        Numeric,
                        Text,
                        create_engine,
                        MetaData,
                        Identity,
                        Connection)
from sqlalchemy.orm import declarative_base, Session

from pytest_logger import Logger


def connect(ip, port, clustered=False, use_ssl=False):
    print_echo = False
    conn_str = f"pysqream+dialect://sqream:sqream@{ip}:{port}/master"
    engine = create_engine(conn_str, echo=print_echo, connect_args={"clustered": clustered, "use_ssl": use_ssl})
    sa.Tinyint = engine.dialect.Tinyint
    session = orm.sessionmaker(bind=engine)()
    metadata = MetaData()
    metadata.bind = engine
    return engine, metadata, session, conn_str


def setTinyint(engine):
    sa.Tinyint = engine.dialect.Tinyint


class TestBase:
    @pytest.fixture()
    def ip(self, pytestconfig):
        return pytestconfig.getoption("ip")

    @pytest.fixture()
    def port(self, pytestconfig):
        return pytestconfig.getoption("port")

    @pytest.fixture(autouse=True)
    def Test_setup_teardown(self, ip, port):
        self.start(ip, port)
        yield
        self.stop()

    def start(self, ip, port):
        ip = ip if ip else socket.gethostbyname(socket.gethostname())
        Logger().info("Before Scenario")
        Logger().info(f"Connect to server {ip}:{port}")
        self.ip = ip
        self.port = port
        self.engine, self.metadata, self.session, self.conn_str = connect(ip, port)
        self.insp = sa.inspect(self.engine)
        setTinyint(self.engine)

    def stop(self):
        Logger().info("After Scenario")
        self.engine.dispose()


class TestBaseOrm(TestBase):
    @pytest.fixture()
    def ip(self, pytestconfig):
        return pytestconfig.getoption("ip")

    @pytest.fixture()
    def Base(self, pytestconfig):
        return self.Base

    @pytest.fixture()
    def user(self):
        return self.user

    @pytest.fixture()
    def address(self):
        return self.address

    @pytest.fixture()
    def dates(self):
        return self.dates

    @pytest.fixture()
    def table1(self):
        return self.table1

    @pytest.fixture()
    def table2(self):
        return self.table2

    @pytest.fixture(autouse=True)
    def Test_setup_teardown(self, ip, port):
        self.Base = declarative_base()

        class Dates(self.Base):
            __tablename__ = "dates"

            id = Column(Integer, Identity(start=0), primary_key=True)
            dates = Column(sa.DateTime)

            def __repr__(self):
                return f"Dates(id={self.id!r}, dates={self.dates!r})"

        class User(self.Base):
            __tablename__ = "user_account"

            id = Column(Integer, Identity(start=0), primary_key=True)
            name = Column(sa.UnicodeText)
            fullname = Column(sa.UnicodeText)

            def __repr__(self):
                return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"

        class Address(self.Base):
            __tablename__ = "address"

            id = Column(sa.Integer, Identity(start=0), primary_key=True)
            email_address = Column(sa.UnicodeText, nullable=False)
            user_id = Column(sa.Integer, nullable=False)

            def __repr__(self):
                return f"Address(id={self.id!r}, email_address={self.email_address!r})"

        self.user = User
        self.address = Address
        self.dates = Dates

        self.start(ip, port)

        self.table1 = Table(
            'table1', self.metadata,
            Column("id", sa.Integer), Column("name", sa.UnicodeText), Column("value", sa.Integer)
        )

        self.table2 = Table(
            'table2', self.metadata,
            Column("id", sa.Integer), Column("name", sa.UnicodeText), Column("value", sa.Integer)
        )

        yield
        self.stop()


class TestBaseTI(TestBase):
    @pytest.fixture()
    def ip(self, pytestconfig):
        return pytestconfig.getoption("ip")

    @pytest.fixture()
    def testware_affinity_matrix(self):
        return self.testware_affinity_matrix

    @pytest.fixture(autouse=True)
    def Test_setup_teardown(self, ip, port):
        self.start(ip, port)

        self.testware_affinity_matrix = sa.Table(
            'testware_affinity_matrix',
            self.metadata,
            sa.Column('technology', sa.TEXT(32)),
            sa.Column('criteria', sa.TEXT(32)),
            sa.Column('category', sa.TEXT(32)),
            sa.Column('component', sa.TEXT(32)),
            sa.Column('svn', sa.TEXT(32)),
            sa.Column('parm_name', sa.TEXT(32)),
            sa.Column('lpt', sa.TEXT(32)),
            sa.Column('tech', sa.TEXT(32)),
            sa.Column('severity', sa.Float)
        )

        if self.insp.has_table(self.testware_affinity_matrix.name):
            self.testware_affinity_matrix.drop(bind=self.engine)

        self.testware_affinity_matrix.create(bind=self.engine)

        yield
        self.stop()


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
    def crud_table_row(self):
        self.Base = declarative_base()

        class Crud(self.Base):
            __tablename__ = "crud_table"

            i = Column(Integer, Identity(start=0), primary_key=True)
            b = Column(Boolean)
            d = Column(Date)
            dt = Column(DateTime)
            n = Column(Numeric(15, 6))
            t = Column(Text)

            def __repr__(self):
                return f"Crud(id={self.i})"

        return Crud

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

    def recreate_all_via_metadata(self, executor: Union[Connection, Session] = None):
        if not executor:
            executor = self.session
        if self.view_name in self.insp.get_view_names():
            executor.execute(text(f"drop view {self.view_name}"))
        self.metadata.drop_all(bind=self.engine)
        self.metadata.create_all(bind=self.engine)
