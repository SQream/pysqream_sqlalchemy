import pytest
import socket
import sys
import os
from pytest_logger import Logger
from sqlalchemy import orm, create_engine, MetaData
import sqlalchemy as sa
from sqlalchemy import Table, Column, String, Integer, ForeignKey, Sequence, Identity
from sqlalchemy.orm import declarative_base, relationship, IdentityMap


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
            'testware_affinity_matrix'
            , self.metadata
            , sa.Column('technology', sa.TEXT(32))
            , sa.Column('criteria', sa.TEXT(32))
            , sa.Column('category', sa.TEXT(32))
            , sa.Column('component', sa.TEXT(32))
            , sa.Column('svn', sa.TEXT(32))
            , sa.Column('parm_name', sa.TEXT(32))
            , sa.Column('lpt', sa.TEXT(32))
            , sa.Column('tech', sa.TEXT(32))
            , sa.Column('severity', sa.Float)
        )

        if self.insp.has_table(self.testware_affinity_matrix.name):
            self.testware_affinity_matrix.drop()

        self.metadata.create_all(bind=self.engine)

        yield
        self.stop()
