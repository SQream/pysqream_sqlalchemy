import pytest
import socket
import sys
import os
from pytest_logger import Logger
from sqlalchemy import orm, create_engine, MetaData
import sqlalchemy as sa
from sqlalchemy import Table, Column
from sqlalchemy.orm import declarative_base


def connect(ip, clustered=False, use_ssl=False, port=5000):
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

    @pytest.fixture(autouse=True)
    def Test_setup_teardown(self, ip):
        ip = ip if ip else socket.gethostbyname(socket.gethostname())
        Logger().info("Before Scenario")
        Logger().info(f"Connect to server {ip}")
        self.ip = ip
        self.engine ,self.metadata ,self.session, self.conn_str = connect(ip)
        setTinyint(self.engine)
        yield
        Logger().info("After Scenario")
        self.engine.dispose()


@pytest.mark.usefixtures('Test_setup_teardown')
class TestBaseSelect(TestBase):

    @pytest.fixture()
    def table1(self):
        return self.table1

    # @pytest.fixture()
    # def Table1(self):
    #     return self.Table1

    @pytest.fixture()
    def table2(self):
        return self.table2

    @pytest.fixture()
    def ip(self, pytestconfig):
        return pytestconfig.getoption("ip")

    @pytest.fixture(autouse=True)
    def Test_setup_teardown(self, ip):
        ip = ip if ip else socket.gethostbyname(socket.gethostname())
        Logger().info("Before Scenario")
        Logger().info(f"Connect to server {ip}")
        self.ip = ip
        self.engine, self.metadata, self.session, self.conn_str = connect(ip)
        setTinyint(self.engine)

        self.table1 = Table(
            'table1', self.metadata,
            Column("id", sa.Integer), Column("name", sa.UnicodeText), Column("value", sa.Integer)
        )

        self.table2 = Table(
            'table2', self.metadata,
            Column("id", sa.Integer), Column("name", sa.UnicodeText), Column("value", sa.Integer)
        )

        yield
        Logger().info("After Scenario")
        self.engine.dispose()

class TestBaseTI:

    @pytest.fixture()
    def ip(self, pytestconfig):
        return pytestconfig.getoption("ip")

    @pytest.fixture()
    def testware_affinity_matrix(self):
        return self.testware_affinity_matrix

    @pytest.fixture(autouse=True)
    def Test_setup_teardown(self, ip):
        ip = ip if ip else socket.gethostbyname(socket.gethostname())
        Logger().info("Before Scenario")
        Logger().info(f"Connect to server {ip}")
        self.ip = ip
        self.engine, self.metadata, self.session, self.conn_str = connect(ip)
        setTinyint(self.engine)

        metadata = MetaData(schema="rfab_ie")
        metadata.bind = self.engine

        if not self.engine.dialect.has_schema(self.engine, metadata.schema):
            self.engine.execute(sa.schema.CreateSchema(metadata.schema))

        self.testware_affinity_matrix = sa.Table(
            'testware_affinity_matrix'
            , metadata
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

        if self.engine.has_table(self.testware_affinity_matrix.name):
            self.testware_affinity_matrix.drop()

        self.testware_affinity_matrix.create()

        yield
        Logger().info("After Scenario")
        self.engine.dispose()


class TestBaseWithoutBeforeAfter:
    @pytest.fixture()
    def ip(self, pytestconfig):
        return pytestconfig.getoption("ip")

    @pytest.fixture(autouse=True)
    def Test_setup_teardown(self, ip):
        self.ip = ip if ip else socket.gethostbyname(socket.gethostname())
        yield