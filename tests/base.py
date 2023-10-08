import pytest
import socket
import sys
import os
from pytest_logger import Logger
from sqlalchemy import orm, create_engine, MetaData
import sqlalchemy as sa


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


class TestBase():

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


class TestBaseWithoutBeforeAfter():
    @pytest.fixture()
    def ip(self, pytestconfig):
        return pytestconfig.getoption("ip")

    @pytest.fixture(autouse=True)
    def Test_setup_teardown(self, ip):
        self.ip = ip if ip else socket.gethostbyname(socket.gethostname())
        yield