def pytest_addoption(parser):
    parser.addoption("--ip", action="store", help="SQream Server ip", default="192.168.0.35")
    parser.addoption("--port", action="store", help="SQream Server ip", default="5000")


def pytest_generate_tests(metafunc):
    metafunc.config.getoption("ip")
    metafunc.config.getoption("port")
