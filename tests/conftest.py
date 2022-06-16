import logging
import pytest
from .util.connectors import *


logging.basicConfig()
LOG = logging.getLogger()


@pytest.fixture
def api():
    return APIConnector()


@pytest.fixture
def redis():
    return RedisConnector()


@pytest.fixture
def db():
    return DBConnector()


@pytest.fixture
def voter_id(api):
    return api.voter_id
