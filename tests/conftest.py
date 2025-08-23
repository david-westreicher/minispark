import pytest
from mini_spark import query


@pytest.fixture(scope="session", autouse=True)
def setup_global():
    query.USE_WORKERS = False
