from tilequeue.query.fixture import make_data_fetcher
from tilequeue.query.pool import DBConnectionPool
from tilequeue.query.postgres import make_db_data_fetcher

# expose the fixture data fetcher under a different name
make_fixture_data_fetcher = make_data_fetcher

__all__ = [
    'DBConnectionPool',
    'make_db_data_fetcher',
    'make_fixture_data_fetcher',
]
