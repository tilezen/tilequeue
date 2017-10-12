from tilequeue.query.fixture import make_fixture_data_fetcher
from tilequeue.query.pool import DBConnectionPool
from tilequeue.query.postgres import make_db_data_fetcher


__all__ = [
    'DBConnectionPool',
    'make_db_data_fetcher',
    'make_fixture_data_fetcher',
]


def make_data_fetcher(cfg, query_cfg, io_pool):
    return make_db_data_fetcher(
        cfg.postgresql_conn_info, cfg.template_path, cfg.reload_templates,
        query_cfg, io_pool)
