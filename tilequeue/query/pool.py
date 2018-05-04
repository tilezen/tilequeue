from itertools import cycle
from itertools import islice
from psycopg2.extras import register_hstore, register_json
import psycopg2
import threading
import ujson
import random


class ConnectionsContextManager(object):

    """Handle automatically closing connections via with statement"""

    def __init__(self, conns):
        self.conns = conns

    def __enter__(self):
        return self.conns

    def __exit__(self, exc_type, exc_val, exc_tb):
        for conn in self.conns:
            try:
                conn.close()
            except Exception:
                pass
        suppress_exception = False
        return suppress_exception


class DBConnectionPool(object):

    """Manage database connections with varying database names"""

    def __init__(self, dbnames, conn_info, readonly=True):
        self.dbnames = cycle(dbnames)
        self.conn_info = conn_info
        self.conn_mapping = {}
        self.lock = threading.Lock()
        self.readonly = readonly

    def _make_conn(self, conn_info):
        # if multiple hosts are provided, select one at random as a kind of
        # simple load balancing.
        host = conn_info.get('host')
        if host and isinstance(host, list):
            host = random.choice(host)
            conn_info = conn_info.copy()
            conn_info['host'] = host

        conn = psycopg2.connect(**conn_info)
        conn.set_session(readonly=self.readonly, autocommit=True)
        register_hstore(conn)
        register_json(conn, loads=ujson.loads)
        return conn

    def get_conns(self, n_conn):
        with self.lock:
            dbnames = list(islice(self.dbnames, n_conn))
        conns = []
        for dbname in dbnames:
            conn_info_with_db = dict(self.conn_info, dbname=dbname)
            conn = self._make_conn(conn_info_with_db)
            conns.append(conn)
        conns_ctx_mgr = ConnectionsContextManager(conns)
        return conns_ctx_mgr
