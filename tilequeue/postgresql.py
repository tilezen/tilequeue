from itertools import cycle
from psycopg2.extras import register_hstore, register_json
import psycopg2
import threading
import ujson


class DBAffinityConnectionsNoLimit(object):

    # Similar to the db affinity pool, but without keeping track of
    # the connections. It's the caller's responsibility to call us
    # back with the connection objects so that we can close them.

    def __init__(self, dbnames, conn_info, readonly=True):
        self.dbnames = cycle(dbnames)
        self.conn_info = conn_info
        self.conn_mapping = {}
        self.lock = threading.Lock()
        self.readonly = readonly

    def _make_conn(self, conn_info):
        conn = psycopg2.connect(**conn_info)
        conn.set_session(readonly=self.readonly, autocommit=True)
        register_hstore(conn)
        register_json(conn, loads=ujson.loads)
        return conn

    def get_conns(self, n_conn):
        with self.lock:
            dbname = self.dbnames.next()
        conn_info_with_db = dict(self.conn_info, dbname=dbname)
        conns = [self._make_conn(conn_info_with_db)
                 for i in range(n_conn)]
        return conns

    def put_conns(self, conns):
        for conn in conns:
            try:
                conn.close()
            except:
                pass

    def closeall(self):
        raise Exception('DBAffinityConnectionsNoLimit pool does not track '
                        'connections')
