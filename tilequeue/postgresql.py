from itertools import cycle
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import register_hstore, register_json
import threading
import ujson as json


class DatabaseCycleConnectionPool(object):

    """
    Maintains a psycopg2 ThreadedConnectionPool for each of the
    given dbnames. When a client requests a set of connections,
    all of those connections will come from the same database.
    """

    def __init__(self, min_conns_per_db, max_conns_per_db, dbnames, conn_info):
        self._pools = []
        self._conns_to_pool = {}

        for dbname in dbnames:
            pool = ThreadedConnectionPool(
                min_conns_per_db,
                max_conns_per_db,
                dbname=dbname,
                **conn_info
            )
            self._pools.append(pool)

        self._pool_cycle = cycle(self._pools)
        self._lock = threading.Lock()

    def get_conns(self, n_conns):
        conns = []

        with self._lock:
            try:
                pool_to_use = next(self._pool_cycle)
                for _ in range(n_conns):
                    conn = pool_to_use.getconn()

                    register_json(conn, loads=json.loads, globally=True)
                    register_hstore(conn, globally=True)

                    self._conns_to_pool[id(conn)] = pool_to_use
                    conns.append(conn)
                assert len(conns) == n_conns, \
                    "Couldn't collect enough connections"
            except:
                self.put_conns(conns)
                conns = []
                raise

        return conns

    def put_conns(self, conns):
        with self._lock:
            for conn in conns:
                pool = self._conns_to_pool.pop(id(conn), None)
                if pool:
                    pool.putconn(conn)

    def closeall(self):
        with self._lock:
            for pool in self._pools:
                pool.closeall()
            self._conns_to_pool.clear()
