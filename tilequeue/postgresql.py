from itertools import chain
from itertools import cycle
import psycopg2
import threading


class RoundRobinConnectionFactory(object):

    def __init__(self, conn_info, hosts):
        self.conn_info = conn_info
        self.hosts_gen = iter(cycle(hosts))

    def __call__(self, ignored_dsn):
        host = self.hosts_gen.next()
        conn_info = dict(self.conn_info, host=host)
        conn = psycopg2.connect(**conn_info)
        return conn


class NoPoolingConnectionPool(object):

    # This adheres to the connection pool interface, but generates a
    # new connection with every request

    def __init__(self, minconn, maxconn, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def getconn(self, key=None):
        return psycopg2.connect(*self.args, **self.kwargs)

    def putconn(self, conn, key=None, close=False):
        # this pool implementation always closes connections
        try:
            conn.close()
        except:
            pass

    def closeall(self):
        pass


class ThreadedConnectionPool(object):

    # Custom version of a threaded connection pool. This is a simpler
    # implementation than what the postgresql connection pool does. In
    # particular, this pool attempts to be much safer when returning
    # connections back to the pool.

    def __init__(self, minconn, maxconn, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.maxconn = maxconn
        self.lock = threading.Lock()
        self.not_in_use = {}
        self.in_use = {}

        for i in xrange(minconn):
            conn = self._make_conn()
            self.not_in_use[id(conn)] = conn

    def _make_conn(self):
        return psycopg2.connect(*self.args, **self.kwargs)

    def getconn(self, key=None):
        assert key is None, 'Keys not supported'

        self.lock.acquire()
        try:
            if self.not_in_use:
                conn_id, conn = self.not_in_use.popitem()
                self.in_use[conn_id] = conn
                return conn
            else:
                if len(self.in_use) == self.maxconn:
                    raise RuntimeError(
                        'Maximum number of connections created: %d' %
                        self.maxconn)
                conn = self._make_conn()
                self.in_use[id(conn)] = conn
                return conn
        finally:
            self.lock.release()

    def putconn(self, conn, key=None, close=False):
        assert key is None, 'Keys not supported'

        self.lock.acquire()
        try:
            conn_id = id(conn)
            if conn_id not in self.in_use:
                raise ValueError('Connection not part of pool')
            del self.in_use[conn_id]
            if close:
                try:
                    conn.close()
                except:
                    pass
            if not conn.closed:
                self.not_in_use[conn_id] = conn
        finally:
            self.lock.release()

    def closeall(self):
        self.lock.acquire()
        try:
            for conn_id, conn in chain(self.not_in_use.iteritems(),
                                       self.in_use.iteritems()):
                try:
                    conn.close()
                except:
                    pass
            self.not_in_use.clear()
            self.in_use.clear()
        finally:
            self.lock.release()
