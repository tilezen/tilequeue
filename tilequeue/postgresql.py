from itertools import cycle
import psycopg2


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
