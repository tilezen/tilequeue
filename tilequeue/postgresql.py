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

        with self.lock:
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

    def putconn(self, conn, key=None, close=False):
        assert key is None, 'Keys not supported'

        with self.lock:
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

    def closeall(self):
        with self.lock:
            for conn_id, conn in chain(self.not_in_use.iteritems(),
                                       self.in_use.iteritems()):
                try:
                    conn.close()
                except:
                    pass
            self.not_in_use.clear()
            self.in_use.clear()


class HostAffinityConnectionPool(object):

    # conn_info is expected to have all connection information except
    # for the host. For each host, n_conn_per_host connections will be
    # made to it. If a connection to a particular host fails, a new
    # one will be created to ensure that all connections are balanced.
    def __init__(self, hosts, n_conn_per_host, conn_info):
        self.hosts = hosts
        self.conn_info = conn_info
        self.n_conn_per_host = n_conn_per_host
        self.lock = threading.Lock()

        self.conns_by_host = {}
        for host in hosts:
            conn_info_with_host = dict(conn_info, host=host)
            for i in range(n_conn_per_host):
                conn = self._make_conn(conn_info_with_host)
                self.conns_by_host.setdefault(host, []).append(conn)

        self.host_conns_in_use = set()
        self.host_conns_not_in_use = set(hosts)

    def _make_conn(self, conn_info):
        return psycopg2.connect(**conn_info)

    def get_conns_for_host(self, host):
        with self.lock:
            assert host in self.conns_by_host, 'Unknown host: %s' % host
            assert host in self.host_conns_not_in_use, \
                'Connections already in use for host: %s' % host
            conns = self.conns_by_host[host]

            if len(conns) < self.n_conn_per_host:
                # we are short some connections
                # create the connections that we expect to have available
                conn_info_with_host = dict(self.conn_info, host=host)
                for i in range(self.n_conn_per_host - len(conns)):
                    conn = self._make_conn(conn_info_with_host)
                    conns.append(conn)

            self.host_conns_in_use.add(host)
            self.host_conns_not_in_use.remove(host)
            return conns

    def put_conns_for_host(self, host):
        with self.lock:
            assert host in self.conns_by_host, 'Unknown host: %s' % host
            assert host in self.host_conns_in_use, \
                'Connections not in use for host: %s' % host

            # check if any connections have been closed
            # those will need to be recreated before being returned to
            # the rotation
            conns_to_return = []
            conns = self.conns_by_host.pop(host)
            try:
                conn_info_with_host = dict(self.conn_info, host=host)
                for conn in conns:
                    if conn.closed:
                        try:
                            conn = self._make_conn(conn_info_with_host)
                        except:
                            print 'Error creating new connection to host: %s' % \
                                host
                            # When connections are fecthed for this
                            # host again, new ones will attempt to be
                            # created at that point
                            continue
                    conns_to_return.append(conn)
            finally:
                # always add whatever connections we have available back
                # and restore the host connection accounting
                self.conns_by_host[host] = conns_to_return
                self.host_conns_in_use.remove(host)
                self.host_conns_not_in_use.add(host)

    def closeall(self):
        with self.lock:
            for host in self.hosts:
                conns = self.conns_by_host[host]
                for conn in conns:
                    try:
                        conn.close()
                    except:
                        pass
            self.conns_by_host.clear()
            self.host_conns_in_use.clear()
            self.host_conns_not_in_use.clear()
            self.hosts = []


class DBAffinityConnections(object):

    # Designed to be used with pgbouncer
    # It expects conn_info to have all connection information except
    # for the dbname. The dbnames will get rotated in for each
    # queryset.
    # No connections are pooled here, but created on demand. This
    # expects that pgbouncer will be performing all the connection
    # pooling for us.

    def __init__(self, dbnames, n_conn_per_db, conn_info):
        self.dbnames = dbnames
        self.n_conn_per_db = n_conn_per_db
        self.conn_info = conn_info
        self.db_to_conns = {}
        self.lock = threading.Lock()

    def _make_conn(self, conn_info):
        conn = psycopg2.connect(**conn_info)
        conn.set_session(readonly=True, autocommit=True)
        return conn

    def get_conns_for_db(self, dbname):
        conn_info_with_db = dict(self.conn_info, dbname=dbname)
        conns = [self._make_conn(conn_info_with_db)
                 for i in range(self.n_conn_per_db)]
        with self.lock:
            assert dbname not in self.db_to_conns, \
                'already in db_to_conns: %s' % dbname
            self.db_to_conns[dbname] = conns
        return conns

    def put_conns_for_db(self, dbname):
        with self.lock:
            conns = self.db_to_conns.pop(dbname)
        for conn in conns:
            try:
                conn.close()
            except:
                pass

    def closeall(self):
        with self.lock:
            for dbname, conns in self.db_to_conns.items():
                for conn in conns:
                    try:
                        conn.close()
                    except:
                        pass
            self.db_to_conns.clear()


class DBAffinityConnectionsNoLimit(object):

    # Similar to the db affinity pool, but without keeping track of
    # the connections. It's the caller's responsibility to call us
    # back with the connection objects so that we can close them.

    def __init__(self, dbnames, n_conn_per_db, conn_info):
        self.dbnames = dbnames
        self.n_conn_per_db = n_conn_per_db
        self.conn_info = conn_info
        self.conn_mapping = {}
        self.lock = threading.Lock()
        self.dbname_index = 0

    def _make_conn(self, conn_info):
        conn = psycopg2.connect(**conn_info)
        conn.set_session(readonly=True, autocommit=True)
        return conn

    def get_conns(self):
        with self.lock:
            dbname = self.dbnames[self.dbname_index]
            self.dbname_index += 1
            if self.dbname_index >= len(self.dbnames):
                self.dbname_index = 0
        conn_info_with_db = dict(self.conn_info, dbname=dbname)
        conns = [self._make_conn(conn_info_with_db)
                 for i in range(self.n_conn_per_db)]
        # convenient for caller to get the conn_info for fetching the
        # columns
        return conns, conn_info_with_db

    def put_conns(self, conns):
        for conn in conns:
            try:
                conn.close()
            except:
                pass

    def closeall(self):
        raise Exception('DBAffinityConnectionsNoLimit pool does not track '
                        'connections')
