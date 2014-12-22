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
