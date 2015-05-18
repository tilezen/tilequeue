from psycopg2.extras import RealDictCursor
from threading import Lock
from tilequeue.postgresql import DBAffinityConnectionsNoLimit
from tilequeue.tile import coord_to_mercator_bounds
from tilequeue.tile import pad_bounds_for_zoom
from TileStache.Goodies.VecTiles.server import query_columns
import sys


# stores the sql columns needed per layer, zoom
# accesses need to be protected by a lock
column_name_cache = {}
column_name_cache_lock = Lock()


def columns_for_query(conn_info, layer_name, zoom, bounds, query):
    srid = 900913
    key = (layer_name, zoom)
    with column_name_cache_lock:
        columns = column_name_cache.get(key)
    if columns:
        return columns
    columns = query_columns(conn_info, srid, query, bounds)
    with column_name_cache_lock:
        column_name_cache[key] = columns
    return columns


def find_columns_for_queries(conn_info, layer_data, zoom, bounds):
    columns_for_queries = []
    for layer_datum in layer_data:
        queries = layer_datum['queries']
        query = queries[min(zoom, len(queries) - 1)]
        if query is None:
            cols = None
        else:
            cols = columns_for_query(
                conn_info, layer_datum['name'], zoom, bounds, query)
        columns_for_queries.append(cols)
    return columns_for_queries


def build_query(srid, subquery, subcolumns, bounds):
    ''' Build and return an PostGIS query.
    '''
    bbox = ('ST_MakeBox2D(ST_MakePoint(%.12f, %.12f), '
            '             ST_MakePoint(%.12f, %.12f))' % bounds)
    bbox = 'ST_SetSRID(%s, %d)' % (bbox, srid)
    geom = 'q.__geometry__'

    subquery = subquery.replace('!bbox!', bbox)
    columns = ['q."%s"' % c for c in subcolumns if c != '__geometry__']

    if '__geometry__' not in subcolumns:
        raise Exception("There's supposed to be a __geometry__ column.")

    columns = ', '.join(columns)

    return '''SELECT %(columns)s,
                     ST_AsBinary(%(geom)s) AS __geometry__
              FROM (
                %(subquery)s
                ) AS q
              WHERE q.__geometry__ && %(bbox)s''' % locals()


def build_feature_queries(bounds, layer_data, zoom, columns_for_queries):
    srid = 900913
    queries_to_execute = []
    for layer_datum, columns in zip(layer_data, columns_for_queries):
        queries = layer_datum['queries']
        subquery = queries[min(zoom, len(queries) - 1)]
        if subquery is None:
            query = None
        else:
            query = build_query(srid, subquery, columns, bounds)

        layer_datum_no_queries = dict((k, v) for k, v in layer_datum.items()
                                      if k != 'queries')
        queries_to_execute.append((layer_datum_no_queries, query))

    return queries_to_execute


def execute_query(conn, query, layer_datum):
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        rows = list(cursor.fetchall())
        return rows, layer_datum
    except:
        # If any exception occurs during query execution, close the
        # connection to ensure it is not in an invalid state. The
        # connection pool knows to create new connections to replace
        # those that are closed
        try:
            conn.close()
        except:
            pass
        raise


def enqueue_queries(sql_conns, thread_pool, layer_data, zoom, bounds, columns):

    queries_to_execute = build_feature_queries(
        bounds, layer_data, zoom, columns)

    empty_results = []
    async_results = []
    for (layer_datum, query), sql_conn in zip(queries_to_execute, sql_conns):
        if query is None:
            empty_feature_layer = dict(
                name=layer_datum['name'],
                features=[],
                layer_datum=layer_datum,
            )
            empty_results.append(empty_feature_layer)
        else:
            async_result = thread_pool.apply_async(
                execute_query, (sql_conn, query, layer_datum))
            async_results.append(async_result)

    return empty_results, async_results


class DataFetcher(object):

    def __init__(self, conn_info, layer_data, io_pool, n_conn):
        self.conn_info = dict(conn_info)
        self.layer_data = layer_data
        self.find_columns_for_queries = find_columns_for_queries
        self.io_pool = io_pool

        self.dbnames = self.conn_info.pop('dbnames')
        self.dbnames_query_index = 0
        self.sql_conn_pool = DBAffinityConnectionsNoLimit(
            self.dbnames, n_conn, self.conn_info)

    def __call__(self, coord):
        zoom = coord.zoom
        unpadded_bounds = coord_to_mercator_bounds(coord)
        # the vtm renderer needs features a little surrounding the
        # bounding box as well, these padded bounds are used in the
        # queries
        padded_bounds = pad_bounds_for_zoom(unpadded_bounds, zoom)

        sql_conns, conn_info = self.sql_conn_pool.get_conns()
        try:
            # first determine the columns for the queries
            # we currently perform the actual query and ask for no data
            # we also cache this per layer, per zoom

            columns_for_queries = self.find_columns_for_queries(
                conn_info, self.layer_data, zoom, padded_bounds)

            # the padded bounds are used here in order to only have to
            # issue a single set of queries to the database for all
            # formats
            empty_results, async_results = enqueue_queries(
                sql_conns, self.io_pool, self.layer_data, zoom,
                padded_bounds, columns_for_queries)

            feature_layers = []
            async_exception = None
            for async_result in async_results:
                try:
                    rows, layer_datum = async_result.get()
                except:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    async_exception = exc_value
                    # iterate through all async results to give others
                    # a chance to close any connections that yielded
                    # exceptions
                    continue

                # don't continue processing if an error occurred on
                # any results
                if async_exception is not None:
                    continue

                # read the bytes out of each row, otherwise the pickle
                # will fail because the geometry is a read buffer
                for row in rows:
                    geometry_bytes = bytes(row.pop('__geometry__'))
                    row['__geometry__'] = geometry_bytes

                feature_layer = dict(name=layer_datum['name'], features=rows,
                                     layer_datum=layer_datum)
                feature_layers.append(feature_layer)

            # bail if an error occurred
            if async_exception is not None:
                raise async_exception

            feature_layers.extend(empty_results)

            return dict(
                feature_layers=feature_layers,
                unpadded_bounds=unpadded_bounds,
                padded_bounds=padded_bounds,
            )

        finally:
            self.sql_conn_pool.put_conns(sql_conns)
