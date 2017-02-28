from psycopg2.extras import RealDictCursor
from tilequeue.postgresql import DBAffinityConnectionsNoLimit
from tilequeue.tile import calc_meters_per_pixel_dim
from tilequeue.transform import calculate_padded_bounds
import sys


def generate_query(start_zoom, template, bounds, zoom):
    if zoom < start_zoom:
        return None
    query = template.render(bounds=bounds, zoom=zoom)
    return query


class JinjaQueryGenerator(object):

    def __init__(self, template, start_zoom):
        self.template = template
        self.start_zoom = start_zoom

    def __call__(self, bounds, zoom):
        return generate_query(self.start_zoom, self.template, bounds, zoom)


class DevJinjaQueryGenerator(object):

    def __init__(self, environment, template_name, start_zoom):
        self.environment = environment
        self.template_name = template_name
        self.start_zoom = start_zoom

    def __call__(self, bounds, zoom):
        template = self.environment.get_template(self.template_name)
        return generate_query(self.start_zoom, template, bounds, zoom)


def jinja_filter_geometry(value):
    return 'ST_AsBinary(%s)' % value


def jinja_filter_bbox_filter(bounds, geometry_col_name, srid=3857):
    min_point = 'ST_MakePoint(%.12f, %.12f)' % (bounds[0], bounds[1])
    max_point = 'ST_MakePoint(%.12f, %.12f)' % (bounds[2], bounds[3])
    bbox_no_srid = 'ST_MakeBox2D(%s, %s)' % (min_point, max_point)
    bbox = 'ST_SetSrid(%s, %d)' % (bbox_no_srid, srid)
    bbox_filter = '%s && %s' % (geometry_col_name, bbox)
    return bbox_filter


def jinja_filter_bbox_intersection(bounds, geometry_col_name, srid=3857):
    min_point = 'ST_MakePoint(%.12f, %.12f)' % (bounds[0], bounds[1])
    max_point = 'ST_MakePoint(%.12f, %.12f)' % (bounds[2], bounds[3])
    bbox_no_srid = 'ST_MakeBox2D(%s, %s)' % (min_point, max_point)
    bbox = 'ST_SetSrid(%s, %d)' % (bbox_no_srid, srid)
    bbox_intersection = 'st_intersection(%s, %s)' % (geometry_col_name, bbox)
    return bbox_intersection


def jinja_filter_bbox_padded_intersection(
        bounds, geometry_col_name, pad_factor=1.1, srid=3857):
    padded_bounds = calculate_padded_bounds(pad_factor, bounds)
    return jinja_filter_bbox_intersection(
        padded_bounds.bounds, geometry_col_name, srid)


def jinja_filter_bbox(bounds, srid=3857):
    min_point = 'ST_MakePoint(%.12f, %.12f)' % (bounds[0], bounds[1])
    max_point = 'ST_MakePoint(%.12f, %.12f)' % (bounds[2], bounds[3])
    bbox_no_srid = 'ST_MakeBox2D(%s, %s)' % (min_point, max_point)
    bbox = 'ST_SetSrid(%s, %d)' % (bbox_no_srid, srid)
    return bbox


def jinja_filter_bbox_overlaps(bounds, geometry_col_name, srid=3857):
    min_point = 'ST_MakePoint(%.12f, %.12f)' % (bounds[0], bounds[1])
    max_point = 'ST_MakePoint(%.12f, %.12f)' % (bounds[2], bounds[3])
    bbox_no_srid = 'ST_MakeBox2D(%s, %s)' % (min_point, max_point)
    bbox = 'ST_SetSrid(%s, %d)' % (bbox_no_srid, srid)
    bbox_filter = \
        '((%(col)s && %(bbox)s) AND st_overlaps(%(col)s, %(bbox)s))' \
        % dict(col=geometry_col_name, bbox=bbox)
    return bbox_filter


def build_feature_queries(unpadded_bounds, layer_data, zoom):
    meters_per_pixel_dim = calc_meters_per_pixel_dim(zoom)
    queries_to_execute = []
    for layer_datum in layer_data:
        query_bounds_pad_fn = layer_datum['query_bounds_pad_fn']
        padded_bounds = query_bounds_pad_fn(
            unpadded_bounds, meters_per_pixel_dim)
        query_generator = layer_datum['query_generator']
        query = query_generator(padded_bounds, zoom)
        queries_to_execute.append((layer_datum, query, padded_bounds))
    return queries_to_execute


def execute_query(conn, query, layer_datum, padded_bounds):
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        rows = list(cursor.fetchall())

        return rows, layer_datum, padded_bounds
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


def trim_layer_datum(layer_datum):
    layer_datum_result = dict(
        [(k, v) for k, v in layer_datum.items()
         if k not in ('query_generator', 'query_bounds_pad_fn')])
    return layer_datum_result


def enqueue_queries(sql_conns, thread_pool, layer_data, zoom, unpadded_bounds):

    queries_to_execute = build_feature_queries(
        unpadded_bounds, layer_data, zoom)

    empty_results = []
    async_results = []
    for (layer_datum, query, padded_bounds), sql_conn in zip(
            queries_to_execute, sql_conns):
        layer_datum = trim_layer_datum(layer_datum)
        if query is None:
            empty_feature_layer = dict(
                name=layer_datum['name'],
                features=[],
                layer_datum=layer_datum,
                padded_bounds=padded_bounds,
            )
            empty_results.append(empty_feature_layer)
        else:
            async_result = thread_pool.apply_async(
                execute_query, (sql_conn, query, layer_datum, padded_bounds))
            async_results.append(async_result)

    return empty_results, async_results


class DataFetcher(object):

    def __init__(self, conn_info, layer_data, io_pool, n_conn):
        self.conn_info = dict(conn_info)
        self.layer_data = layer_data
        self.io_pool = io_pool

        self.dbnames = self.conn_info.pop('dbnames')
        self.dbnames_query_index = 0
        self.sql_conn_pool = DBAffinityConnectionsNoLimit(
            self.dbnames, self.conn_info)
        self.n_conn = n_conn

    def __call__(self, zoom, unpadded_bounds, layer_data=None):
        if layer_data is None:
            layer_data = self.layer_data

        sql_conns = self.sql_conn_pool.get_conns(self.n_conn)
        try:
            # the padded bounds are used here in order to only have to
            # issue a single set of queries to the database for all
            # formats
            empty_results, async_results = enqueue_queries(
                sql_conns, self.io_pool, layer_data, zoom, unpadded_bounds)

            feature_layers = []
            async_exception = None
            for async_result in async_results:
                try:
                    rows, layer_datum, padded_bounds = async_result.get()
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
                # only keep values that are not None
                read_rows = []
                for row in rows:
                    read_row = {}
                    for k, v in row.items():
                        if isinstance(v, buffer):
                            v = bytes(v)
                        if v is not None:
                            read_row[k] = v
                    read_rows.append(read_row)

                feature_layer = dict(
                    name=layer_datum['name'], features=read_rows,
                    layer_datum=layer_datum,
                    padded_bounds=padded_bounds,
                )
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
