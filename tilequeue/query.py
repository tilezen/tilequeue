from collections import namedtuple
from psycopg2.extras import RealDictCursor
from tilequeue.postgresql import DBConnectionPool
from tilequeue.transform import calculate_padded_bounds
import sys


DataSource = namedtuple('DataSource', 'name template, start_zoom')


def make_source(name, template, start_zoom):
    return DataSource(name, template, start_zoom)


class TemplateFinder(object):

    """Look up the jinja template

    The cache_templates option is expected to be set in production to
    avoid having to regenerate the template repeatedly.
    """

    def __init__(self, jinja_environment, cache_templates=False):
        self.environment = jinja_environment
        self.cache_templates = cache_templates
        if cache_templates:
            self.template_cache = {}

    def __call__(self, source_name):
        template = None
        if self.cache_templates:
            template = self.template_cache.get(source_name)
        if not template:
            template = self.environment.get_template(source_name)
            if self.cache_templates:
                self.template_cache[source_name] = template
        return template


class TemplateQueryGenerator(object):

    def __init__(self, template_finder):
        self.template_finder = template_finder

    def __call__(self, source, bounds, zoom):
        template = self.template_finder(source)

        # TODO bounds padding
        padded_bounds = dict(
            polygon=bounds,
            line=bounds,
            point=bounds,
        )

        query = template.render(bounds=padded_bounds, zoom=zoom)
        return query


class SourcesQueriesGenerator(object):

    def __init__(self, sources, query_generator):
        self.sources = sources
        self.query_generator = query_generator

    def __call__(self, zoom, bounds):
        queries = []
        for source in self.sources:
            if source.start_zoom <= zoom:
                query = self.query_generator(source.template, bounds, zoom)
                queries.append(query)
        return queries


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


def execute_query(conn, query):
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        rows = list(cursor.fetchall())

        return rows
    except:
        # TODO this kind of thing is only necessary if we re-use connections

        # If any exception occurs during query execution, close the
        # connection to ensure it is not in an invalid state. The
        # connection pool knows to create new connections to replace
        # those that are closed
        try:
            conn.close()
        except:
            pass
        raise


class DataFetchException(Exception):

    """Capture all exceptions when trying to read data"""

    def __init__(self, exceptions):
        self.exceptions = exceptions
        msgs = ', '.join([x.message for x in exceptions])
        super(DataFetchException, self).__init__(msgs)


class DataFetcher(object):

    def __init__(self, conn_info, queries_generator, io_pool):
        self.conn_info = dict(conn_info)
        self.queries_generator = queries_generator
        self.io_pool = io_pool

        self.dbnames = self.conn_info.pop('dbnames')
        self.dbnames_query_index = 0
        self.sql_conn_pool = DBConnectionPool(
            self.dbnames, self.conn_info)

    def __call__(self, zoom, unpadded_bounds):
        queries = self.queries_generator(zoom, unpadded_bounds)

        n_conns = len(queries)
        assert n_conns, 'no queries'

        with self.sql_conn_pool.get_conns(n_conns) as sql_conns:
            async_results = []
            for query, conn in zip(queries, sql_conns):
                async_result = self.io_pool.apply_async(
                    execute_query, (conn, query))
                async_results.append(async_result)

            all_source_rows = []
            async_exceptions = []
            for async_result in async_results:
                try:
                    source_rows = async_result.get()
                    # TODO can all the source rows just be smashed together?
                    # seems like it because the data allows discrimination
                    all_source_rows.extend(source_rows)
                except:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    async_exception = exc_value
                    async_exceptions.append(async_exception)
                    continue

            if async_exceptions:
                raise DataFetchException(async_exceptions)

        read_rows = []
        for row in all_source_rows:
            read_row = {}
            for k, v in row.items():
                if isinstance(v, buffer):
                    v = bytes(v)
                if v is not None:
                    read_row[k] = v
            read_rows.append(read_row)

        return read_rows
