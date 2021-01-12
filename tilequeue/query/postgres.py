from collections import namedtuple
from jinja2 import Environment
from jinja2 import FileSystemLoader
from psycopg2.extras import RealDictCursor
from tilequeue.query import DBConnectionPool
from tilequeue.transform import calculate_padded_bounds
import sys


TemplateSpec = namedtuple('TemplateSpec', 'template start_zoom end_zoom')
DataSource = namedtuple('DataSource', 'name template_specs')


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
            template_queries = []
            for template_spec in source.template_specs:
                # NOTE: end_zoom is exclusive
                if template_spec.start_zoom <= zoom < template_spec.end_zoom:
                    template_query = self.query_generator(
                        template_spec.template, bounds, zoom)
                    template_queries.append(template_query)
            if template_queries:
                source_query = '\nUNION ALL\n'.join(template_queries)
                queries.append(source_query)
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
    """
    Check whether the boundary of the geometry intersects with the bounding
    box.

    Note that the usual meaning of "overlaps" in GIS terminology is that the
    boundaries of the box and polygon intersect, but not the interiors. This
    means that if the box or polygon is completely within the other, then
    st_overlaps will be false.

    However, that's not what we want. This is used for boundary testing, and
    while we don't want to pull out a whole country boundary if the bounding
    box is fully within it, we _do_ want to if the country boundary is within
    the bounding box.

    Therefore, this test has an extra "or st_contains" test to also pull in any
    boundaries which are completely within the bounding box.
    """

    min_point = 'ST_MakePoint(%.12f, %.12f)' % (bounds[0], bounds[1])
    max_point = 'ST_MakePoint(%.12f, %.12f)' % (bounds[2], bounds[3])
    bbox_no_srid = 'ST_MakeBox2D(%s, %s)' % (min_point, max_point)
    bbox = 'ST_SetSrid(%s, %d)' % (bbox_no_srid, srid)
    bbox_filter = \
        '((%(col)s && %(bbox)s) AND (' \
        '  st_overlaps(%(col)s, %(bbox)s) OR' \
        '  st_contains(%(bbox)s, %(col)s)' \
        '))' \
        % dict(col=geometry_col_name, bbox=bbox)
    return bbox_filter


def execute_query(conn, query):
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        rows = list(cursor.fetchall())

        return rows
    except Exception:
        # TODO this kind of thing is only necessary if we re-use connections

        # If any exception occurs during query execution, close the
        # connection to ensure it is not in an invalid state. The
        # connection pool knows to create new connections to replace
        # those that are closed
        try:
            conn.close()
        except Exception:
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

    def fetch_tiles(self, all_data):
        # postgres data fetcher doesn't need this kind of session management,
        # so we can just return the same object for all uses.
        for data in all_data:
            yield self, data

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
                except Exception:
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
                if v is not None:
                    read_row[k] = v
            read_rows.append(read_row)

        return read_rows


def make_jinja_environment(template_path):
    environment = Environment(loader=FileSystemLoader(template_path))
    environment.filters['geometry'] = jinja_filter_geometry
    environment.filters['bbox_filter'] = jinja_filter_bbox_filter
    environment.filters['bbox_intersection'] = jinja_filter_bbox_intersection
    environment.filters['bbox_padded_intersection'] = (
        jinja_filter_bbox_padded_intersection)
    environment.filters['bbox'] = jinja_filter_bbox
    environment.filters['bbox_overlaps'] = jinja_filter_bbox_overlaps
    return environment


def make_queries_generator(sources, template_path, reload_templates):
    jinja_environment = make_jinja_environment(template_path)
    cache_templates = not reload_templates
    template_finder = TemplateFinder(jinja_environment, cache_templates)
    query_generator = TemplateQueryGenerator(template_finder)
    queries_generator = SourcesQueriesGenerator(sources, query_generator)
    return queries_generator


def parse_source_data(queries_cfg):
    sources_cfg = queries_cfg['sources']
    sources = []
    for source_name, templates in sources_cfg.items():
        template_specs = []
        for template_data in templates:
            template = template_data['template']
            start_zoom = int(template_data.get('start_zoom', 0))
            # NOTE: end_zoom is exclusive
            end_zoom = int(template_data.get('end_zoom', 21))
            template_spec = TemplateSpec(template, start_zoom, end_zoom)
            template_specs.append(template_spec)
        source = DataSource(source_name, template_specs)
        sources.append(source)
    return sources


def make_db_data_fetcher(postgresql_conn_info, template_path, reload_templates,
                         query_cfg, io_pool):
    """
    Returns an object which is callable with the zoom and unpadded bounds and
    which returns a list of rows.
    """

    sources = parse_source_data(query_cfg)
    queries_generator = make_queries_generator(
        sources, template_path, reload_templates)
    return DataFetcher(
        postgresql_conn_info, queries_generator, io_pool)
