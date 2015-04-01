from cStringIO import StringIO
from multiprocessing.pool import ThreadPool
from psycopg2.extras import RealDictCursor
from shapely import geometry
from shapely.wkb import dumps
from shapely.wkb import loads
from tilequeue.format import json_format
from tilequeue.format import mapbox_format
from tilequeue.format import topojson_format
from tilequeue.format import vtm_format
from tilequeue.postgresql import DBAffinityConnections
from TileStache.Geography import SphericalMercator
from TileStache.Goodies.VecTiles.ops import transform
from TileStache.Goodies.VecTiles.server import query_columns
from TileStache.Goodies.VecTiles.server import tolerances
import math
import sys


class RenderData(object):

    def __init__(self, format, feature_layers, bounds, padded_bounds):
        self.format = format
        self.feature_layers = feature_layers
        self.bounds = bounds
        self.padded_bounds = padded_bounds


# stores the sql columns needed per layer, zoom
column_name_cache = {}


def columns_for_query(conn_info, layer_name, zoom, bounds, query):
    srid = 900913
    key = (layer_name, zoom)
    columns = column_name_cache.get(key)
    if columns:
        return columns
    columns = query_columns(conn_info, srid, query, bounds)
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
              WHERE ST_IsValid(q.__geometry__) AND
                    q.__geometry__ && %(bbox)s''' % locals()


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
        queries_to_execute.append(
            (layer_datum, query))
    return queries_to_execute


class RenderDataFetcher(object):

    def __init__(self, conn_info, layer_data, formats,
                 find_columns_for_queries=find_columns_for_queries):
        # copy conn_info so we can pop dbnames off
        self.conn_info = dict(conn_info)
        self.formats = formats
        self.layer_data = layer_data
        self.spherical_mercator = SphericalMercator()
        self.find_columns_for_queries = find_columns_for_queries
        self.thread_pool = None
        self._is_initialized = False
        self.dbnames = None
        self.dbnames_query_index = 0

    def initialize(self, thread_pool):
        assert not self._is_initialized, 'Multiple initialization'

        self.thread_pool = thread_pool

        n_layers = len(self.layer_data)
        n_conn = n_layers

        self.dbnames = self.conn_info.pop('dbnames')
        self.sql_conn_pool = DBAffinityConnections(
            self.dbnames, n_conn, self.conn_info)
        self.dbnames_query_index = 0

        self._is_initialized = True

    def __call__(self, coord):
        assert self._is_initialized, 'Need to call initialize first'

        ul = self.spherical_mercator.coordinateProj(coord)
        lr = self.spherical_mercator.coordinateProj(coord.down().right())
        minx = min(ul.x, lr.x)
        miny = min(ul.y, lr.y)
        maxx = max(ul.x, lr.x)
        maxy = max(ul.y, lr.y)
        bounds = minx, miny, maxx, maxy

        zoom = coord.zoom
        tolerance = tolerances[zoom]
        padding = 5 * tolerance

        # the vtm renderer needs features a little surrounding the
        # bounding box as well, these padded bounds are used in the
        # queries
        padded_bounds = (
            minx - padding, miny - padding,
            maxx + padding, maxy + padding,
        )
        shape_padded_bounds = geometry.box(*padded_bounds)

        dbname = self.dbnames[self.dbnames_query_index]
        self.dbnames_query_index += 1
        if self.dbnames_query_index == len(self.dbnames):
            self.dbnames_query_index = 0

        sql_conns = self.sql_conn_pool.get_conns_for_db(dbname)
        try:
            # first determine the columns for the queries
            # we currently perform the actual query and ask for no data
            # we also cache this per layer, per zoom
            col_conn_info = dict(self.conn_info, dbname=dbname)
            columns_for_queries = self.find_columns_for_queries(
                col_conn_info, self.layer_data, zoom, padded_bounds)

            # the padded bounds are used here in order to only have to
            # issue a single set of queries to the database for all
            # formats
            empty_results, async_results = enqueue_queries(
                sql_conns, self.thread_pool, self.layer_data, zoom,
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

                geometry_types = layer_datum['geometry_types']
                layer_name = layer_datum['name']
                features = []
                for row in rows:
                    assert '__geometry__' in row, \
                        'Missing __geometry__ in query for: %s' % layer_name
                    assert '__id__' in row, \
                        'Missing __id__ in query for: %s' % layer_name

                    wkb = bytes(row.pop('__geometry__'))
                    shape = loads(wkb)

                    if shape.is_empty:
                        continue

                    if geometry_types is not None:
                        geom_type = shape.__geo_interface__['type']
                        if geom_type not in geometry_types:
                            continue

                    if not shape_padded_bounds.intersects(shape):
                        continue

                    feature_id = row.pop('__id__')
                    props = dict((k, v) for k, v in row.items()
                                 if v is not None)

                    # the shapely object itself is kept here, it gets
                    # converted back to wkb during transformation
                    features.append((shape, props, feature_id))

                feature_layer = dict(name=layer_name, features=features,
                                     layer_datum=layer_datum)
                feature_layers.append(feature_layer)

            # bail if an error occurred
            if async_exception is not None:
                raise async_exception

            feature_layers.extend(empty_results)
            render_data = [
                RenderData(format, feature_layers, bounds, padded_bounds)
                for format in self.formats
            ]

            return render_data

        finally:
            self.sql_conn_pool.put_conns_for_db(dbname)


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


half_circumference_meters = 20037508.342789244


def mercator_point_to_wgs84(point):
    x, y = point

    x /= half_circumference_meters
    y /= half_circumference_meters

    y = (2 * math.atan(math.exp(y * math.pi)) - (math.pi / 2)) / math.pi

    x *= 180
    y *= 180

    return x, y


def rescale_point(bounds, scale):
    minx, miny, maxx, maxy = bounds

    def fn(point):
        x, y = point

        xfac = scale / (maxx - minx)
        yfac = scale / (maxy - miny)
        x = x * xfac - minx * xfac
        y = y * yfac - miny * yfac

        return x, y

    return fn


def apply_to_all_coords(fn):
    return lambda shape: transform(shape, fn)


def transform_feature_layers(feature_layers, format, scale, unpadded_bounds,
                             padded_bounds, coord):
    if format in (json_format, topojson_format):
        transform_fn = apply_to_all_coords(mercator_point_to_wgs84)
    elif format in (mapbox_format, vtm_format):
        transform_fn = apply_to_all_coords(
            rescale_point(unpadded_bounds, scale))
    else:
        # in case we add a new format, default to no transformation
        transform_fn = lambda shape: shape

    is_vtm_format = format == vtm_format
    shape_unpadded_bounds = geometry.box(*unpadded_bounds)
    shape_padded_bounds = geometry.box(*padded_bounds)

    transformed_feature_layers = []
    for feature_layer in feature_layers:
        features = feature_layer['features']
        transformed_features = []

        layer_datum = feature_layer['layer_datum']
        is_clipped = layer_datum['is_clipped']

        for shape, props, feature_id in features:

            if is_vtm_format:
                if is_clipped:
                    shape = shape.intersection(shape_padded_bounds)
            else:
                # for non vtm formats, we need to explicitly check if
                # the geometry intersects with the unpadded bounds
                if not shape_unpadded_bounds.intersects(shape):
                    continue
                # now we know that we should include the geometry, but
                # if the geometry should be clipped, we'll clip to the
                # unpadded bounds
                if is_clipped:
                    shape = shape.intersection(shape_unpadded_bounds)

            # perform any simplification as necessary
            tolerance = tolerances[coord.zoom]
            simplify_until = layer_datum['simplify_until']
            suppress_simplification = layer_datum['suppress_simplification']
            if (coord.zoom not in suppress_simplification and
                    coord.zoom < simplify_until):
                shape = shape.simplify(tolerance, preserve_topology=True)

            # perform the format specific geometry transformations
            shape = transform_fn(shape)

            # apply any configured layer transformations
            layer_transform_fn = layer_datum['transform_fn']
            if layer_transform_fn is not None:
                shape, props, feature_id = layer_transform_fn(
                    shape, props, feature_id)

            # the formatters all expect wkb
            wkb = dumps(shape)

            transformed_features.append((wkb, props, feature_id))

        sort_fn = layer_datum['sort_fn']
        if sort_fn:
            transformed_features = sort_fn(transformed_features)

        transformed_feature_layer = dict(
            name=feature_layer['name'],
            features=transformed_features,
            layer_datum=layer_datum,
        )
        transformed_feature_layers.append(transformed_feature_layer)

    return transformed_feature_layers


class RenderJob(object):

    scale = 4096

    def __init__(self, coord, formats, feature_fetcher, store, thread_pool):
        self.coord = coord
        self.formats = formats
        self.feature_fetcher = feature_fetcher
        self.store = store
        self.thread_pool = thread_pool

    def __call__(self):
        render_data = self.feature_fetcher(self.coord)
        # sort based on formatter
        # this is done to place the mapbox format last, as it mutates
        # the properties to add a uid property
        render_data.sort(key=lambda x: x.format.sort_key)

        async_jobs = []
        for render_datum in render_data:
            format = render_datum.format
            feature_layers = render_datum.feature_layers
            bounds = render_datum.bounds

            feature_layers = transform_feature_layers(
                feature_layers, format, self.scale, bounds,
                render_datum.padded_bounds, self.coord)

            tile_data_file = StringIO()
            format.format_tile(tile_data_file, feature_layers, self.coord,
                               bounds)
            tile_data = tile_data_file.getvalue()
            async_result = self.thread_pool.apply_async(
                self.store.write_tile,
                (tile_data, self.coord, format)
            )
            async_jobs.append(async_result)

        for async_job in async_jobs:
            async_job.wait()

    def __repr__(self):
        return 'RenderJob(%s, %s)' % (self.coord, self.format)


class RenderJobCreator(object):

    def __init__(self, tilestache_config, formats, store, feature_fetcher):
        self.tilestache_config = tilestache_config
        self.formats = formats
        self.feature_fetcher = feature_fetcher
        self.store = store

    def initialize(self):
        # create a thread pool, shared between fetching features and
        # writing to s3
        n_layers = len(self.feature_fetcher.layer_data)
        n_threads = n_layers
        self.thread_pool = ThreadPool(n_threads)

        # process local initialization
        self.feature_fetcher.initialize(self.thread_pool)

    def create(self, coord):
        return RenderJob(coord, self.formats, self.feature_fetcher,
                         self.store, self.thread_pool)

    def process_jobs_for_coord(self, coord):
        job = self.create(coord)
        job()


def make_feature_fetcher(conn_info, tilestache_config, formats):
    layers = tilestache_config.layers
    all_layer = layers.get('all')
    assert all_layer is not None, 'All layer is expected in tilestache config'
    layer_names = all_layer.provider.names
    layer_data = []
    for layer_name in layer_names:
        assert layer_name in layers, \
            ('Layer not found in config but found in all layers: %s'
             % layer_name)
        layer = layers[layer_name]
        layer_datum = dict(
            name=layer_name,
            queries=layer.provider.queries,
            is_clipped=layer.provider.clip,
            geometry_types=layer.provider.geometry_types,
            simplify_until=layer.provider.simplify_until,
            suppress_simplification=layer.provider.suppress_simplification,
            transform_fn=layer.provider.transform_fn,
            sort_fn=layer.provider.sort_fn,
        )
        layer_data.append(layer_datum)

    data_fetcher = RenderDataFetcher(conn_info, layer_data, formats)
    return data_fetcher
