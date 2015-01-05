from cStringIO import StringIO
from multiprocessing.pool import ThreadPool
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from shapely.wkb import dumps
from shapely.wkb import loads
from tilequeue.format import json_format
from tilequeue.format import mapbox_format
from tilequeue.format import topojson_format
from tilequeue.format import vtm_format
from TileStache.Geography import SphericalMercator
from TileStache.Goodies.VecTiles.ops import transform
from TileStache.Goodies.VecTiles.server import build_query
from TileStache.Goodies.VecTiles.server import query_columns
from TileStache.Goodies.VecTiles.server import tolerances
import math
import traceback


# This is what will get passed from fetching data. Ideally, this would
# be separate from the data, but at the moment the opensciencemap
# renderer mandates slightly different data. This forces us to track
# which data is which to ultimately dispatch on the correct formatter.
class RenderData(object):

    def __init__(self, format, feature_layers, bounds):
        self.format = format
        self.feature_layers = feature_layers
        self.bounds = bounds


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


def build_feature_queries(bounds, layer_data, zoom, tolerance,
                          padding, scale, columns_for_queries):
    is_geo = False
    srid = 900913
    queries_to_execute = []
    for layer_datum, columns in zip(layer_data, columns_for_queries):
        queries = layer_datum['queries']
        subquery = queries[min(zoom, len(queries) - 1)]
        if subquery is None:
            query = None
        else:
            if (zoom >= layer_datum['simplify_until'] or
                    zoom in layer_datum['suppress_simplification']):
                tolerance = None
            query = build_query(
                srid, subquery, columns, bounds, tolerance,
                is_geo, layer_datum['is_clipped'], padding, scale)
        queries_to_execute.append(
            (layer_datum, query))
    return queries_to_execute


class RenderDataFetcher(object):

    def __init__(self, conn_info, layer_data, formats,
                 find_columns_for_queries=find_columns_for_queries):
        self.conn_info = conn_info
        self.formats = formats
        self.layer_data = layer_data
        self.spherical_mercator = SphericalMercator()
        self.find_columns_for_queries = find_columns_for_queries
        self.thread_pool = None
        self._is_initialized = False

    def initialize(self, thread_pool):
        assert not self._is_initialized, 'Multiple initialization'

        self.thread_pool = thread_pool

        # we expect to execute 2 sets of queries for each layer
        n_layers = len(self.layer_data)
        n_conn = n_layers * 2

        # Setting the min number of connections to the same as the max
        # forces all connections to be created up front. This should
        # help with performance, but more importantly will force any
        # round robin connection factory to do all its work initially,
        # rather than become interleaved with fetching the columns
        # from postgresql, and potentially ending up with an
        # imbalanced set of hosts.
        self.sql_conn_pool = ThreadedConnectionPool(n_conn, n_conn,
                                                    **self.conn_info)

        self._is_initialized = True

    def __call__(self, coord):
        assert self._is_initialized, 'Need to call initialize first'

        ul = self.spherical_mercator.coordinateProj(coord)
        lr = self.spherical_mercator.coordinateProj(coord.down().right())
        bounds = (
            min(ul.x, lr.x),
            min(ul.y, lr.y),
            max(ul.x, lr.x),
            max(ul.y, lr.y)
        )

        zoom = coord.zoom
        tolerance = tolerances[zoom]
        non_vtm_padding = 0
        vtm_padding = 5 * tolerance
        # scaling for mapbox format will be performed in python
        non_vtm_scale = None
        vtm_scale = 4096

        has_vtm = any((format == vtm_format for format in self.formats))
        has_non_vtm = any((format != vtm_format for format in self.formats))

        # first determine the columns for the queries
        # we currently perform the actual query and ask for no data
        # we also cache this per layer, per zoom
        columns_for_queries = self.find_columns_for_queries(
            self.conn_info, self.layer_data, zoom, bounds)

        render_data = []

        if has_vtm:
            vtm_empty_results, vtm_async_results = enqueue_queries(
                self.thread_pool, self.sql_conn_pool,
                self.layer_data, zoom, bounds, tolerance, vtm_padding,
                vtm_scale, columns_for_queries)

        if has_non_vtm:
            non_vtm_empty_results, non_vtm_async_results = enqueue_queries(
                self.thread_pool, self.sql_conn_pool,
                self.layer_data, zoom, bounds, tolerance,
                non_vtm_padding, non_vtm_scale, columns_for_queries)

        def feature_layers_from_results(async_results):
            feature_layers = []
            for async_result in async_results:
                rows, layer_datum = async_result.get()

                geometry_types = layer_datum['geometry_types']
                layer_name = layer_datum['name']
                features = []
                for row in rows:
                    assert '__geometry__' in row, \
                        'Missing __geometry__ in query for: %s' % layer_name
                    assert '__id__' in row, \
                        'Missing __id__ in query for: %s' % layer_name

                    wkb = bytes(row.pop('__geometry__'))
                    id = row.pop('__id__')

                    if geometry_types is not None:
                        shape = loads(wkb)
                        geom_type = shape.__geo_interface__['type']
                        if geom_type not in geometry_types:
                            continue

                    props = dict((k, v) for k, v in row.items()
                                 if v is not None)
                    features.append((wkb, props, id))

                feature_layer = dict(name=layer_name, features=features,
                                     layer_datum=layer_datum)
                feature_layers.append(feature_layer)
            return feature_layers

        if has_vtm:
            vtm_feature_layers = feature_layers_from_results(vtm_async_results)
            vtm_feature_layers.extend(vtm_empty_results)
            vtm_render_data = RenderData(
                vtm_format, vtm_feature_layers, bounds)
            render_data.append(vtm_render_data)

        if has_non_vtm:
            non_vtm_feature_layers = feature_layers_from_results(
                non_vtm_async_results)
            non_vtm_feature_layers.extend(non_vtm_empty_results)
            non_vtm_render_data = [
                RenderData(format, non_vtm_feature_layers, bounds)
                for format in self.formats if format != vtm_format
            ]
            render_data.extend(non_vtm_render_data)

        return render_data


def execute_query(conn_pool, query, layer_datum):
    conn = conn_pool.getconn()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        rows = list(cursor.fetchall())
        return rows, layer_datum
    finally:
        # more resilient way to put conn back to pool
        conn_pool._lock.acquire()
        try:
            conn_pool._putconn(conn)
        except:
            print 'Error: exception when returning conn to pool'
            print traceback.format_exc()

            # ensure that the conn is completely cleared from the pool
            conn_id = id(conn)
            # remove indexes to connection
            key = conn_pool._rused.get(conn_id)
            if key is not None:
                del conn_pool._rused[conn_id]
                if key in conn_pool._used:
                    del conn_pool._used[key]
            # remove connection from pool
            for i, x in enumerate(conn_pool._pool):
                if conn_id == id(x):
                    del conn_pool._pool[i]
                    break
            # and safely close the connection
            try:
                conn.close()
            except:
                pass

            print 'Cleared connection from pool'

        finally:
            conn_pool._lock.release()


def enqueue_queries(thread_pool, conn_pool, layer_data, zoom, bounds,
                    tolerance, padding, scale, columns):
    queries_to_execute = build_feature_queries(
        bounds, layer_data, zoom,
        tolerance, padding, scale, columns)

    empty_results = []
    async_results = []
    for layer_datum, query in queries_to_execute:
        if query is None:
            empty_feature_layer = dict(
                name=layer_datum['name'],
                features=[],
                layer_datum=layer_datum,
            )
            empty_results.append(empty_feature_layer)
        else:
            async_result = thread_pool.apply_async(
                execute_query, (conn_pool, query, layer_datum))
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


def transform_feature_layers(feature_layers, format, bounds, scale):
    if format in (json_format, topojson_format):
        transform_fn = apply_to_all_coords(mercator_point_to_wgs84)
    elif format == mapbox_format:
        transform_fn = apply_to_all_coords(rescale_point(bounds, scale))
    else:
        # because vtm gets its own query, it doesn't need any post processing
        return feature_layers

    transformed_feature_layers = []
    for feature_layer in feature_layers:
        features = feature_layer['features']
        transformed_features = []
        for wkb, props, id in features:
            shape = loads(wkb)
            new_shape = transform_fn(shape)
            new_wkb = dumps(new_shape)
            transformed_features.append((new_wkb, props, id))
        transformed_feature_layer = dict(
            name=feature_layer['name'],
            features=transformed_features,
            layer_datum=feature_layer['layer_datum'],
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
        async_jobs = []
        for render_datum in render_data:
            format = render_datum.format
            feature_layers = render_datum.feature_layers
            bounds = render_datum.bounds

            feature_layers = transform_feature_layers(
                feature_layers, format, bounds, self.scale)

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
        # we execute vtm queries concurrently
        n_threads = n_layers * 2
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
    # layer_data interface:
    # list of dicts with these keys: name, queries, is_clipped, geometry_types

    layers = tilestache_config.layers
    all_layer = layers.get('all')
    assert all_layer is not None, 'All layer is expected in tilestache config'
    layer_names = all_layer.provider.names
    layer_data = []
    for layer_name in layer_names:
        # NOTE: obtain postgis connection information from first layer
        # this assumes all connection info is exactly the same
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
        )
        layer_data.append(layer_datum)

    data_fetcher = RenderDataFetcher(conn_info, layer_data, formats)
    return data_fetcher
