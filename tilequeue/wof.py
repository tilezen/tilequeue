from collections import namedtuple
from contextlib import closing
from cStringIO import StringIO
from operator import attrgetter
from shapely import geos
from tilequeue.tile import coord_marshall_int
from tilequeue.tile import coord_unmarshall_int
from tilequeue.tile import mercator_point_to_coord
import csv
import psycopg2
import pyproj
import Queue
import requests
import shapely.wkb
import threading

merc_proj = pyproj.Proj(init='epsg:3857')
latlng_proj = pyproj.Proj(proj='latlong')


def generate_csv_lines(requests_result):
    for line in requests_result.iter_lines():
        if line:
            yield line


Neighbourhood = namedtuple('Neighbourhood', 'wof_id name x y')


def fetch_wof_neighbourhoods(url):
    r = requests.get(url, stream=True)
    csv_line_generator = generate_csv_lines(r)
    reader = csv.reader(csv_line_generator)

    it = iter(reader)
    header = it.next()

    lbl_lat_idx = header.index('lbl_latitude')
    lbl_lng_idx = header.index('lbl_longitude')
    name_idx = header.index('name')
    wof_id_idx = header.index('id')

    min_row_length = max(lbl_lat_idx, lbl_lng_idx, name_idx, wof_id_idx) + 1

    for row in it:
        if len(row) < min_row_length:
            continue

        wof_id_str = row[wof_id_idx]
        if not wof_id_str:
            continue
        try:
            wof_id = int(wof_id_str)
        except ValueError:
            continue

        name = row[name_idx]
        if not name:
            continue

        lat_str = row[lbl_lat_idx]
        lng_str = row[lbl_lng_idx]
        try:
            lat = float(lat_str)
            lng = float(lng_str)
        except ValueError:
            continue

        x, y = pyproj.transform(latlng_proj, merc_proj, lng, lat)

        neighbourhood = Neighbourhood(wof_id, name, x, y)
        yield neighbourhood


class WofNeighbourhoodFetcher(object):

    def __init__(self, neighbourhood_url):
        self.neighbourhood_url = neighbourhood_url

    def __call__(self):
        return fetch_wof_neighbourhoods(self.neighbourhood_url)


def create_neighbourhood_file_object(neighbourhoods):
    # tell shapely to include the srid when generating WKBs
    geos.WKBWriter.defaults['include_srid'] = True

    buf = StringIO()
    for n in neighbourhoods:
        buf.write('%d\t' % n.wof_id)
        buf.write('%s\t' % n.name)

        p = shapely.geometry.Point(n.x, n.y)
        geos.lgeos.GEOSSetSRID(p._geom, 900913)
        buf.write(p.wkb_hex)
        buf.write('\n')

    buf.seek(0)

    return buf


class WofModel(object):

    def __init__(self, postgresql_conn_info):
        self.postgresql_conn_info = postgresql_conn_info
        self.table = 'wof_neighbourhood'

    def _create_conn(self):
        conn = psycopg2.connect(**self.postgresql_conn_info)
        conn.set_session(autocommit=False)
        return conn

    def find_previous_neighbourhoods(self):
        with closing(self._create_conn()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'SELECT wof_id, name, ST_AsBinary(label_position) '
                    'FROM %s ORDER BY wof_id ASC' % self.table)

                neighbourhoods = []
                for row in cursor:
                    wof_id, name, label_bytes = row

                    wof_id = int(wof_id)
                    label_bytes = bytes(label_bytes)
                    label_shape = shapely.wkb.loads(label_bytes)
                    x = label_shape.x
                    y = label_shape.y

                    neighbourhood = Neighbourhood(wof_id, name, x, y)
                    neighbourhoods.append(neighbourhood)
                return neighbourhoods

    def insert_neighbourhoods(self, neighbourhoods,
                              has_existing_neighbourhoods):
        # create this whole input file like object outside of the transaction
        nf = create_neighbourhood_file_object(neighbourhoods)
        # close the connection
        with closing(self._create_conn()) as conn:
            # commit the transaction
            with conn as conn:
                with conn.cursor() as cursor:
                    if has_existing_neighbourhoods:
                        cursor.execute('TRUNCATE %s' % self.table)
                    cursor.copy_from(nf, self.table)


def diff_neighbourhoods(xs, ys):
    # NOTE this requires that both xs and ys be sequences of
    # neighbourhoods, sorted by wof_id in ascending order

    # returns a sequence of tuples:
    # (None, x) -> neighbourhoods that have been added
    # (x, None) -> neighbourhoods that have been removed
    # (x, y)    -> neighbourhoods that have been updated
    diffs = []

    n_xs = len(xs)
    n_ys = len(ys)
    idx_xs = 0
    idx_ys = 0

    # iterate through both lists while we still have values for both
    while idx_xs < n_xs and idx_ys < n_ys:

        x = xs[idx_xs]
        y = ys[idx_ys]

        if x.wof_id < y.wof_id:
            diffs.append((x, None))
            idx_xs += 1
            continue

        if y.wof_id < x.wof_id:
            diffs.append((None, y))
            idx_ys += 1
            continue

        if (x.name != y.name or
                abs(x.x - y.x) > 1e-9 or
                abs(x.y - y.y) > 1e-9):
            diffs.append((x, y))

        idx_xs += 1
        idx_ys += 1

    # catch any differences
    while idx_xs < n_xs:
        x = xs[idx_xs]
        diffs.append((x, None))
        idx_xs += 1

    while idx_ys < n_ys:
        y = ys[idx_ys]
        diffs.append((None, y))
        idx_ys += 1

    return diffs


def coord_int_at_mercator_point(z, x, y):
    coord = mercator_point_to_coord(z, x, y)
    coord_int = coord_marshall_int(coord)
    return coord_int


def generate_tile_expiry_list(zoom, diffs):
    coord_ints = set()

    def add_neighbourhood_diff(n):
        if n is not None:
            coord_int = coord_int_at_mercator_point(zoom, n.x, n.y)
            coord_ints.add(coord_int)

    for n1, n2 in diffs:
        # for our purposes, we will expire any kind of modification,
        # whether the neighbourhoods were added, removed, or updated
        add_neighbourhood_diff(n1)
        add_neighbourhood_diff(n2)

    return coord_ints


class WofProcessor(object):

    def __init__(self, fetcher, model, redis_cache_index, intersector,
                 coords_enqueuer, logger):
        self.fetcher = fetcher
        self.model = model
        self.redis_cache_index = redis_cache_index
        self.intersector = intersector
        self.coords_enqueuer = coords_enqueuer
        self.logger = logger
        self.zoom_expiry = 18
        self.zoom_until = 11

    def __call__(self):
        # perform IO to get old/new neighbourhoods and tiles of
        # interest in parallel

        # queues to pass the results through the threads
        prev_neighbourhoods_queue = Queue.Queue(1)
        next_neighbourhoods_queue = Queue.Queue(1)
        toi_queue = Queue.Queue(1)

        # functions for the threads
        def find_prev_neighbourhoods():
            prev_neighbourhoods = self.model.find_previous_neighbourhoods()
            prev_neighbourhoods_queue.put(prev_neighbourhoods)

        def fetch_next_neighbourhoods():
            # ensure that we have a list here
            next_neighbourhoods = list(self.fetcher())
            next_neighbourhoods_queue.put(next_neighbourhoods)

        def fetch_toi():
            toi = self.redis_cache_index.fetch_tiles_of_interest()
            toi_queue.put(toi)

        self.logger.info('Fetching tiles of interest in background ...')
        self.logger.info('Fetching old and new neighbourhoods ...')

        # start the threads in parallel
        prev_neighbourhoods_thread = threading.Thread(
            target=find_prev_neighbourhoods)
        prev_neighbourhoods_thread.start()

        next_neighbourhoods_thread = threading.Thread(
            target=fetch_next_neighbourhoods)
        next_neighbourhoods_thread.start()

        toi_thread = threading.Thread(target=fetch_toi)
        toi_thread.start()

        # ensure we're done with finding the next and previous
        # neighbourhoods by this point
        prev_neighbourhoods_thread.join()
        next_neighbourhoods_thread.join()

        self.logger.info('Fetching old and new neighbourhoods ... done')

        prev_neighbourhoods = prev_neighbourhoods_queue.get()
        next_neighbourhoods = next_neighbourhoods_queue.get()
        has_existing_neighbourhoods = bool(prev_neighbourhoods)

        if has_existing_neighbourhoods:
            self.logger.info('Existing neighbourhoods detected, diffing ...')
            by_neighborhood_id = attrgetter('wof_id')
            # the model is expected to return records in ascending order by id
            # it doesn't seem like the neighbourhoods in the wof csv
            # are in ascending order, so we sort explicitly here
            next_neighbourhoods.sort(key=by_neighborhood_id)
            # the diff algorithm depends on the neighbourhood lists
            # being in sorted order by id
            diffs = diff_neighbourhoods(prev_neighbourhoods,
                                        next_neighbourhoods)
            self.logger.info('Diff complete')
        else:
            self.logger.info('No existing neighbourhooods found')
            # on first run, all neighbourhoods are treated as additions
            diffs = [(None, x) for x in next_neighbourhoods]

        self.logger.info('Generating tile expiry list ...')
        expired_coord_ints = generate_tile_expiry_list(self.zoom_expiry, diffs)
        self.logger.info('Generating tile expiry list ... done - '
                         'Found %d expired tiles' % len(expired_coord_ints))

        # ensure we're done fetching the tiles of interest by this point
        toi_thread.join()
        toi = toi_queue.get()

        self.logger.info('Have tiles of interest')

        # intersect the tiles of interest with the expired coords from
        # the neighbourhood diff
        self.logger.info('Intersecting %d tiles of interest with %d expired '
                         'tiles' % (len(toi), len(expired_coord_ints)))
        toi_expired_coord_ints = self.intersector(
            expired_coord_ints, toi, self.zoom_until)
        coords = map(coord_unmarshall_int, toi_expired_coord_ints)
        self.logger.info('Intersection complete, will expired %d tiles' %
                         len(coords))

        # we shouldn't enqueue the coordinates and insert the data at
        # the same time because we may end up in a situation where we
        # process an affected tile before the new data exists in
        # postgresql

        if diffs:
            # only insert neighbourhoods if we actually have some diffs
            self.logger.info('Inserting %d neighbourhoods ...' %
                             len(next_neighbourhoods))
            self.model.insert_neighbourhoods(next_neighbourhoods,
                                             has_existing_neighbourhoods)
            self.logger.info('Inserting %d neighbourhoods ... done' %
                             len(next_neighbourhoods))
        else:
            self.logger.info('No diffs found, not updating data')

        if coords:
            self.logger.info('Asking enqueuer to enqueue %d coords ...' %
                             len(coords))
            self.coords_enqueuer(coords)
            self.logger.info('Asking enqueuer to enqueue %d coords ... done' %
                             len(coords))
        else:
            self.logger.info('No expired tiles to enqueue')


def make_wof_neighbourhood_fetcher(neighbourhood_url):
    fetcher = WofNeighbourhoodFetcher(neighbourhood_url)
    return fetcher


def make_wof_model(postgresql_conn_info):
    wof_model = WofModel(postgresql_conn_info)
    return wof_model


def make_wof_processor(
        fetcher, model, redis_cache_index, sqs_queue, n_threads, logger):
    from tilequeue.command import explode_and_intersect
    from tilequeue.command import ThreadedEnqueuer
    threaded_enqueuer = ThreadedEnqueuer(sqs_queue, n_threads, logger)
    wof_processor = WofProcessor(
        fetcher, model, redis_cache_index, explode_and_intersect,
        threaded_enqueuer, logger)
    return wof_processor
