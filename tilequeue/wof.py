from collections import namedtuple
from contextlib import closing
from cStringIO import StringIO
from itertools import imap
from operator import attrgetter
from shapely import geos
from tilequeue.tile import coord_marshall_int
from tilequeue.tile import coord_unmarshall_int
from tilequeue.tile import mercator_point_to_coord
import csv
import json
import os.path
import psycopg2
import pyproj
import Queue
import requests
import shapely.geometry
import shapely.wkb
import threading

merc_proj = pyproj.Proj(init='epsg:3857')
latlng_proj = pyproj.Proj(proj='latlong')


def generate_csv_lines(requests_result):
    for line in requests_result.iter_lines():
        if line:
            yield line


NeighbourhoodMeta = namedtuple(
    'NeighbourhoodMeta',
    'wof_id name label_position hash')
Neighbourhood = namedtuple(
    'Neighbourhood',
    'wof_id name label_position hash geometry n_photos')


def parse_neighbourhood_meta_csv(csv_line_generator):
    reader = csv.reader(csv_line_generator)

    it = iter(reader)
    header = it.next()

    lbl_lat_idx = header.index('lbl_latitude')
    lbl_lng_idx = header.index('lbl_longitude')
    name_idx = header.index('name')
    wof_id_idx = header.index('id')
    hash_idx = header.index('file_hash')

    min_row_length = max(
        lbl_lat_idx, lbl_lng_idx, name_idx, wof_id_idx, hash_idx) + 1

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

        file_hash = row[hash_idx]

        label_x, label_y = pyproj.transform(latlng_proj, merc_proj, lng, lat)
        label_position = shapely.geometry.Point(label_x, label_y)

        neighbourhood_meta = NeighbourhoodMeta(
            wof_id, name, label_position, file_hash)
        yield neighbourhood_meta


def fetch_wof_url_meta_neighbourhoods(url):
    r = requests.get(url, stream=True)
    assert r.status_code == 200, 'Failure requesting: %s' % url

    csv_line_generator = generate_csv_lines(r)
    return parse_neighbourhood_meta_csv(csv_line_generator)


def create_neighbourhood_from_json(json_data, neighbourhood_meta):
    wof_id = json_data['id']

    geometry = json_data['geometry']
    shape = shapely.geometry.shape(geometry)

    props = json_data['properties']
    name = props['wof:name']
    n_photos = props.get('misc:photo_sum')
    label_lat = props['lbl:latitude']
    label_lng = props['lbl:longitude']
    label_merc_x, label_merc_y = pyproj.transform(latlng_proj, merc_proj,
                                                  label_lng, label_lat)
    label_position = shapely.geometry.Point(label_merc_x, label_merc_y)

    neighbourhood = Neighbourhood(
        wof_id, name, label_position, neighbourhood_meta.hash, shape, n_photos)
    return neighbourhood


def fetch_url_raw_neighbourhood(url, neighbourhood_meta):
    r = requests.get(url)
    assert r.status_code == 200, 'Failure requesting: %s' % url

    doc = r.json()
    neighbourhood = create_neighbourhood_from_json(doc, neighbourhood_meta)
    return neighbourhood


def fetch_fs_raw_neighbourhood(path, neighbourhood_meta):
    with open(path) as fp:
        json_data = json.load(fp)
    neighbourhood = create_neighbourhood_from_json(json_data,
                                                   neighbourhood_meta)
    return neighbourhood


def generate_wof_url(url_prefix, wof_id):
    wof_id_str = str(wof_id)
    grouped = []
    grouping = []
    for c in wof_id_str:
        grouping.append(c)
        if len(grouping) == 3:
            grouped.append(grouping)
            grouping = []
    if grouping:
        grouped.append(grouping)
    grouped_part = '/'.join([''.join(part) for part in grouped])
    wof_url = '%s/%s/%s.geojson' % (url_prefix, grouped_part, wof_id_str)
    return wof_url


def make_fetch_raw_url_fn(data_url_prefix):
    def fn(neighbourhood_meta):
        wof_url = generate_wof_url(
            data_url_prefix, neighbourhood_meta.wof_id)
        neighbourhood = fetch_url_raw_neighbourhood(wof_url,
                                                    neighbourhood_meta)
        return neighbourhood
    return fn


def make_fetch_raw_filesystem_fn(data_path):
    def fn(neighbourhood_meta):
        # this will work for OS's with / separators
        wof_path = generate_wof_url(
            data_path, neighbourhood_meta.wof_id)
        neighbourhood = fetch_fs_raw_neighbourhood(wof_path,
                                                   neighbourhood_meta)
        return neighbourhood
    return fn


def threaded_fetch(neighbourhood_metas, n_threads, fetch_raw_fn):
    queue_size = n_threads * 10
    neighbourhood_input_queue = Queue.Queue(queue_size)
    neighbourhood_output_queue = Queue.Queue(len(neighbourhood_metas))

    def _fetch_raw_neighbourhood():
        while True:
            neighbourhood_meta = neighbourhood_input_queue.get()
            if neighbourhood_meta is None:
                break
            neighbourhood = fetch_raw_fn(neighbourhood_meta)
            neighbourhood_output_queue.put(neighbourhood)

    fetch_threads = []
    for i in xrange(n_threads):
        fetch_thread = threading.Thread(target=_fetch_raw_neighbourhood)
        fetch_thread.start()
        fetch_threads.append(fetch_thread)

    for neighbourhood_meta in neighbourhood_metas:
        neighbourhood_input_queue.put(neighbourhood_meta)

    for fetch_thread in fetch_threads:
        neighbourhood_input_queue.put(None)

    neighbourhoods = []
    for i in xrange(len(neighbourhood_metas)):
        neighbourhood = neighbourhood_output_queue.get()
        neighbourhoods.append(neighbourhood)

    for fetch_thread in fetch_threads:
        fetch_thread.join()

    return neighbourhoods


class WofUrlNeighbourhoodFetcher(object):

    def __init__(self, neighbourhood_url, data_url_prefix, n_threads):
        self.neighbourhood_url = neighbourhood_url
        self.data_url_prefix = data_url_prefix
        self.n_threads = n_threads

    def fetch_meta_neighbourhoods(self):
        return fetch_wof_url_meta_neighbourhoods(self.neighbourhood_url)

    def fetch_raw_neighbourhoods(self, neighbourhood_metas):
        url_fetch_fn = make_fetch_raw_url_fn(self.data_url_prefix)
        neighbourhoods = threaded_fetch(neighbourhood_metas, self.n_threads,
                                        url_fetch_fn)
        return neighbourhoods


class WofFilesystemNeighbourhoodFetcher(object):

    def __init__(self, wof_data_path, n_threads):
        self.wof_data_path = wof_data_path
        self.n_threads = n_threads

    def fetch_meta_neighbourhoods(self):
        meta_fs_path = os.path.join(
            self.wof_data_path, 'meta', 'wof-neighbourhood-latest.csv')
        with open(meta_fs_path) as fp:
            meta_neighbourhoods = list(parse_neighbourhood_meta_csv(fp))
        return meta_neighbourhoods

    def fetch_raw_neighbourhoods(self, neighbourhood_metas):
        data_prefix = os.path.join(
            self.wof_data_path, 'data')
        fs_fetch_fn = make_fetch_raw_filesystem_fn(data_prefix)
        neighbourhoods = threaded_fetch(neighbourhood_metas, self.n_threads,
                                        fs_fetch_fn)
        return neighbourhoods


def create_neighbourhood_file_object(neighbourhoods):
    # tell shapely to include the srid when generating WKBs
    geos.WKBWriter.defaults['include_srid'] = True

    def escape_string(s):
        return s.replace('\t', ' ').replace('\n', ' ')

    buf = StringIO()
    for n in neighbourhoods:
        buf.write('%d\t' % n.wof_id)
        buf.write('%s\t' % escape_string(n.name))
        buf.write('%s\t' % escape_string(n.hash))
        if n.n_photos is None:
            buf.write('\\N\t')
        else:
            buf.write('%d\t' % n.n_photos)

        geos.lgeos.GEOSSetSRID(n.label_position._geom, 900913)
        buf.write(n.label_position.wkb_hex)
        buf.write('\t')

        geos.lgeos.GEOSSetSRID(n.geometry._geom, 900913)
        buf.write(n.geometry.wkb_hex)
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

    def find_previous_neighbourhood_meta(self):
        with closing(self._create_conn()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'SELECT wof_id, name, hash, ST_AsBinary(label_position) '
                    'FROM %s ORDER BY wof_id ASC' % self.table)

                ns = []
                for row in cursor:
                    wof_id, name, hash, label_bytes = row
                    wof_id = int(wof_id)
                    label_bytes = bytes(label_bytes)
                    label_position = shapely.wkb.loads(label_bytes)
                    n = NeighbourhoodMeta(wof_id, name, label_position, hash)
                    ns.append(n)
                return ns

    def sync_neighbourhoods(
            self, neighbourhoods_to_add, neighbourhoods_to_update,
            ids_to_remove):

        def gen_data(n):
            return dict(
                table=self.table,
                name=n.name,
                hash=n.hash,
                n_photos=('NULL' if n.n_photos is None else n.n_photos),
                label_position=n.label_position.wkb_hex,
                geometry=n.geometry.wkb_hex,
                wof_id=n.wof_id,
            )

        # set up all the sync updates in advance
        if ids_to_remove:
            ids_to_remove_str = ', '.join(imap(str, ids_to_remove))
        if neighbourhoods_to_update:
            updates = []
            for n in neighbourhoods_to_update:
                update_data = gen_data(n)
                update_sql = (
                    'UPDATE %(table)s SET '
                    "name='%(name)s', "
                    "hash='%(hash)s', "
                    'n_photos=%(n_photos)s, '
                    'label_position=ST_SetSRID'
                    "('%(label_position)s'::geometry, 900913), "
                    "geometry=ST_SetSRID('%(geometry)s'::geometry, 900913) "
                    'WHERE wof_id=%(wof_id)s' % update_data)
                updates.append(update_sql)
        if neighbourhoods_to_add:
            addition_tuples = []
            for n in neighbourhoods_to_add:
                addition = gen_data(n)
                addition_tuple = (
                    "(%(wof_id)s, '%(name)s', '%(hash)s', %(n_photos)s, "
                    "ST_SetSRID('%(label_position)s'::geometry, 900913), "
                    "ST_SetSRID('%(geometry)s'::geometry, 900913))" % addition)
                addition_tuples.append(addition_tuple)
            inserts = ', '.join(addition_tuples)

        # this closes the connection
        with closing(self._create_conn()) as conn:
            # this commits the transaction
            with conn as conn:
                # this frees any resources associated with the cursor
                with conn.cursor() as cursor:
                    if ids_to_remove:
                        cursor.execute('DELETE FROM %s WHERE wof_id IN (%s)' %
                                       (self.table, ids_to_remove_str))
                    if neighbourhoods_to_update:
                        for update_sql in updates:
                            cursor.execute(update_sql)
                    if neighbourhoods_to_add:
                        cursor.execute(
                            'INSERT INTO %s VALUES %s' % (self.table, inserts))

    def insert_neighbourhoods(self, neighbourhoods):
        # create this whole input file like object outside of the transaction
        nf = create_neighbourhood_file_object(neighbourhoods)
        # close the connection
        with closing(self._create_conn()) as conn:
            # commit the transaction
            with conn as conn:
                with conn.cursor() as cursor:
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

        if x.hash != y.hash:
            # if there are any differences the hash will be different
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
            x = n.label_position.x
            y = n.label_position.y
            coord_int = coord_int_at_mercator_point(zoom, x, y)
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
        meta_neighbourhoods_queue = Queue.Queue(1)
        toi_queue = Queue.Queue(1)

        # functions for the threads
        def find_prev_neighbourhoods():
            prev_neighbourhoods = (
                self.model.find_previous_neighbourhood_meta())
            prev_neighbourhoods_queue.put(prev_neighbourhoods)

        def fetch_meta_neighbourhoods():
            # ensure that we have a list here
            meta_neighbourhoods = list(
                self.fetcher.fetch_meta_neighbourhoods())
            meta_neighbourhoods_queue.put(meta_neighbourhoods)

        def fetch_toi():
            toi = self.redis_cache_index.fetch_tiles_of_interest()
            toi_queue.put(toi)

        self.logger.info('Fetching tiles of interest in background ...')
        self.logger.info('Fetching old and new neighbourhoods ...')

        # start the threads in parallel
        prev_neighbourhoods_thread = threading.Thread(
            target=find_prev_neighbourhoods)
        prev_neighbourhoods_thread.start()

        meta_neighbourhoods_thread = threading.Thread(
            target=fetch_meta_neighbourhoods)
        meta_neighbourhoods_thread.start()

        toi_thread = threading.Thread(target=fetch_toi)
        toi_thread.start()

        # ensure we're done with finding the next and previous
        # neighbourhoods by this point
        prev_neighbourhoods_thread.join()
        meta_neighbourhoods_thread.join()

        self.logger.info('Fetching old and new neighbourhoods ... done')

        prev_neighbourhoods = prev_neighbourhoods_queue.get()
        meta_neighbourhoods = meta_neighbourhoods_queue.get()

        self.logger.info('Diffing neighbourhoods ...')
        by_neighborhood_id = attrgetter('wof_id')
        # the model is expected to return records in ascending order by id
        # it doesn't seem like the neighbourhoods in the wof csv
        # are in ascending order, so we sort explicitly here
        meta_neighbourhoods.sort(key=by_neighborhood_id)
        # the diff algorithm depends on the neighbourhood lists
        # being in sorted order by id
        diffs = diff_neighbourhoods(prev_neighbourhoods,
                                    meta_neighbourhoods)
        self.logger.info('Diffing neighbourhoods ... done')

        # we need to fetch neighbourhoods that have either been
        # updated or are new
        wof_neighbourhoods_to_fetch = []
        # based on the diff, we'll need to keep track of how we'll
        # need to update
        ids_to_add = set()
        ids_to_update = set()
        ids_to_remove = set()
        for dx, dy in diffs:
            if dy is not None:
                if dx is None:
                    ids_to_add.add(dy.wof_id)
                else:
                    ids_to_update.add(dy.wof_id)
                wof_neighbourhoods_to_fetch.append(dy)
            else:
                ids_to_remove.add(dx.wof_id)

        if wof_neighbourhoods_to_fetch:
            self.logger.info('Fetching %d raw neighbourhoods ...' %
                             len(wof_neighbourhoods_to_fetch))
            raw_neighbourhoods = self.fetcher.fetch_raw_neighbourhoods(
                wof_neighbourhoods_to_fetch)
            self.logger.info('Fetching %d raw neighbourhoods ... done' %
                             len(wof_neighbourhoods_to_fetch))
        else:
            self.logger.info('No raw neighbourhoods found to fetch')
            raw_neighbourhoods = ()

        sync_neighbourhoods_thread = None
        if diffs:
            self.logger.info("Sync'ing neighbourhoods ...")
            # raw_neighbourhoods contains both the neighbourhoods to
            # add and update
            # we split it up here
            neighbourhoods_to_update = []
            neighbourhoods_to_add = []
            for neighbourhood in raw_neighbourhoods:
                if neighbourhood.wof_id in ids_to_add:
                    neighbourhoods_to_add.append(neighbourhood)
                elif neighbourhood.wof_id in ids_to_update:
                    neighbourhoods_to_update.append(neighbourhood)
                else:
                    assert 0, '%d should have been found to add or update' % (
                        neighbourhood.wof_id)

            if neighbourhoods_to_add:
                self.logger.info('Inserting neighbourhoods: %d' %
                                 len(neighbourhoods_to_add))
            if neighbourhoods_to_update:
                self.logger.info('Updating neighbourhoods: %d' %
                                 len(neighbourhoods_to_update))
            if ids_to_remove:
                self.logger.info('Removing neighbourhoods: %d' %
                                 len(ids_to_remove))

            def _sync_neighbourhoods():
                self.model.sync_neighbourhoods(
                    neighbourhoods_to_add, neighbourhoods_to_update,
                    ids_to_remove)
            sync_neighbourhoods_thread = threading.Thread(
                target=_sync_neighbourhoods)
            sync_neighbourhoods_thread.start()

        else:
            self.logger.info('No diffs found, no sync necessary')

        if diffs:
            self.logger.info('Generating tile expiry list ...')
            expired_coord_ints = generate_tile_expiry_list(
                self.zoom_expiry, diffs)
            self.logger.info(
                'Generating tile expiry list ... done - '
                'Found %d expired tiles' % len(expired_coord_ints))
        else:
            self.logger.info('No diffs found, not generating expired coords')
            expired_coord_ints = ()

        # ensure we're done fetching the tiles of interest by this point
        toi_thread.join()
        toi = toi_queue.get()

        self.logger.info('Have tiles of interest')

        if diffs:
            # intersect the tiles of interest with the expired coords from
            # the neighbourhood diff
            self.logger.info('Intersecting %d tiles of interest with %d '
                             'expired tiles' % (
                                 len(toi), len(expired_coord_ints)))
            toi_expired_coord_ints = self.intersector(
                expired_coord_ints, toi, self.zoom_until)
            coords = map(coord_unmarshall_int, toi_expired_coord_ints)
            self.logger.info('Intersection complete, will expire %d tiles' %
                             len(coords))
        else:
            self.logger.info('No diffs found, no need to intersect')
            coords = ()

        # we need to finish sync'ing neighbourhoods before we enqueue
        # coordinates
        if sync_neighbourhoods_thread is not None:
            sync_neighbourhoods_thread.join()
            self.logger.info("Sync'ing neighbourhoods ... done")

        if coords:
            self.logger.info('Asking enqueuer to enqueue %d coords ...' %
                             len(coords))
            self.coords_enqueuer(coords)
            self.logger.info('Asking enqueuer to enqueue %d coords ... done' %
                             len(coords))
        else:
            self.logger.info('No expired tiles to enqueue')


class WofInitialLoader(object):

    def __init__(self, fetcher, model, logger):
        self.fetcher = fetcher
        self.model = model
        self.logger = logger

    def __call__(self):
        self.logger.info('Fetching meta neighbourhoods csv ...')
        neighbourhood_metas = list(self.fetcher.fetch_meta_neighbourhoods())
        self.logger.info('Fetching meta neighbourhoods csv ... done')

        self.logger.info('Fetching raw neighbourhoods ...')
        neighbourhoods = self.fetcher.fetch_raw_neighbourhoods(
            neighbourhood_metas)
        self.logger.info('Fetching raw neighbourhoods ... done')

        self.logger.info('Inserting %d neighbourhoods ...' %
                         len(neighbourhoods))
        self.model.insert_neighbourhoods(neighbourhoods)
        self.logger.info('Inserting %d neighbourhoods ... done' %
                         len(neighbourhoods))


def make_wof_url_neighbourhood_fetcher(
        neighbourhood_url, data_prefix_url, n_threads):
    fetcher = WofUrlNeighbourhoodFetcher(
        neighbourhood_url, data_prefix_url, n_threads)
    return fetcher


def make_wof_filesystem_neighbourhood_fetcher(wof_data_path, n_threads):
    fetcher = WofFilesystemNeighbourhoodFetcher(
        wof_data_path, n_threads)
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


def make_wof_initial_loader(fetcher, model, logger):
    wof_loader = WofInitialLoader(fetcher, model, logger)
    return wof_loader
