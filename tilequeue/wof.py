from collections import namedtuple
from contextlib import closing
from cStringIO import StringIO
from datetime import datetime
from operator import attrgetter
from psycopg2.extras import register_hstore
from shapely import geos
from tilequeue.tile import coord_marshall_int
from tilequeue.tile import coord_unmarshall_int
from tilequeue.tile import mercator_point_to_coord
from tilequeue.tile import reproject_lnglat_to_mercator
import csv
import edtf
import json
import os.path
import psycopg2
import Queue
import requests
import shapely.geometry
import shapely.ops
import shapely.wkb
import threading


DATABASE_SRID = 3857


def generate_csv_lines(requests_result):
    for line in requests_result.iter_lines():
        if line:
            yield line


neighbourhood_placetypes_to_int = dict(
    neighbourhood=1,
    microhood=2,
    macrohood=3,
    borough=4,
)
neighbourhood_int_to_placetypes = {
    1: 'neighbourhood',
    2: 'microhood',
    3: 'macrohood',
    4: 'borough',
}


NeighbourhoodMeta = namedtuple(
    'NeighbourhoodMeta',
    'wof_id placetype name hash label_position')
Neighbourhood = namedtuple(
    'Neighbourhood',
    'wof_id placetype name hash label_position geometry n_photos area '
    'min_zoom max_zoom is_landuse_aoi inception cessation l10n_names')


def parse_neighbourhood_meta_csv(csv_line_generator, placetype):
    reader = csv.reader(csv_line_generator)

    it = iter(reader)
    header = it.next()

    lbl_lat_idx = header.index('lbl_latitude')
    lbl_lng_idx = header.index('lbl_longitude')
    name_idx = header.index('name')
    wof_id_idx = header.index('id')
    hash_idx = header.index('file_hash')
    superseded_by_idx = header.index('superseded_by')

    min_row_length = (max(
        lbl_lat_idx, lbl_lng_idx, name_idx, wof_id_idx, hash_idx,
        superseded_by_idx) + 1)

    for row in it:
        if len(row) < min_row_length:
            continue

        superseded_by = row[superseded_by_idx]
        if superseded_by:
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

        label_x, label_y = reproject_lnglat_to_mercator(lng, lat)
        label_position = shapely.geometry.Point(label_x, label_y)

        neighbourhood_meta = NeighbourhoodMeta(
            wof_id, placetype, name, file_hash, label_position)
        yield neighbourhood_meta


def _make_requests_session_with_retries(max_retries):
    from requests.adapters import HTTPAdapter
    from requests.packages.urllib3.util import Retry

    s = requests.Session()
    a = HTTPAdapter(
        max_retries=Retry(
            total=max_retries,
            status_forcelist=[  # this is a list of statuses to consider to be
                                # an error and retry.
                429,  # Too many requests (i.e: back off)
                500,  # Generic internal server error
                502,  # Bad Gateway - i.e: upstream failure
                503,  # Unavailable, temporarily
                504,  # Gateway timeout
                522   # Origin connection timed out
            ],
            backoff_factor=1.0  # back off for 0s, 1s, 3s, 7s, etc... after
                                # each successive failure. (factor*(2^N-1))
        ))

    # use retry for both HTTP and HTTPS connections.
    s.mount('http://', a)
    s.mount('https://', a)

    return s


def fetch_wof_url_meta_neighbourhoods(url, placetype, max_retries):
    s = _make_requests_session_with_retries(max_retries)
    r = s.get(url, stream=True)
    assert r.status_code == 200, 'Failure requesting: %s' % url

    csv_line_generator = generate_csv_lines(r)
    return parse_neighbourhood_meta_csv(csv_line_generator, placetype)


class NeighbourhoodFailure(object):

    def __init__(self, wof_id, reason, message, halt=False, skipped=False,
                 funky=False, superseded=False):
        # halt is a signal that threads should stop fetching. This
        # would happen during a network IO error or when we get an
        # unexpected http response when fetching raw json files. In
        # some scenarios this could be recoverable, but because that
        # isn't always the case we assume that we should stop further
        # requests for more raw json files, and just process what we
        # have so far.

        # skipped means that we won't log this failure, ie there was
        # an earlier "halt" error and processing of further records
        # has stopped.

        # funky is a signal downstream that this is a "soft" or
        # expected failure, in the sense that it only means that we
        # should skip the record, but we didn't actually detect any
        # errors with the processing

        # superseded is set when the json has a value for
        # wof:superseded. This would indicate a data inconsistency
        # because the meta csv file didn't have it set if we're trying
        # to fetch the raw json in the first place. But this is meant
        # to catch this scenario.

        self.wof_id = wof_id
        self.reason = reason
        self.message = message
        self.halt = halt
        self.skipped = skipped
        self.funky = funky
        self.superseded = superseded


# given a string, parse it as EDTF while allowing a single 'u' or None to mean
# completely unknown, and return the EDTF object.
def _normalize_edtf(s):
    if s and s != 'u':
        try:
            return edtf.EDTF(s)
        except:
            pass

    # when all else fails, return the "most unknown" EDTF.
    return edtf.EDTF('uuuu')


def create_neighbourhood_from_json(json_data, neighbourhood_meta):

    def failure(reason):
        return NeighbourhoodFailure(
            neighbourhood_meta.wof_id, reason, json.dumps(json_data))

    if not isinstance(json_data, dict):
        return failure('Unexpected json')

    props = json_data.get('properties')
    if props is None or not isinstance(props, dict):
        return failure('Missing properties')

    superseded_by = props.get('wof:superseded_by')
    # these often show up as empty lists, so we do a truthy test
    # instead of expicitly checking for None
    if superseded_by:
        return NeighbourhoodFailure(
            neighbourhood_meta.wof_id,
            'superseded_by: %s' % superseded_by,
            json.dumps(json_data), superseded=True)

    geometry = json_data.get('geometry')
    if geometry is None:
        return failure('Missing geometry')

    try:
        shape_lnglat = shapely.geometry.shape(geometry)
    except:
        return failure('Unexpected geometry')

    shape_mercator = shapely.ops.transform(
        reproject_lnglat_to_mercator, shape_lnglat)

    # ignore any features that are marked as funky
    is_funky = props.get('mz:is_funky')
    if is_funky is not None:
        try:
            is_funky = int(is_funky)
        except ValueError:
            return failure('Unexpected mz:is_funky value %s' % is_funky)
        if is_funky != 0:
            return NeighbourhoodFailure(
                neighbourhood_meta.wof_id,
                'mz:is_funky value is not 0: %s' % is_funky,
                json.dumps(json_data), funky=True)

    wof_id = props.get('wof:id')
    if wof_id is None:
        return failure('Missing wof:id')
    try:
        wof_id = int(wof_id)
    except ValueError:
        return failure('wof_id is not an int: %s' % wof_id)

    name = props.get('wof:name')
    if name is None:
        return failure('Missing name')

    n_photos = props.get('misc:photo_sum')
    if n_photos is not None:
        try:
            n_photos = int(n_photos)
        except ValueError:
            return failure('misc:photo_sum is not an int: %s' % n_photos)

    label_lat = props.get('lbl:latitude')
    label_lng = props.get('lbl:longitude')
    if label_lat is None or label_lng is None:
        # first, try to fall back to geom:* when lbl:* is missing. we'd prefer
        # to have lbl:*, but it's better to have _something_ than nothing.
        label_lat = props.get('geom:latitude')
        label_lng = props.get('geom:longitude')

        if label_lat is None or label_lng is None:
            return failure('Missing lbl:latitude or lbl:longitude and ' +
                           'geom:latitude or geom:longitude')

    try:
        label_lat = float(label_lat)
        label_lng = float(label_lng)
    except ValueError:
        return failure('lbl:latitude or lbl:longitude not float')

    label_merc_x, label_merc_y = reproject_lnglat_to_mercator(
        label_lng, label_lat)
    label_position = shapely.geometry.Point(label_merc_x, label_merc_y)

    placetype = props.get('wof:placetype')
    if placetype is None:
        return failure('Missing wof:placetype')

    default_min_zoom = 15
    default_max_zoom = 16

    min_zoom = props.get('mz:min_zoom')
    if min_zoom is None:
        min_zoom = default_min_zoom
    else:
        try:
            min_zoom = float(min_zoom)
        except ValueError:
            return failure('mz:min_zoom not float: %s' % min_zoom)
    max_zoom = props.get('mz:max_zoom')
    if max_zoom is None:
        max_zoom = default_max_zoom
    else:
        try:
            max_zoom = float(max_zoom)
        except ValueError:
            return failure('mz:max_zoom not float: %s' % max_zoom)

    is_landuse_aoi = props.get('mz:is_landuse_aoi')
    if is_landuse_aoi is not None:
        try:
            is_landuse_aoi = int(is_landuse_aoi)
        except ValueError:
            return failure('is_landuse_aoi not int: %s' % is_landuse_aoi)
        is_landuse_aoi = is_landuse_aoi != 0

    if shape_mercator.type in ('Polygon', 'MultiPolygon'):
        area = int(shape_mercator.area)
    else:
        area = None

    # for the purposes of display, we only care about the times when something
    # should first start to be shown, and the time when it should stop
    # showing.
    edtf_inception = _normalize_edtf(props.get('edtf:inception'))
    edtf_cessation = _normalize_edtf(props.get('edtf:cessation'))
    edtf_deprecated = _normalize_edtf(props.get('edtf:deprecated'))

    # check that the dates are valid first to return back a better error
    inception_earliest = edtf_inception.date_earliest()
    cessation_latest = edtf_cessation.date_latest()
    deprecated_latest = edtf_deprecated.date_latest()
    if inception_earliest is None:
        return failure('invalid edtf:inception: %s' %
                       props.get('edtf:inception'))
    if cessation_latest is None:
        return failure('invalid edtf:cessation: %s' %
                       props.get('edtf:cessation'))
    if deprecated_latest is None:
        return failure('invalid edtf:deprecated: %s' %
                       props.get('edtf:deprecated'))

    # the 'edtf:inception' property gives us approximately the former and we
    # take the earliest date it could mean. the 'edtf:cessation' and
    # 'edtf:deprecated' would both stop the item showing, so we take the
    # earliest of each's latest possible date.
    inception = inception_earliest
    cessation = min(cessation_latest, deprecated_latest)

    # grab any names in other languages
    lang_suffix_size = len('_preferred')
    l10n_names = {}
    for k, v in props.iteritems():
        if not v:
            continue
        if not k.startswith('name:') or not k.endswith('_preferred'):
            continue
        if isinstance(v, list):
            v = v[0]
        lang = k[:-lang_suffix_size]
        l10n_names[lang] = v
    if not l10n_names:
        l10n_names = None

    neighbourhood = Neighbourhood(
        wof_id, placetype, name, neighbourhood_meta.hash, label_position,
        shape_mercator, n_photos, area, min_zoom, max_zoom, is_landuse_aoi,
        inception, cessation, l10n_names)
    return neighbourhood


def fetch_url_raw_neighbourhood(url, neighbourhood_meta, max_retries):
    try:
        s = _make_requests_session_with_retries(max_retries)
        r = s.get(url)
    except Exception, e:
        # if there is an IO error when fetching the url itself, we'll
        # want to halt too
        return NeighbourhoodFailure(
            neighbourhood_meta.wof_id, 'IO Error fetching %s' % url, str(e),
            halt=True)
    if r.status_code != 200:
        # once we don't get a 200, signal that we should stop all
        # remaining processing
        return NeighbourhoodFailure(
            neighbourhood_meta.wof_id,
            'Invalid response %d for %s' % (r.status_code, url), r.text,
            halt=True)

    try:
        doc = r.json()
    except Exception, e:
        return NeighbourhoodFailure(
            neighbourhood_meta.wof_id, 'Response is not json for %s' % url,
            r.text)
    try:
        neighbourhood = create_neighbourhood_from_json(doc, neighbourhood_meta)
    except Exception, e:
        return NeighbourhoodFailure(
            neighbourhood_meta.wof_id,
            'Unexpected exception parsing json',
            json.dumps(doc))

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


def make_fetch_raw_url_fn(data_url_prefix, max_retries):
    def fn(neighbourhood_meta):
        wof_url = generate_wof_url(
            data_url_prefix, neighbourhood_meta.wof_id)
        neighbourhood = fetch_url_raw_neighbourhood(wof_url,
                                                    neighbourhood_meta,
                                                    max_retries)
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

    stop = threading.Event()

    def _fetch_raw_neighbourhood():
        while True:
            neighbourhood_meta = neighbourhood_input_queue.get()
            if neighbourhood_meta is None:
                break
            if stop.is_set():
                # assume all remaining neighbourhoods are failures
                # these will get skipped
                neighbourhood_output_queue.put(NeighbourhoodFailure(
                    neighbourhood_meta.wof_id,
                    'Skipping remaining neighbourhoods',
                    'Skipping remaining neighbourhoods',
                    skipped=True))
                continue

            neighbourhood = fetch_raw_fn(neighbourhood_meta)
            if isinstance(neighbourhood, NeighbourhoodFailure):
                failure = neighbourhood
                # if this is the type of error that should stop all
                # processing, notify all other threads
                if failure.halt:
                    stop.set()
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
    failures = []
    for i in xrange(len(neighbourhood_metas)):
        neighbourhood = neighbourhood_output_queue.get()
        if isinstance(neighbourhood, NeighbourhoodFailure):
            failures.append(neighbourhood)
        else:
            neighbourhoods.append(neighbourhood)

    for fetch_thread in fetch_threads:
        fetch_thread.join()

    return neighbourhoods, failures


class WofUrlNeighbourhoodFetcher(object):

    def __init__(self, neighbourhood_url, microhood_url, macrohood_url,
                 borough_url, data_url_prefix, n_threads, max_retries):
        self.neighbourhood_url = neighbourhood_url
        self.microhood_url = microhood_url
        self.macrohood_url = macrohood_url
        self.borough_url = borough_url
        self.data_url_prefix = data_url_prefix
        self.n_threads = n_threads
        self.max_retries = max_retries

    def fetch_meta_neighbourhoods(self):
        return fetch_wof_url_meta_neighbourhoods(
            self.neighbourhood_url, 'neighbourhood', self.max_retries)

    def fetch_meta_microhoods(self):
        return fetch_wof_url_meta_neighbourhoods(
            self.microhood_url, 'microhood', self.max_retries)

    def fetch_meta_macrohoods(self):
        return fetch_wof_url_meta_neighbourhoods(
            self.macrohood_url, 'macrohood', self.max_retries)

    def fetch_meta_boroughs(self):
        return fetch_wof_url_meta_neighbourhoods(
            self.borough_url, 'borough', self.max_retries)

    def fetch_raw_neighbourhoods(self, neighbourhood_metas):
        url_fetch_fn = make_fetch_raw_url_fn(self.data_url_prefix,
                                             self.max_retries)
        neighbourhoods, failures = threaded_fetch(
            neighbourhood_metas, self.n_threads, url_fetch_fn)
        return neighbourhoods, failures


class WofFilesystemNeighbourhoodFetcher(object):

    def __init__(self, wof_data_path, n_threads):
        self.wof_data_path = wof_data_path
        self.n_threads = n_threads

    def _fetch_meta_neighbourhoods(self, placetype):
        meta_fs_path = os.path.join(
            self.wof_data_path, 'meta', 'wof-%s-latest.csv' % placetype)
        with open(meta_fs_path) as fp:
            meta_neighbourhoods = list(
                parse_neighbourhood_meta_csv(fp, placetype))
        return meta_neighbourhoods

    def fetch_meta_neighbourhoods(self):
        return self._fetch_meta_neighbourhoods('neighbourhood')

    def fetch_meta_microhoods(self):
        return self._fetch_meta_neighbourhoods('microhood')

    def fetch_meta_macrohoods(self):
        return self._fetch_meta_neighbourhoods('macrohood')

    def fetch_meta_boroughs(self):
        return self._fetch_meta_neighbourhoods('borough')

    def fetch_raw_neighbourhoods(self, neighbourhood_metas):
        data_prefix = os.path.join(
            self.wof_data_path, 'data')
        fs_fetch_fn = make_fetch_raw_filesystem_fn(data_prefix)
        neighbourhoods, failures = threaded_fetch(
            neighbourhood_metas, self.n_threads, fs_fetch_fn)
        return neighbourhoods, failures


def create_neighbourhood_file_object(neighbourhoods, curdate=None):
    if curdate is None:
        curdate = datetime.now().date()

    # tell shapely to include the srid when generating WKBs
    geos.WKBWriter.defaults['include_srid'] = True

    buf = StringIO()

    def escape_string(s):
        return s.encode('utf-8').replace('\t', ' ').replace('\n', ' ')

    def escape_hstore_string(s):
        s = escape_string(s)
        if ' ' in s:
            s = s.replace('"', '\\\\"')
            s = '"%s"' % s
        return s

    def write_nullable_int(buf, x):
        if x is None:
            buf.write('\\N\t')
        else:
            buf.write('%d\t' % x)

    for n in neighbourhoods:
        buf.write('%d\t' % n.wof_id)
        buf.write('%d\t' % neighbourhood_placetypes_to_int[n.placetype])
        buf.write('%s\t' % escape_string(n.name))
        buf.write('%s\t' % escape_string(n.hash))

        write_nullable_int(buf, n.n_photos)
        write_nullable_int(buf, n.area)

        buf.write('%d\t' % n.min_zoom)
        buf.write('%d\t' % n.max_zoom)

        if n.is_landuse_aoi is None:
            buf.write('\\N\t')
        else:
            buf.write('%s\t' % ('true' if n.is_landuse_aoi else 'false'))

        geos.lgeos.GEOSSetSRID(n.label_position._geom, DATABASE_SRID)
        buf.write(n.label_position.wkb_hex)
        buf.write('\t')

        geos.lgeos.GEOSSetSRID(n.geometry._geom, DATABASE_SRID)
        buf.write(n.geometry.wkb_hex)
        buf.write('\t')

        buf.write('%s\t' % n.inception.isoformat())
        buf.write('%s\t' % n.cessation.isoformat())

        is_visible = n.inception < curdate and n.cessation >= curdate
        is_visible_str = 't' if is_visible else 'f'
        buf.write('%s\t' % is_visible_str)

        if n.l10n_names:
            hstore_items = []
            for k, v in n.l10n_names.items():
                k = escape_hstore_string(k)
                v = escape_hstore_string(v)
                hstore_items.append("%s=>%s" % (k, v))
            hstore_items_str = ','.join(hstore_items)
            buf.write('%s' % hstore_items_str)
        else:
            buf.write('\\N')

        buf.write('\n')

    buf.seek(0)

    return buf


class WofModel(object):

    def __init__(self, postgresql_conn_info):
        self.postgresql_conn_info = postgresql_conn_info
        self.table = 'wof_neighbourhood'

    def _create_conn(self):
        conn = psycopg2.connect(**self.postgresql_conn_info)
        register_hstore(conn)
        conn.set_session(autocommit=False)
        return conn

    def find_previous_neighbourhood_meta(self):
        with closing(self._create_conn()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'SELECT wof_id, placetype, name, hash, '
                    'ST_AsBinary(label_position) '
                    'FROM %s ORDER BY wof_id ASC' % self.table)

                ns = []
                for row in cursor:
                    wof_id, placetype_int, name, hash, label_bytes = row
                    wof_id = int(wof_id)
                    label_bytes = bytes(label_bytes)
                    label_position = shapely.wkb.loads(label_bytes)
                    placetype = neighbourhood_int_to_placetypes[placetype_int]
                    n = NeighbourhoodMeta(
                        wof_id, placetype, name, hash, label_position)
                    ns.append(n)
                return ns

    def sync_neighbourhoods(
            self, neighbourhoods_to_add, neighbourhoods_to_update,
            ids_to_remove):

        geos.WKBWriter.defaults['include_srid'] = True

        def gen_data(n):
            geos.lgeos.GEOSSetSRID(n.label_position._geom, DATABASE_SRID)
            geos.lgeos.GEOSSetSRID(n.geometry._geom, DATABASE_SRID)

            return dict(
                table=self.table,
                placetype=neighbourhood_placetypes_to_int[n.placetype],
                name=n.name,
                hash=n.hash,
                n_photos=n.n_photos,
                area=n.area,
                min_zoom=n.min_zoom,
                max_zoom=n.max_zoom,
                is_landuse_aoi=n.is_landuse_aoi,
                inception=n.inception,
                cessation=n.cessation,
                label_position=n.label_position.wkb_hex,
                geometry=n.geometry.wkb_hex,
                wof_id=n.wof_id,
                l10n_name=n.l10n_names,
            )

        if ids_to_remove:
            ids_to_remove_str = ', '.join(map(str, ids_to_remove))
        if neighbourhoods_to_update:
            update_data = map(gen_data, neighbourhoods_to_update)
        if neighbourhoods_to_add:
            insert_data = map(gen_data, neighbourhoods_to_add)

        # this closes the connection
        with closing(self._create_conn()) as conn:
            # this commits the transaction
            with conn as conn:
                # this frees any resources associated with the cursor
                with conn.cursor() as cursor:

                    if ids_to_remove:
                        cursor.execute(
                            'DELETE FROM %s WHERE wof_id IN (%s)' %
                            (self.table, ids_to_remove_str))

                    if neighbourhoods_to_update:
                        cursor.executemany(
                            'UPDATE ' + self.table + ' SET '
                            'placetype=%(placetype)s, '
                            'name=%(name)s, '
                            'hash=%(hash)s, '
                            'n_photos=%(n_photos)s, '
                            'area=%(area)s, '
                            'min_zoom=%(min_zoom)s, '
                            'max_zoom=%(max_zoom)s, '
                            'is_landuse_aoi=%(is_landuse_aoi)s, '
                            'inception=%(inception)s, '
                            'cessation=%(cessation)s, '
                            'label_position=%(label_position)s, '
                            'l10n_name=%(l10n_name)s, '
                            'geometry=%(geometry)s '
                            'WHERE wof_id=%(wof_id)s',
                            update_data)

                    if neighbourhoods_to_add:
                        cursor.executemany(
                            'INSERT INTO ' + self.table + ' '
                            '(wof_id, placetype, name, hash, n_photos, area, '
                            'min_zoom, max_zoom, is_landuse_aoi, '
                            'inception, cessation, '
                            'label_position, geometry, l10n_name) '
                            'VALUES (%(wof_id)s, %(placetype)s, %(name)s, '
                            '%(hash)s, %(n_photos)s, %(area)s, %(min_zoom)s, '
                            '%(max_zoom)s, %(is_landuse_aoi)s, '
                            '%(inception)s, %(cessation)s, '
                            '%(label_position)s, %(geometry)s, %(l10n_name)s)',
                            insert_data)

    def insert_neighbourhoods(self, neighbourhoods):
        # create this whole input file like object outside of the transaction
        nf = create_neighbourhood_file_object(neighbourhoods)
        # close the connection
        with closing(self._create_conn()) as conn:
            # commit the transaction
            with conn as conn:
                with conn.cursor() as cursor:
                    cursor.copy_from(nf, self.table)

    # update the whole table so that the `is_visible` flag is accurate for the
    # `current_date`. this returns a list of coords at `zoom` which have
    # changed visibility from true to false or vice-versa.
    def update_visible_timestamp(self, zoom, current_date):
        coords = set()

        def coord_int(row):
            x, y = row
            return coord_int_at_mercator_point(zoom, x, y)

        # close the connection
        with closing(self._create_conn()) as conn:
            # commit the transaction
            with conn as conn:
                with conn.cursor() as cursor:
                    # select the x, y position of the label for each WOF
                    # neighbourhood that changed visibility when the date
                    # was updated to `current_date`.
                    cursor.execute(
                        'SELECT st_x(n.label_position) as x, '
                        '       st_y(n.label_position) as y '
                        'FROM ('
                        '  SELECT wof_update_visible_ids(%s::date) AS id '
                        ') u '
                        'JOIN wof_neighbourhood n '
                        'ON n.wof_id = u.id',
                        (current_date.isoformat(),))
                    for result in cursor:
                        coords.add(coord_int(result))

        return coords


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


def log_failure(logger, failure):
    if not (failure.skipped or failure.funky or failure.superseded):
        failure_message_one_line = failure.message.replace('\n', ' | ')
        logger.error('Neighbourhood failure for %d: %r - %r' % (
            failure.wof_id, failure.reason, failure_message_one_line))


class WofProcessor(object):

    def __init__(self, fetcher, model, redis_cache_index, intersector,
                 coords_enqueuer, logger, current_date):
        self.fetcher = fetcher
        self.model = model
        self.redis_cache_index = redis_cache_index
        self.intersector = intersector
        self.coords_enqueuer = coords_enqueuer
        self.logger = logger
        self.zoom_expiry = 16
        self.zoom_until = 11
        self.current_date = current_date

    def __call__(self):
        # perform IO to get old/new neighbourhoods and tiles of
        # interest in parallel

        # queues to pass the results through the threads
        prev_neighbourhoods_queue = Queue.Queue(1)
        meta_neighbourhoods_queue = Queue.Queue(1)
        meta_microhoods_queue = Queue.Queue(1)
        meta_macrohoods_queue = Queue.Queue(1)
        meta_boroughs_queue = Queue.Queue(1)
        toi_queue = Queue.Queue(1)

        # functions for the threads
        def find_prev_neighbourhoods():
            prev_neighbourhoods = (
                self.model.find_previous_neighbourhood_meta())
            prev_neighbourhoods_queue.put(prev_neighbourhoods)

        def make_fetch_meta_csv_fn(fn, queue):
            neighbourhood_metas = list(fn())
            queue.put(neighbourhood_metas)

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
            target=make_fetch_meta_csv_fn(
                self.fetcher.fetch_meta_neighbourhoods,
                meta_neighbourhoods_queue))
        meta_neighbourhoods_thread.start()

        meta_microhoods_thread = threading.Thread(
            target=make_fetch_meta_csv_fn(
                self.fetcher.fetch_meta_microhoods,
                meta_microhoods_queue))
        meta_microhoods_thread.start()

        meta_macrohoods_thread = threading.Thread(
            target=make_fetch_meta_csv_fn(
                self.fetcher.fetch_meta_macrohoods,
                meta_macrohoods_queue))
        meta_macrohoods_thread.start()

        meta_boroughs_thread = threading.Thread(
            target=make_fetch_meta_csv_fn(
                self.fetcher.fetch_meta_boroughs,
                meta_boroughs_queue))
        meta_boroughs_thread.start()

        toi_thread = threading.Thread(target=fetch_toi)
        toi_thread.start()

        # ensure we're done with finding the next and previous
        # neighbourhoods by this point
        prev_neighbourhoods_thread.join()
        meta_neighbourhoods_thread.join()
        meta_microhoods_thread.join()
        meta_macrohoods_thread.join()
        meta_boroughs_thread.join()

        self.logger.info('Fetching old and new neighbourhoods ... done')

        prev_neighbourhoods = prev_neighbourhoods_queue.get()
        meta_neighbourhoods = meta_neighbourhoods_queue.get()
        meta_microhoods = meta_microhoods_queue.get()
        meta_macrohoods = meta_macrohoods_queue.get()
        meta_boroughs = meta_boroughs_queue.get()

        # each of these has the appropriate placetype set now
        meta_neighbourhoods = (
            meta_neighbourhoods + meta_microhoods + meta_macrohoods +
            meta_boroughs)

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
            raw_neighbourhoods, failures = (
                self.fetcher.fetch_raw_neighbourhoods(
                    wof_neighbourhoods_to_fetch))
            self.logger.info('Fetching %d raw neighbourhoods ... done' %
                             len(wof_neighbourhoods_to_fetch))
        else:
            self.logger.info('No raw neighbourhoods found to fetch')
            raw_neighbourhoods = ()
            failures = []

        # we should just remove any neighbourhoods from add/update lists
        # also keep track of these ids to remove from the diffs too
        failed_wof_ids = set()
        superseded_by_wof_ids = set()
        funky_wof_ids = set()
        for failure in failures:
            failure_wof_id = failure.wof_id
            log_failure(self.logger, failure)

            if failure.funky:
                # this scenario is triggered for new neighbourhoods,
                # or if a neighbourhood became funky
                # we handle both of these scenarios in tests later on,
                # but for now we just track the id of the funky
                # neighbourhoods
                funky_wof_ids.add(failure_wof_id)

            if failure.superseded:
                self.logger.warn(
                    'superseded_by inconsistency for %s' % failure_wof_id)
                # this means that we had a value for superseded_by in
                # the raw json, but not in the meta file
                # this should get treated as a removal
                superseded_by_wof_ids.add(failure_wof_id)

            failed_wof_ids.add(failure_wof_id)
            ids_to_add.discard(failure_wof_id)
            ids_to_update.discard(failure_wof_id)

        # we'll only log the number of funky records that we found
        if funky_wof_ids:
            self.logger.warn('Number of funky neighbourhoods: %d' %
                             len(funky_wof_ids))

        # now we'll want to ensure that the failed ids are not present
        # in any additions or updates
        new_diffs = []
        for n1, n2 in diffs:
            if n2 is None or n2.wof_id not in failed_wof_ids:
                new_diffs.append((n1, n2))
        diffs = new_diffs

        # and we'll want to also treat any superseded_by
        # inconsistencies as removals
        # but we need the original neighbourhood meta object to
        # generate the diff, for its label position to expire the
        # appropriate tile
        if superseded_by_wof_ids:
            for n in prev_neighbourhoods:
                if n.wof_id in superseded_by_wof_ids:
                    ids_to_remove.add(n.wof_id)
                    diffs.append((n, None))

        # if the neighbourhood became funky and we had it in our
        # existing set, we'll want to remove it
        if funky_wof_ids:
            for n in prev_neighbourhoods:
                if n.wof_id in funky_wof_ids:
                    ids_to_remove.add(n.wof_id)
                    diffs.append((n, None))

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
            expired_coord_ints = set()

        # ensure we're done fetching the tiles of interest by this point
        toi_thread.join()
        toi = toi_queue.get()

        self.logger.info('Have tiles of interest')

        # we need to finish sync'ing neighbourhoods before we flip the
        # visibility flag and enqueue coordinates
        if sync_neighbourhoods_thread is not None:
            sync_neighbourhoods_thread.join()
            self.logger.info("Sync'ing neighbourhoods ... done")

        # update the current timestamp, returning the list of coords that
        # have changed visibility.
        visibility_updates = \
            self.model.update_visible_timestamp(
                self.zoom_expiry, self.current_date)
        self.logger.info('Have %d tile expiries from visibility changes.'
                         % len(visibility_updates))
        expired_coord_ints.update(visibility_updates)

        if diffs:
            # intersect the tiles of interest with the expired coords from
            # the neighbourhood diff
            self.logger.info('Intersecting %d tiles of interest with %d '
                             'expired tiles' % (
                                 len(toi), len(expired_coord_ints)))
            toi_expired_coord_ints, _ = self.intersector(
                expired_coord_ints, toi, self.zoom_until)
            coords = map(coord_unmarshall_int, toi_expired_coord_ints)
            self.logger.info('Intersection complete, will expire %d tiles' %
                             len(coords))
        else:
            self.logger.info('No diffs found, no need to intersect')
            coords = ()

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

        self.logger.info('Fetching meta microhoods csv ...')
        microhood_metas = list(self.fetcher.fetch_meta_microhoods())
        self.logger.info('Fetching meta microhoods csv ... done')

        self.logger.info('Fetching meta macrohoods csv ...')
        macrohood_metas = list(self.fetcher.fetch_meta_macrohoods())
        self.logger.info('Fetching meta macrohoods csv ... done')

        self.logger.info('Fetching meta boroughs csv ...')
        borough_metas = list(self.fetcher.fetch_meta_boroughs())
        self.logger.info('Fetching meta boroughs csv ... done')

        neighbourhood_metas = (
            neighbourhood_metas + microhood_metas + macrohood_metas +
            borough_metas)

        self.logger.info('Fetching raw neighbourhoods ...')
        neighbourhoods, failures = self.fetcher.fetch_raw_neighbourhoods(
            neighbourhood_metas)
        for failure in failures:
            log_failure(self.logger, failure)
        self.logger.info('Fetching raw neighbourhoods ... done')

        self.logger.info('Inserting %d neighbourhoods ...' %
                         len(neighbourhoods))
        self.model.insert_neighbourhoods(neighbourhoods)
        self.logger.info('Inserting %d neighbourhoods ... done' %
                         len(neighbourhoods))


def make_wof_url_neighbourhood_fetcher(
        neighbourhood_url, microhood_url, macrohood_url, borough_url,
        data_prefix_url, n_threads, max_retries):
    fetcher = WofUrlNeighbourhoodFetcher(
        neighbourhood_url, microhood_url, macrohood_url, borough_url,
        data_prefix_url, n_threads, max_retries)
    return fetcher


def make_wof_filesystem_neighbourhood_fetcher(wof_data_path, n_threads):
    fetcher = WofFilesystemNeighbourhoodFetcher(
        wof_data_path, n_threads)
    return fetcher


def make_wof_model(postgresql_conn_info):
    wof_model = WofModel(postgresql_conn_info)
    return wof_model


def make_wof_processor(
        fetcher, model, redis_cache_index, sqs_queue, n_threads, logger,
        current_date):
    from tilequeue.command import explode_and_intersect
    from tilequeue.command import ThreadedEnqueuer
    threaded_enqueuer = ThreadedEnqueuer(sqs_queue, n_threads, logger)
    wof_processor = WofProcessor(
        fetcher, model, redis_cache_index, explode_and_intersect,
        threaded_enqueuer, logger, current_date)
    return wof_processor


def make_wof_initial_loader(fetcher, model, logger):
    wof_loader = WofInitialLoader(fetcher, model, logger)
    return wof_loader
