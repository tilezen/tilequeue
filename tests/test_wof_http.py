import unittest
try:
    # Python 2.x
    import BaseHTTPServer as http
except ImportError:
    # Python 3.x
    from http import server as http
import contextlib
from httptestserver import Server
from tilequeue.wof import make_wof_url_neighbourhood_fetcher, \
    WofProcessor
import datetime


# a mock wof model which does nothing - these tests are about
# fetching the data, not parsing it.
class _NullWofModel(object):

    def __init__(self):
        self.added = 0
        self.updated = 0
        self.removed = 0

    def find_previous_neighbourhood_meta(self):
        return []

    def sync_neighbourhoods(
            self, neighbourhoods_to_add, neighbourhoods_to_update,
            ids_to_remove):
        self.added = self.added + len(neighbourhoods_to_add)
        self.updated = self.updated + len(neighbourhoods_to_update)
        self.removed = self.removed + len(ids_to_remove)

    def insert_neighbourhoods(self, neighbourhoods):
        pass

    def update_visible_timestamp(self, zoom, day):
        return set()


class _WofHandlerContext(object):

    def __init__(self, failure_count=0, content={}, failure_code=500):
        self.request_counts = {}
        self.failure_count = failure_count
        self.content = content
        self.failure_code = failure_code


class _WofErrorHandler(http.BaseHTTPRequestHandler):

    def __init__(self, context, *args):
        self.wof_ctx = context
        http.BaseHTTPRequestHandler.__init__(self, *args)

    def do_GET(self):
        request_count = self.wof_ctx.request_counts.get(self.path, 0)

        if request_count < self.wof_ctx.failure_count:
            self.wof_ctx.request_counts[self.path] = request_count + 1
            self.send_response(self.wof_ctx.failure_code)
            self.end_headers()
            self.wfile.write("")

        else:
            self.send_response(200)
            content_type, content \
                = self.wof_ctx.content.get(self.path, ('text/plain', ''))
            self.send_header('Content-Type', content_type)
            self.end_headers()
            self.wfile.write(content)


# fake Redis object to keep the code happy
class _NullRedisTOI(object):

    def fetch_tiles_of_interest(self):
        return []


# guard function to run a test HTTP server on another thread and reap it when
# it goes out of scope.
@contextlib.contextmanager
def _test_http_server(handler):
    server = Server('127.0.0.1', 0, 'http', handler)
    server.start()
    yield server


# simple logger that's easy to turn the output on and off.
class _SimpleLogger(object):

    def __init__(self, verbose=True):
        self.verbose = verbose

    def info(self, msg):
        if self.verbose:
            print("INFO: %s" % msg)

    def warn(self, msg):
        if self.verbose:
            print("WARN: %s" % msg)

    def error(self, msg):
        if self.verbose:
            print("ERROR: %s" % msg)


class TestWofHttp(unittest.TestCase):

    def _simple_test(self, num_failures=0, failure_code=500, max_retries=3):
        context = _WofHandlerContext(num_failures, {
            '/meta/neighbourhoods.csv': (
                'text/plain; charset=utf-8',
                "bbox,cessation,deprecated,file_hash,fullname,geom_hash,"
                "geom_latitude,geom_longitude,id,inception,iso,lastmodified,"
                "lbl_latitude,lbl_longitude,name,parent_id,path,placetype,"
                "source,superseded_by,supersedes\n"
                "\"0,0,0,0\",u,,00000000000000000000000000000000,,"
                "00000000000000000000000000000000,0,0,1,u,,0,0,0,Null Island,"
                "-1,1/1.geojson,neighbourhood,null,,\n"
            ),
            '/meta/microhoods.csv': (
                'text/plain; charset=utf-8',
                "bbox,cessation,deprecated,file_hash,fullname,geom_hash,"
                "geom_latitude,geom_longitude,id,inception,iso,lastmodified,"
                "lbl_latitude,lbl_longitude,name,parent_id,path,placetype,"
                "source,superseded_by,supersedes\n"
            ),
            '/meta/macrohoods.csv': (
                'text/plain; charset=utf-8',
                "bbox,cessation,deprecated,file_hash,fullname,geom_hash,"
                "geom_latitude,geom_longitude,id,inception,iso,lastmodified,"
                "lbl_latitude,lbl_longitude,name,parent_id,path,placetype,"
                "source,superseded_by,supersedes\n"
            ),
            '/data/1/1.geojson': (
                'application/json; charset=utf-8',
                '{"id":1,"type":"Feature","properties":{"wof:id":1,' +
                '"wof:name":"Null Island","lbl:latitude":0.0,' +
                '"lbl:longitude":0.0,"wof:placetype":"neighbourhood"},' +
                '"geometry":{"coordinates":[0,0],"type":"Point"}}'
            )
        }, failure_code)

        def handler(*args):
            return _WofErrorHandler(context, *args)

        model = _NullWofModel()

        with _test_http_server(handler) as server:
            fetcher = make_wof_url_neighbourhood_fetcher(
                server.url('/meta/neighbourhoods.csv'),
                server.url('/meta/microhoods.csv'),
                server.url('/meta/macrohoods.csv'),
                server.url('/meta/boroughs.csv'),
                server.url('/data'),
                1, max_retries)
            redis = _NullRedisTOI()

            def intersector(dummy1, dummy2, dummy3):
                return [], None

            def enqueuer(dummy):
                pass

            logger = _SimpleLogger(False)

            today = datetime.date.today()
            processor = WofProcessor(fetcher, model, redis, intersector,
                                     enqueuer, logger, today)
            processor()

        self.assertEqual(model.added, 1)
        self.assertEqual(model.updated, 0)
        self.assertEqual(model.removed, 0)

    # if there are no failures, then the process should complete correctly.
    def test_without_failures(self):
        self._simple_test(0, 502)

    # if there are fewer failures than the number of retries, then it should
    # process without an error.
    def test_with_single_failure(self):
        self._simple_test(1, 502)

    # however, if we try to fetch a URL and it's missing then that really
    # should be an error - probably indicates that we're not using the right
    # logic to form the URLs.
    def test_with_missing(self):
        with self.assertRaises(AssertionError):
            self._simple_test(1, 404)

    # if we try to fetch a URL and it's forbidden then that really should be an
    # error - probably indicates a configuration problem with WOF.
    def test_with_forbidden(self):
        with self.assertRaises(AssertionError):
            self._simple_test(1, 403)
