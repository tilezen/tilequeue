from botocore.exceptions import ClientError
from collections import defaultdict
from cStringIO import StringIO
from ModestMaps.Core import Coordinate
from msgpack import Unpacker
from raw_tiles.tile import Tile
from tilequeue.command import explode_and_intersect
from tilequeue.queue.message import MessageHandle
from tilequeue.store import calc_hash
from tilequeue.tile import coord_marshall_int
from tilequeue.tile import coord_unmarshall_int
from tilequeue.toi import load_set_from_gzipped_fp
from tilequeue.utils import format_stacktrace_one_line
from tilequeue.utils import grouper
from tilequeue.utils import time_block
from time import gmtime
import zipfile


class SqsQueue(object):

    def __init__(self, sqs_client, queue_url, recv_wait_time_seconds):
        self.sqs_client = sqs_client
        self.queue_url = queue_url
        self.recv_wait_time_seconds = recv_wait_time_seconds

    def send(self, payloads):
        """
        enqueue a sequence of payloads to the sqs queue

        Each payload is already expected to be pre-formatted for the queue. At
        this time, it should be a comma separated list of coordinates strings
        that are grouped by their parent zoom.
        """
        msgs = []
        for i, payload in enumerate(payloads):
            msg_id = str(i)
            msg = dict(
                Id=msg_id,
                MessageBody=payload,
            )
            msgs.append(msg)
        resp = self.sqs_client.send_message_batch(
            QueueUrl=self.queue_url,
            Entries=msgs,
        )
        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception('Invalid status code from sqs: %s' %
                            resp['ResponseMetadata']['HTTPStatusCode'])
        failed_messages = resp.get('Failed')
        if failed_messages:
            # TODO maybe retry failed messages if not sender's fault? up to a
            # certain maximum number of attempts?
            # http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Client.send_message_batch # noqa
            raise Exception('Messages failed to send to sqs: %s' %
                            len(failed_messages))

    def read(self):
        """read a single message from the queue"""
        resp = self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1,
            AttributeNames=('SentTimestamp',),
            WaitTimeSeconds=self.recv_wait_time_seconds,
        )
        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception('Invalid status code from sqs: %s' %
                            resp['ResponseMetadata']['HTTPStatusCode'])
        msgs = resp.get('Messages')
        if not msgs:
            return None
        assert len(msgs) == 1
        msg = msgs[0]
        payload = msg['Body']
        handle = msg['ReceiptHandle']
        timestamp = msg['Attributes']['SentTimestamp']
        metadata = dict(timestamp=timestamp)
        msg_handle = MessageHandle(handle, payload, metadata)
        return msg_handle

    def done(self, msg_handle):
        """acknowledge completion of message"""
        self.sqs_client.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=msg_handle.handle,
        )


class RawrEnqueuer(object):
    """enqueue coords from expiry grouped by parent zoom"""

    def __init__(self, rawr_queue, msg_marshaller, group_by_zoom, logger,
                 stats_handler):
        self.rawr_queue = rawr_queue
        self.msg_marshaller = msg_marshaller
        self.group_by_zoom = group_by_zoom
        self.logger = logger
        self.stats_handler = stats_handler

    def __call__(self, coords):
        grouped_by_zoom = defaultdict(list)
        for coord in coords:
            assert self.group_by_zoom <= coord.zoom
            parent = coord.zoomTo(self.group_by_zoom).container()
            parent_coord_int = coord_marshall_int(parent)
            grouped_by_zoom[parent_coord_int].append(coord)

        n_coords = 0
        payloads = []
        for _, coords in grouped_by_zoom.iteritems():
            payload = self.msg_marshaller.marshall(coords)
            payloads.append(payload)
            n_coords += len(coords)
        n_payloads = len(payloads)

        rawr_queue_batch_size = 10
        n_msgs_sent = 0
        for payloads_chunk in grouper(payloads, rawr_queue_batch_size):
            self.rawr_queue.send(payloads_chunk)
            n_msgs_sent += 1

        if self.logger:
            self.logger.info(
                'Rawr tiles enqueued: '
                'coords(%d) payloads(%d) enqueue_calls(%d))' %
                (n_coords, n_payloads, n_msgs_sent))

        self.stats_handler(n_coords, n_payloads, n_msgs_sent)


def common_parent(coords, parent_zoom):
    """
    Return the common parent for coords

    Also check that all coords do indeed share the same parent coordinate.
    """
    parent = None
    for coord in coords:
        assert parent_zoom <= coord.zoom
        coord_parent = coord.zoomTo(parent_zoom).container()
        if parent is None:
            parent = coord_parent
        else:
            assert parent == coord_parent
    assert parent is not None, 'No coords?'
    return parent


def convert_coord_object(coord):
    """Convert ModestMaps.Core.Coordinate -> raw_tiles.tile.Tile"""
    assert isinstance(coord, Coordinate)
    coord = coord.container()
    return Tile(int(coord.zoom), int(coord.column), int(coord.row))


def convert_to_coord_ints(coords):
    for coord in coords:
        coord_int = coord_marshall_int(coord)
        yield coord_int


class RawrToiIntersector(object):

    """
    Explode and intersect coordinates with the toi

    Prior to enqueueing the coordinates that have had their rawr tile
    generated, the list should get intersected with the toi.
    """

    def __init__(self, s3_client, bucket, key):
        self.s3_client = s3_client
        self.bucket = bucket
        self.key = key
        # state to avoid pulling down the whole list every time
        self.prev_toi = None
        self.etag = None

    def tiles_of_interest(self):
        """conditionally get the toi from s3"""
        get_options = dict(
            Bucket=self.bucket,
            Key=self.key,
        )
        if self.etag:
            get_options['IfNoneMatch'] = self.etag
        try:
            resp = self.s3_client.get_object(**get_options)
        except Exception as e:
            # boto3 client treats 304 responses as exceptions
            if isinstance(e, ClientError):
                resp = getattr(e, 'response', None)
                assert resp
            else:
                raise e
        status_code = resp['ResponseMetadata']['HTTPStatusCode']
        if status_code == 304:
            assert self.prev_toi
            toi = self.prev_toi
        elif status_code == 200:
            body = resp['Body']
            try:
                gzip_payload = body.read()
            finally:
                try:
                    body.close()
                except Exception:
                    pass
            gzip_file_obj = StringIO(gzip_payload)
            toi = load_set_from_gzipped_fp(gzip_file_obj)
            self.prev_toi = toi
            self.etag = resp['ETag']
        else:
            assert 0, 'Unknown status code from toi get: %s' % status_code

        return toi

    def __call__(self, coords):
        toi = self.tiles_of_interest()
        coord_ints = convert_to_coord_ints(coords)
        intersected_coord_ints, intersect_metrics = \
            explode_and_intersect(coord_ints, toi)
        coords = map(coord_unmarshall_int, intersected_coord_ints)
        return coords, intersect_metrics


class RawrTileGenerationPipeline(object):

    """Entry point for rawr process command"""

    def __init__(
            self, rawr_queue, msg_marshaller, group_by_zoom, rawr_gen,
            queue_writer, rawr_toi_intersector, stats_handler,
            rawr_proc_logger):
        self.rawr_queue = rawr_queue
        self.msg_marshaller = msg_marshaller
        self.group_by_zoom = group_by_zoom
        self.rawr_gen = rawr_gen
        self.queue_writer = queue_writer
        self.rawr_toi_intersector = rawr_toi_intersector
        self.stats_handler = stats_handler
        self.rawr_proc_logger = rawr_proc_logger

    def __call__(self):
        while True:
            timing = {}

            try:
                # NOTE: it's ok if reading from the queue takes a long time
                with time_block(timing, 'queue_read'):
                    msg_handle = self.rawr_queue.read()
            except Exception as e:
                self.log_exception(e, 'queue read')
                continue

            if not msg_handle:
                # this gets triggered when no messages are returned
                continue

            try:
                coords = self.msg_marshaller.unmarshall(msg_handle.payload)
            except Exception as e:
                self.log_exception(e, 'unmarshall payload')
                continue

            try:
                parent = common_parent(coords, self.group_by_zoom)
            except Exception as e:
                self.log_exception(e, 'find parent')
                continue

            try:
                rawr_tile_coord = convert_coord_object(parent)
            except Exception as e:
                self.log_exception(e, 'convert coord', parent)
                continue

            try:
                with time_block(timing, 'rawr_gen'):
                    self.rawr_gen(rawr_tile_coord)
            except Exception as e:
                self.log_exception(e, 'rawr tile gen', parent)
                continue

            try:
                with time_block(timing, 'toi_intersect'):
                    coords_to_enqueue, intersect_metrics = \
                        self.rawr_toi_intersector(coords)
            except Exception as e:
                self.log_exception(e, 'intersect coords', parent)
                continue

            try:
                with time_block(timing, 'queue_write'):
                    n_enqueued, n_inflight = \
                        self.queue_writer.enqueue_batch(coords_to_enqueue)
            except Exception as e:
                self.log_exception(e, 'queue write', parent)
                continue

            try:
                with time_block(timing, 'queue_done'):
                    self.rawr_queue.done(msg_handle)
            except Exception as e:
                self.log_exception(e, 'queue done', parent)
                continue

            try:
                self.rawr_proc_logger.processed(
                    intersect_metrics, n_enqueued, n_inflight, timing, parent)
            except Exception as e:
                self.log_exception(e, 'log', parent)
                continue

            try:
                self.stats_handler(
                    intersect_metrics, n_enqueued, n_inflight, timing)
            except Exception as e:
                self.log_exception(e, 'stats', parent)

    def log_exception(self, exception, msg, parent_coord=None):
        stacktrace = format_stacktrace_one_line()
        self.rawr_proc_logger.error(msg, exception, stacktrace, parent_coord)


def make_rawr_zip_payload(rawr_tile, date_time=None):
    """make a zip file from the rawr tile formatted data"""
    if date_time is None:
        date_time = gmtime()[0:6]

    buf = StringIO()
    with zipfile.ZipFile(buf, mode='w') as z:
        for fmt_data in rawr_tile.all_formatted_data:
            zip_info = zipfile.ZipInfo(fmt_data.name, date_time)
            z.writestr(zip_info, fmt_data.data, zipfile.ZIP_DEFLATED)
    return buf.getvalue()


def unpack_rawr_zip_payload(table_sources, io):
    """unpack a zipfile and turn it into a callable "tables" object."""
    # the io we get from S3 is streaming, so we can't seek on it, but zipfile
    # seems to require that. so we buffer it all in memory. RAWR tiles are
    # generally up to around 100MB in size, which should be safe to store in
    # RAM.
    from tilequeue.query.common import Table
    buf = io.BytesIO(io.read())
    zfh = zipfile.ZipFile(buf, 'r')

    def get_table(table_name):
        fh = zfh.open(table_name, 'r')
        unpacker = Unpacker(file_like=fh)
        source = table_sources[table_name]
        return Table(source, unpacker)

    return get_table


def make_rawr_s3_path(tile, prefix, suffix):
    path_to_hash = '%d/%d/%d%s' % (tile.z, tile.x, tile.y, suffix)
    path_hash = calc_hash(path_to_hash)
    path_with_hash = '%s/%s/%s' % (prefix, path_hash, path_to_hash)
    return path_with_hash


def make_rawr_enqueuer(rawr_queue, msg_marshaller, group_by_zoom, logger,
                       stats_handler):
    return RawrEnqueuer(rawr_queue, msg_marshaller, group_by_zoom, logger,
                        stats_handler)


class RawrS3Sink(object):

    """Rawr sink to write to s3"""

    def __init__(self, s3_client, bucket, prefix, suffix):
        self.s3_client = s3_client
        self.bucket = bucket
        self.prefix = prefix
        self.suffix = suffix

    def __call__(self, rawr_tile):
        payload = make_rawr_zip_payload(rawr_tile)
        location = make_rawr_s3_path(rawr_tile.tile, self.prefix, self.suffix)
        self.s3_client.put_object(
                Body=payload,
                Bucket=self.bucket,
                ContentType='application/zip',
                ContentLength=len(payload),
                Key=location,
        )


# implement the "get_table" interface, but always return an empty list. this
# allows us to fake an empty tile that might not be backed by any real data.
def _empty_table(table_name):
    return []


class RawrS3Source(object):

    """Rawr source to read from S3. Uses a thread pool for I/O if provided."""

    def __init__(self, s3_client, bucket, prefix, suffix, table_sources,
                 io_pool=None, allow_missing_tiles=False):
        self.s3_client = s3_client
        self.bucket = bucket
        self.prefix = prefix
        self.suffix = suffix
        self.table_sources = table_sources
        self.io_pool = io_pool
        self.allow_missing_tiles = allow_missing_tiles

    def _get_object(self, tile):
        location = make_rawr_s3_path(tile, self.prefix, self.suffix)

        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket,
                Key=location,
            )
        except Exception, e:
            # if we allow missing tiles, then translate a 404 exception into a
            # value response. this is useful for local or dev environments
            # where we might not have a global build, but don't want the lack
            # of RAWR tiles to kill jobs.
            if self.allow_missing_tiles and isinstance(e, ClientError):
                if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                    return None
            raise

        return response

    def __call__(self, tile):
        # throws an exception if the object is missing - RAWR tiles
        if self.io_pool:
            response = self.thread_pool.apply(
                self._get_object, (tile,))
        else:
            response = self._get_object(tile)

        if response is None:
            return _empty_table

        # check that the response isn't a delete marker.
        assert not response['DeleteMarker']

        body = response['Body']
        return unpack_rawr_zip_payload(self.table_sources, body)
