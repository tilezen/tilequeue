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
from tilequeue.tile import serialize_coord
from tilequeue.toi import load_set_from_gzipped_fp
from tilequeue.utils import format_stacktrace_one_line
from tilequeue.utils import grouper
from tilequeue.utils import time_block
from time import gmtime
import json
import zipfile


class SqsQueue(object):

    def __init__(self, sqs_client, queue_url):
        self.sqs_client = sqs_client
        self.queue_url = queue_url

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
        )
        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise Exception('Invalid status code from sqs: %s' %
                            resp['ResponseMetadata']['HTTPStatusCode'])
        msgs = resp['Messages']
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

    def __init__(self, rawr_queue, msg_marshaller, group_by_zoom, logger):
        self.rawr_queue = rawr_queue
        self.msg_marshaller = msg_marshaller
        self.group_by_zoom = group_by_zoom
        self.logger = logger

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
                'Expiry processed: '
                'coords(%d) payloads(%d) enqueue-calls(%d))' %
                (n_coords, n_payloads, n_msgs_sent))


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
                except:
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
        intersected_coord_ints, _ = explode_and_intersect(coord_ints, toi)
        for coord_int in intersected_coord_ints:
            coord = coord_unmarshall_int(coord_int)
            yield coord


class RawrTileGenerationPipeline(object):

    """Entry point for rawr process command"""

    def __init__(
            self, rawr_queue, msg_marshaller, group_by_zoom, rawr_gen,
            queue_writer, rawr_toi_intersector, logger=None):
        self.rawr_queue = rawr_queue
        self.msg_marshaller = msg_marshaller
        self.group_by_zoom = group_by_zoom
        self.rawr_gen = rawr_gen
        self.queue_writer = queue_writer
        self.rawr_toi_intersector = rawr_toi_intersector
        self.logger = logger

    def __call__(self):
        while True:
            try:
                msg_handle = None
                parent = None
                rawr_tile_generated = False
                coords_intersected = False
                coords_enqueued = False
                timing = {}

                # NOTE: it's ok if reading from the queue takes a long time
                with time_block(timing, 'queue-read'):
                    msg_handle = self.rawr_queue.read()

                coords = self.msg_marshaller.unmarshall(msg_handle.payload)
                parent = common_parent(coords, self.group_by_zoom)
                rawr_tile_coord = convert_coord_object(parent)

                with time_block(timing, 'rawr-gen'):
                    self.rawr_gen(rawr_tile_coord)
                rawr_tile_generated = True

                with time_block(timing, 'toi-intersect'):
                    # because this returns a generator, the timing is wrong
                    # unless we realize immediately
                    coords_to_enqueue = list(self.rawr_toi_intersector(coords))
                coords_intersected = True

                with time_block(timing, 'queue-write'):
                    self.queue_writer.enqueue_batch(coords_to_enqueue)
                coords_enqueued = True

                with time_block(timing, 'queue-done'):
                    self.rawr_queue.done(msg_handle)

                if self.logger:
                    self.logger.info(
                        'Rawr message processed: '
                        'tile(%s) n-coords(%s) timing(%s)' % (
                            serialize_coord(parent),
                            len(coords),
                            json.dumps(timing),
                        ))
            except Exception:
                stacktrace = format_stacktrace_one_line()
                if self.logger:
                    if not msg_handle:
                        msg = 'could not read rawr tile from queue'
                    elif not rawr_tile_generated:
                        msg = 'generating rawr tile'
                    elif not coords_intersected:
                        msg = 'intersecting coords'
                    elif not coords_enqueued:
                        msg = 'enqueueing coords'
                    else:
                        msg = 'acknowledging coord'
                    msg = 'Error: %s' % msg
                    if parent:
                        msg += ' for parent: %s' % serialize_coord(parent)
                    msg += ' - %s' % stacktrace
                    self.logger.info(msg)


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


def unpack_rawr_zip_payload(io):
    """unpack a zipfile and turn it into a callable "tables" object."""
    # TODO: the io we get from S3 is streaming, so we can't seek on it, but
    # zipfile seems to require that. should we write a temporary file rather
    # than storing it all in memory?
    buf = io.BytesIO(io.read())
    zfh = zipfile.ZipFile(buf, 'r')

    def get_table(table_name):
        fh = zfh.open(table_name, 'r')
        unpacker = Unpacker(file_like=fh)
        for obj in unpacker:
            yield obj

    return get_table


def make_rawr_s3_path(tile, prefix, suffix):
    path_to_hash = '%d/%d/%d%s' % (tile.z, tile.x, tile.y, suffix)
    path_hash = calc_hash(path_to_hash)
    path_with_hash = '%s/%s/%s' % (prefix, path_hash, path_to_hash)
    return path_with_hash


def make_rawr_enqueuer(rawr_queue, msg_marshaller, group_by_zoom, logger):
    return RawrEnqueuer(rawr_queue, msg_marshaller, group_by_zoom, logger)


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


class RawrS3Source(object):

    """Rawr source to read from S3"""

    def __init__(self, s3_client, bucket, prefix, suffix):
        self.s3_client = s3_client
        self.bucket = bucket
        self.prefix = prefix
        self.suffix = suffix

    def __call__(self, tile):
        location = make_rawr_s3_path(tile, self.prefix, self.suffix)

        # TODO: i guess this throws an exception if the object is missing, or
        # there's a server error. do we want to catch it, or is propagating it
        # the right option?
        response = self.s3_client.get_object(
                Bucket=self.bucket,
                Key=location,
        )

        # check that the response isn't a delete marker.
        assert not response['DeleteMarker']

        body = response['Body']
        return unpack_rawr_zip_payload(body)
