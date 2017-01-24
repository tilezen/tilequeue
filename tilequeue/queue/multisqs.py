from boto import connect_sqs
from boto.sqs.message import RawMessage
from tilequeue.tile import coord_is_valid
from tilequeue.tile import coord_marshall_int
from tilequeue.tile import CoordMessage
from tilequeue.tile import deserialize_coord
from tilequeue.tile import serialize_coord


class MultiSqsQueue(object):

    inflight_key = 'tilequeue.in-flight'
    queue_buf_size = 10

    def __init__(self, sqs_queues, get_queue_name_for_zoom, redis_client,
                 is_seeding=False):
        self.sqs_queues = sqs_queues
        self.redis_client = redis_client
        self.get_queue_name_for_zoom = get_queue_name_for_zoom
        self.is_seeding = is_seeding
        self.sqs_queue_for_name = dict([(x.name, x) for x in sqs_queues])

    def enqueue(self, coord):
        if not coord_is_valid(coord):
            # TODO log?
            return
        coord_int = coord_marshall_int(coord)
        if not self._inflight(coord):
            payload = serialize_coord(coord)
            message = RawMessage()
            message.set_body(payload)
            sqs_queue_name = self.get_queue_name_for_zoom(coord.zoom)
            sqs_queue = self.sqs_queue_for_name.get(sqs_queue_name)
            assert sqs_queue, 'No queue found for: %s' % sqs_queue_name
            sqs_queue.write(message)
            self._add_to_flight(coord_int)

    def _inflight(self, coord_int):
        return (not self.is_seeding) and self.redis_client.sismember(
            self.inflight_key, coord_int)

    def _add_to_flight(self, coord_int):
        self.redis_client.sadd(self.inflight_key, coord_int)

    def _write_batch(self, sqs_queue, buf):
        assert len(buf) <= self.queue_buf_size
        msg_tuples = []
        coord_ints = []
        for i, (coord, coord_int) in enumerate(buf):
            msg_id = str(i)
            coord_str = serialize_coord(coord)
            msg_delay = 0
            msg_tuple = (msg_id, coord_str, msg_delay)
            msg_tuples.append(msg_tuple)
            coord_ints.append(coord_int)

        sqs_queue.write_batch(msg_tuples)
        self.redis_client.sadd(self.inflight_key, *coord_ints)

    def enqueue_batch(self, coords):
        buf_per_queue = {}
        n_queued = 0
        n_in_flight = 0
        for coord in coords:
            # TODO log?
            if not coord_is_valid(coord):
                continue
            coord_int = coord_marshall_int(coord)
            if self._inflight(coord_int):
                n_in_flight += 1
            else:
                n_queued += 1
                sqs_queue_name = self.get_queue_name_for_zoom(coord.zoom)
                queue_buf = buf_per_queue.setdefault(sqs_queue_name, [])
                queue_buf.append((coord, coord_int))
                if len(queue_buf) == self.queue_buf_size:
                    sqs_queue = self.sqs_queue_for_name.get(sqs_queue_name)
                    assert sqs_queue_name, \
                        'Missing queue for: %s' % sqs_queue_name
                    self._write_batch(sqs_queue, queue_buf)
                    del queue_buf[:]

        for queue_name, queue_buf in buf_per_queue.items():
            if queue_buf:
                sqs_queue = self.sqs_queue_for_name.get(queue_name)
                assert sqs_queue, 'Missing queue for: %s' % queue_name
                self._write_batch(sqs_queue, queue_buf)

        return n_queued, n_in_flight

    def read(self, max_to_read=None):
        if max_to_read is None:
            max_to_read = self.queue_buf_size

        coord_messages = []
        left_to_read = max_to_read

        for sqs_queue in self.sqs_queues:

            queue_messages = sqs_queue.get_messages(
                num_messages=left_to_read,
                attributes=('SentTimestamp',))

            for qm in queue_messages:

                data = qm.get_body()
                coord = deserialize_coord(data)
                if coord is None:
                    # TODO log?
                    continue

                try:
                    timestamp = float(qm.attributes.get('SentTimestamp'))
                except (TypeError, ValueError):
                    timestamp = None

                metadata = dict(
                    queue_name=sqs_queue.name,
                    timestamp=timestamp,
                )
                coord_message = CoordMessage(coord, qm, metadata)
                coord_messages.append(coord_message)
                left_to_read -= 1

            assert(left_to_read >= 0)
            if left_to_read == 0:
                break

        return coord_messages

    def job_done(self, coord_message):
        queue_name = None
        if coord_message.metadata:
            queue_name = coord_message.metadata.get('queue_name')
        assert queue_name, \
            'Missing queue name metadata for coord: %s' % serialize_coord(
                coord_message.coord)

        sqs_queue = self.sqs_queue_for_name.get(queue_name)
        assert sqs_queue, 'Missing queue for: %s' % queue_name

        coord_int = coord_marshall_int(coord_message.coord)

        self.redis_client.srem(self.inflight_key, coord_int)
        sqs_queue.delete_message(coord_message.message_handle)

    def clear(self):
        self.redis_client.delete(self.inflight_key)
        n = 0
        for sqs_queue in self.sqs_queues:
            while True:
                # TODO newer versions of boto have a purge method on
                # queues
                msgs = sqs_queue.get_messages(self.queue_buf_size)
                if not msgs:
                    break
                sqs_queue.delete_message_batch(msgs)
                n += len(msgs)

        return n

    def close(self):
        pass


def make_multi_sqs_queue(
        queue_names, get_queue_name_for_zoom, redis_client,
        is_seeding=False, aws_access_key_id=None, aws_secret_access_key=None):

    conn = connect_sqs(aws_access_key_id, aws_secret_access_key)

    sqs_queues = []
    for queue_name in queue_names:
        aws_queue = conn.get_queue(queue_name)
        assert aws_queue is not None, \
            'Could not get sqs queue with name: %s' % queue_name
        aws_queue.set_message_class(RawMessage)
        sqs_queues.append(aws_queue)

    result = MultiSqsQueue(
        sqs_queues, get_queue_name_for_zoom, redis_client, is_seeding)
    return result
