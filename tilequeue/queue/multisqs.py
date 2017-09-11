from boto import connect_sqs
from boto.sqs.message import RawMessage
from tilequeue.queue import MessageHandle
from tilequeue.tile import deserialize_coord
from tilequeue.tile import serialize_coord


class MultiSqsQueue(object):

    queue_buf_size = 10

    def __init__(self, sqs_queues, get_queue_name_for_zoom):
        self.sqs_queues = sqs_queues
        self.get_queue_name_for_zoom = get_queue_name_for_zoom
        self.sqs_queue_for_name = dict([(x.name, x) for x in sqs_queues])

    def enqueue(self, coord):
        payload = serialize_coord(coord)
        message = RawMessage()
        message.set_body(payload)
        sqs_queue_name = self.get_queue_name_for_zoom(coord.zoom)
        sqs_queue = self.sqs_queue_for_name.get(sqs_queue_name)
        assert sqs_queue, 'No queue found for: %s' % sqs_queue_name
        sqs_queue.write(message)

    def _write_batch(self, sqs_queue, buf):
        assert len(buf) <= self.queue_buf_size
        msg_tuples = []
        for i, coord in enumerate(buf):
            msg_id = str(i)
            coord_str = serialize_coord(coord)
            msg_delay = 0
            msg_tuple = (msg_id, coord_str, msg_delay)
            msg_tuples.append(msg_tuple)

        sqs_queue.write_batch(msg_tuples)

    def enqueue_batch(self, coords):
        buf_per_queue = {}
        for coord in coords:
            sqs_queue_name = self.get_queue_name_for_zoom(coord.zoom)
            queue_buf = buf_per_queue.setdefault(sqs_queue_name, [])
            queue_buf.append(coord)
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

    def read(self):
        msg_handles = []

        read_size = 10
        for sqs_queue in self.sqs_queues:

            queue_messages = sqs_queue.get_messages(
                num_messages=read_size,
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
                msg_handle = MessageHandle(qm, coord, metadata)
                msg_handles.append(msg_handle)

            if msg_handles:
                break

        return msg_handles

    def job_done(self, msg_handle):
        queue_name = None
        if msg_handle.metadata:
            queue_name = msg_handle.metadata.get('queue_name')
        assert queue_name, \
            'Missing queue name metadata for coord: %s' % serialize_coord(
                msg_handle.coord)

        sqs_queue = self.sqs_queue_for_name.get(queue_name)
        assert sqs_queue, 'Missing queue for: %s' % queue_name

        sqs_queue.delete_message(msg_handle.handle)

    def clear(self):
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


def make_multi_sqs_queue(queue_names, get_queue_name_for_zoom, redis_client,
                         aws_access_key_id=None, aws_secret_access_key=None):

    # TODO can consider taking the queue objects themselves here
    # instead of creating them each
    # the creation can be handled separately
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
