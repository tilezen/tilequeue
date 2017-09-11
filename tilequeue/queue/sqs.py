from boto import connect_sqs
from boto.sqs.message import RawMessage
from tilequeue.queue import MessageHandle
from tilequeue.tile import deserialize_coord
from tilequeue.tile import serialize_coord
from tilequeue.utils import grouper


class SqsQueue(object):

    def __init__(self, sqs_queue):
        self.sqs_queue = sqs_queue

    def enqueue(self, coord):
        payload = serialize_coord(coord)
        message = RawMessage()
        message.set_body(payload)
        self.sqs_queue.write(message)

    def _write_batch(self, coords):
        assert len(coords) <= 10

    def enqueue_batch(self, coords):
        # sqs can only send 10 messages at once
        for coord_chunk in grouper(coords, 10):
            msg_tuples = []

            for i, coord in enumerate(coords):
                msg_tuples.append((str(i), serialize_coord(coord), 0))

            self.sqs_queue.write_batch(msg_tuples)

    def read(self):
        msg_handles = []
        read_size = 10
        sqs_messages = self.sqs_queue.get_messages(
            num_messages=read_size, attributes=["SentTimestamp"])
        for sqs_message in sqs_messages:
            data = sqs_message.get_body()
            coord = deserialize_coord(data)
            if coord is None:
                # TODO log?
                continue
            try:
                timestamp = float(sqs_message.attributes.get('SentTimestamp'))
            except (TypeError, ValueError):
                timestamp = None

            metadata = dict(
                queue_name=self.sqs_queue.name,
                timestamp=timestamp,
            )
            msg_handle = MessageHandle(sqs_message, coord, metadata)
            msg_handles.append(msg_handle)
        return msg_handles

    def job_done(self, msg_handle):
        self.sqs_queue.delete_message(msg_handle.handle)

    def clear(self):
        n = 0
        num_messages = 10
        while True:
            msgs = self.sqs_queue.get_messages(num_messages)
            if not msgs:
                break
            self.sqs_queue.delete_message_batch(msgs)
            n += len(msgs)
        return n

    def close(self):
        pass


def make_sqs_queue(queue_name, redis_client,
                   aws_access_key_id=None, aws_secret_access_key=None):
    # TODO should this just take the queue directly instead? we can
    # offer another function to create the queue separately
    conn = connect_sqs(aws_access_key_id, aws_secret_access_key)
    queue = conn.get_queue(queue_name)
    assert queue is not None, \
        'Could not get sqs queue with name: %s' % queue_name
    queue.set_message_class(RawMessage)
    return SqsQueue(queue, redis_client)
