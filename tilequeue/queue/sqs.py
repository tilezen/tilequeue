from boto import connect_sqs
from boto.sqs.message import RawMessage
from tilequeue.tile import CoordMessage
from tilequeue.tile import deserialize_coord
from tilequeue.tile import serialize_coord


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
        msg_tuples = [(str(i), serialize_coord(coord), 0)
                      for i, coord in enumerate(coords)]
        self.sqs_queue.write_batch(msg_tuples)

    def enqueue_batch(self, coords):
        buffer = []
        n = 0
        for coord in coords:
            buffer.append(coord)
            if len(buffer) == 10:
                self._write_batch(buffer)
                del buffer[:]
            n += 1
        if buffer:
            self._write_batch(buffer)
        return n

    def read(self, max_to_read=1, timeout_seconds=20):
        coord_messages = []
        messages = self.sqs_queue.get_messages(num_messages=max_to_read)
        if not messages:
            message = self.sqs_queue.read(wait_time_seconds=timeout_seconds)
            if message is None:
                return []
            messages = [message]
        for message in messages:
            data = message.get_body()
            coord = deserialize_coord(data)
            if coord is None:
                # log?
                continue
            coord_message = CoordMessage(coord, message)
            coord_messages.append(coord_message)
        return coord_messages

    def job_done(self, message):
        self.sqs_queue.delete_message(message)

    def jobs_done(self, messages):
        self.sqs_queue.delete_message_batch(messages)

    def clear(self):
        n = 0
        while True:
            msgs = self.sqs_queue.get_messages(10)
            if not msgs:
                break
            self.sqs_queue.delete_message_batch(msgs)
            n += len(msgs)
        return n

    def close(self):
        pass


def make_sqs_queue(queue_name,
                   aws_access_key_id=None, aws_secret_access_key=None):
    # this doesn't actually create a queue in aws, it just creates a python
    # queue object
    conn = connect_sqs(aws_access_key_id, aws_secret_access_key)
    queue = conn.get_queue(queue_name)
    assert queue is not None, \
        'Could not get sqs queue with name: %s' % queue_name
    queue.set_message_class(RawMessage)
    return SqsQueue(queue)
