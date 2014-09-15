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

    def enqueue_batch(self, coords):
        # unclear if boto will handle submitting more than 10 at once
        # to be safe we do that here
        if len(coords) <= 10:
            messages = []
            for i, coord in enumerate(coords):
                msg_tuple = (str(i), serialize_coord(coord), 0)
                messages.append(msg_tuple)
            self.sqs_queue.write_batch(messages)
        else:
            self.enqueue_batch(coords[:10])
            self.enqueue_batch(coords[10:])

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

def make_sqs_queue(queue_name, aws_access_key_id=None, aws_secret_access_key=None):
    # this doesn't actually create a queue in aws, it just creates a python
    # queue object
    conn = connect_sqs(aws_access_key_id, aws_secret_access_key)
    queue = conn.get_queue(queue_name)
    assert queue is not None, 'Could not get sqs queue with name: %s' % queue_name
    queue.set_message_class(RawMessage)
    return SqsQueue(queue)
