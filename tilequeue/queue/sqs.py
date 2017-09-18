from boto import connect_sqs
from boto.sqs.message import RawMessage
from tilequeue.queue import MessageHandle
from tilequeue.utils import grouper


class SqsQueue(object):

    def __init__(self, sqs_queue):
        self.sqs_queue = sqs_queue

    def enqueue(self, payload):
        message = RawMessage()
        message.set_body(payload)
        self.sqs_queue.write(message)

    def enqueue_batch(self, payloads):
        # sqs can only send 10 messages at once
        for payloads_chunk in grouper(payloads, 10):
            msg_tuples = []
            for i, payload in enumerate(payloads_chunk):
                msg_tuples.append((str(i), payload, 0))
            self.sqs_queue.write_batch(msg_tuples)

    def read(self, metadata=None):
        read_size = 10
        if metadata is not None:
            metadata_read_size = metadata.get('size')
            if metadata_read_size:
                read_size = metadata_read_size

        msg_handles = []
        sqs_messages = self.sqs_queue.get_messages(
            num_messages=read_size, attributes=["SentTimestamp"])
        for sqs_message in sqs_messages:
            payload = sqs_message.get_body()
            try:
                timestamp = float(sqs_message.attributes.get('SentTimestamp'))
            except (TypeError, ValueError):
                timestamp = None

            metadata = dict(
                queue_name=self.sqs_queue.name,
                timestamp=timestamp,
            )
            msg_handle = MessageHandle(sqs_message, payload, metadata)
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


def make_sqs_queue(
        queue_name, aws_access_key_id=None, aws_secret_access_key=None):
    conn = connect_sqs(aws_access_key_id, aws_secret_access_key)
    queue = conn.get_queue(queue_name)
    assert queue is not None, \
        'Could not get sqs queue with name: %s' % queue_name
    queue.set_message_class(RawMessage)
    return SqsQueue(queue)
