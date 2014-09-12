from boto import connect_sqs
from boto.sqs.message import RawMessage
from tilequeue.tile import deserialize_tile_job
from tilequeue.tile import serialize_tile_job
from tilequeue.tile import TileJobMessage

class SqsQueue(object):

    def __init__(self, sqs_queue):
        self.sqs_queue = sqs_queue

    def enqueue_tile_job(self, tile_job):
        payload = serialize_tile_job(tile_job)
        message = RawMessage()
        message.set_body(payload)
        self.sqs_queue.write(message)

    def enqueue_tile_jobs(self, tile_jobs):
        # unclear if boto will handle submitting more than 10 at once
        # to be safe we do that here
        if len(tile_jobs) <= 10:
            messages = []
            for i, tile_job in enumerate(tile_jobs):
                msg_tuple = (str(i), serialize_tile_job(tile_job), 0)
                messages.append(msg_tuple)
            self.sqs_queue.write_batch(messages)
        else:
            self.enqueue_tile_jobs(tile_jobs[:10])
            self.enqueue_tile_jobs(tile_jobs[10:])

    def read_tile_jobs(self, max_tile_jobs=1, timeout_seconds=20):
        tile_job_messages = []
        messages = self.sqs_queue.get_messages(num_messages=max_tile_jobs)
        if not messages:
            message = self.sqs_queue.read(wait_time_seconds=timeout_seconds)
            if message is None:
                return []
            messages = [message]
        for message in messages:
            data = message.get_body()
            tile_job = deserialize_tile_job(data)
            if tile_job is None:
                # log?
                continue
            tile_job_message = TileJobMessage(tile_job, message)
            tile_job_messages.append(tile_job_message)
        return tile_job_messages

    def job_done(self, message):
        self.sqs_queue.delete_message(message)

    def jobs_done(self, messages):
        self.sqs_queue.delete_message_batch(messages)

def make_sqs_queue(queue_name, aws_access_key_id, aws_secret_access_key):
    # this doesn't actually create a queue in aws, it just creates a python
    # queue object
    conn = connect_sqs(aws_access_key_id, aws_secret_access_key)
    queue = conn.get_queue(queue_name)
    assert queue is not None, 'Could not get sqs queue with name: %s' % queue_name
    return SqsQueue(queue)
