from tilequeue.queue import MessageHandle
from tilequeue.utils import grouper
import time


class RedisQueue(object):
    """
    Redis backed queue implementation

    No attempts are made to guarantee messages are not lost if not
    acknowledged. If a worker reads messages from the queue and
    crashes, they are lost.
    """

    enqueue_batch_size = 100
    sleep_time_seconds_when_empty = 10

    def __init__(self, redis_client, queue_key):
        self.redis_client = redis_client
        self.queue_key = queue_key

    def enqueue(self, payload):
        self.redis_client.rpush(payload)

    def enqueue_batch(self, payloads):
        for payloads_chunk in grouper(payloads, self.enqueue_batch_size):
            self.redis_client.rpush(self.queue_key, *payloads_chunk)

    def read(self):
        read_size = 10
        with self.redis_client.pipeline() as pipe:
            pipe.lrange(self.queue_key, 0, read_size - 1)
            pipe.ltrim(self.queue_key, read_size, -1)
            payloads, _ = pipe.execute()
        if not payloads:
            time.sleep(self.sleep_time_seconds_when_empty)
            return []
        msg_handles = []
        for payload in payloads:
            msg_handle = MessageHandle(None, payload)
            msg_handles.append(msg_handle)
        return msg_handles

    def job_progress(self, handle):
        pass

    def job_done(self, msg_handle):
        pass

    def clear(self):
        with self.redis_client.pipeline() as pipe:
            pipe.llen(self.queue_key)
            pipe.delete(self.queue_key)
            n, _ = pipe.execute()
        return n

    def close(self):
        pass


def make_redis_queue(redis_client, queue_key):
    return RedisQueue(redis_client, queue_key)
