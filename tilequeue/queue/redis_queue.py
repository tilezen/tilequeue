from tilequeue.queue import MessageHandle
from tilequeue.tile import coord_marshall_int
from tilequeue.tile import coord_unmarshall_int
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

    def enqueue(self, coord):
        coord_int = coord_marshall_int(coord)
        self.redis_client.rpush(coord_int)

    def enqueue_batch(self, coords):
        for coords_chunk in grouper(coords, self.enqueue_batch_size):
            coord_ints = map(coord_marshall_int, coords_chunk)
            self.redis_client.rpush(self.queue_key, *coord_ints)

    def read(self):
        read_size = 10
        with self.redis_client.pipeline() as pipe:
            pipe.lrange(self.queue_key, 0, read_size - 1)
            pipe.ltrim(self.queue_key, read_size, -1)
            coord_ints, _ = pipe.execute()
        if not coord_ints:
            time.sleep(self.sleep_time_seconds_when_empty)
            return []
        msg_handles = []
        for coord_int in coord_ints:
            coord = coord_unmarshall_int(coord_int)
            msg_handle = MessageHandle(None, coord)
            msg_handles.append(msg_handle)
        return msg_handles

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
