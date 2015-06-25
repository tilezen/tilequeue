from tilequeue.tile import coord_marshall_int
from tilequeue.tile import CoordMessage
from tilequeue.tile import coord_unmarshall_int
import time


class RedisQueue(object):
    """
    Redis backed queue implementation

    No attempts are made to guarantee messages are not lost if not
    acknowledged. If a worker reads messages from the queue and
    crashes, they are lost.
    """

    enqueue_batch_size = 1024
    sleep_time_seconds_when_empty = 10

    def __init__(self, redis_client, queue_key):
        self.redis_client = redis_client
        self.queue_key = queue_key

    def enqueue(self, coord):
        coord_int = coord_marshall_int(coord)
        self.redis_client.rpush(coord_int)

    def enqueue_batch(self, coords):
        n = 0
        coord_buffer = []
        for coord in coords:
            coord_int = coord_marshall_int(coord)
            coord_buffer.append(coord_int)
            n += 1
            if len(coord_buffer) >= self.enqueue_batch_size:
                self.redis_client.rpush(self.queue_key, *coord_buffer)
                del coord_buffer[:]
        if coord_buffer:
            self.redis_client.rpush(self.queue_key, *coord_buffer)
        return n, 0

    def read(self, max_to_read=10):
        with self.redis_client.pipeline() as pipe:
            pipe.lrange(self.queue_key, 0, max_to_read - 1)
            pipe.ltrim(self.queue_key, max_to_read, -1)
            coord_ints, _ = pipe.execute()
        if not coord_ints:
            time.sleep(self.sleep_time_seconds_when_empty)
            return []
        coord_msgs = []
        for coord_int in coord_ints:
            coord = coord_unmarshall_int(coord_int)
            coord_msg = CoordMessage(coord, None)
            coord_msgs.append(coord_msg)
        return coord_msgs

    def job_done(self, coord_message):
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
