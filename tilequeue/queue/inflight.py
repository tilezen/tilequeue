from tilequeue.tile import coord_marshall_int
from tilequeue.utils import grouper


class InFlightGuardedQueue(object):

    """
    check a redis key around queue operations to prevent dupes

    This serves as an optimization to prevent duplicates in the queue,
    and therefore the operations are ordered such that in the event of
    a failure, the preference is given to have the item missing from
    redis but exist in the queue.
    """

    def __init__(self, tile_queue, redis_client, inflight_key):
        self.tile_queue = tile_queue
        self.redis_client = redis_client

    def _inflight(self, coord_int):
        return self.redis_client.sismember(self.inflight_key, coord_int)

    def enqueue(self, coord):
        coord_int = coord_marshall_int(coord)
        if self._inflight(coord_int):
            return
        self.tile_queue.enqueue(coord)
        self.redis_client.sadd(self.inflight_key, coord_int)

    def enqueue_batch(self, coords):
        n_queued = 0
        n_inflight = 0
        # seems like a reasonable size to send to redis at a time
        redis_batch_size = 100
        for coords_chunk in grouper(coords, redis_batch_size):
            coords_to_enqueue = []
            coord_ints_to_add = []
            for coord in coords_chunk:
                coord_int = coord_marshall_int(coord)
                if self._inflight(coord_int):
                    n_inflight += 1
                else:
                    coords_to_enqueue.append(coord)
                    coord_ints_to_add.append(coord_int)
                    n_queued += 1
            if coords_to_enqueue:
                self.tile_queue.enqueue_batch(coords_to_enqueue)
                self.redis_client.sadd(self.inflight_key, *coord_ints_to_add)
        return n_queued, n_inflight

    def read(self):
        return self.tile_queue.read()

    def job_done(self, coord_message):
        coord_int = coord_marshall_int(coord_message.coord)
        self.redis_client.srem(self.inflight_key, coord_int)
        self.tile_queue.job_done(coord_message)

    def clear(self):
        self.redis_client.delete(self.inflight_key)
        self.tile_queue.clear()


def make_inflight_queue(tile_queue, redis_client,
                        inflight_key='tilequeue.in-flight'):
    return InFlightGuardedQueue(tile_queue, redis_client, inflight_key)
