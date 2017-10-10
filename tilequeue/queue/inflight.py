from tilequeue.tile import coord_marshall_int
from tilequeue.utils import grouper


class RedisInFlightManager(object):

    """
    manage in flight list

    Manage the two operations for the inflight list:

    1. filter coordinates out that are already in flight
    2. mark coordinates as in flight (presumably these were just enqueued)
    """

    def __init__(self, redis_client, inflight_key, chunk_size=100):
        self.redis_client = redis_client
        self.inflight_key = inflight_key
        self.chunk_size = chunk_size

    def is_inflight(self, coord):
        coord_int = coord_marshall_int(coord)
        return self.redis_client.sismember(self.inflight_key, coord_int)

    def filter(self, coords):
        for coord in coords:
            if not self.is_inflight(coord):
                yield coord

    def mark_inflight(self, coords):
        for coords_chunk in grouper(coords, self.chunk_size):
            coord_ints = map(coord_marshall_int, coords_chunk)
            self.redis_client.sadd(self.inflight_key, *coord_ints)

    def unmark_inflight(self, coord):
        coord_int = coord_marshall_int(coord)
        self.redis_client.srem(self.inflight_key, coord_int)


class NoopInFlightManager(object):

    def filter(self, coords):
        return coords

    def is_inflight(self, coord_int):
        return False

    def mark_inflight(self, coords):
        pass

    def unmark_inflight(self, coord):
        pass
