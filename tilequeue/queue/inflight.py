from tilequeue.tile import coord_marshall_int
from tilequeue.utils import grouper


class InFlightManager(object):

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

    def filter(self, coords):
        for coord in coords:
            coord_int = coord_marshall_int(coord)
            if not self.redis_client.sismember(self.inflight_key, coord_int):
                yield coord

    def mark_inflight(self, coords):
        for coords_chunk in grouper(coords, self.chunk_size):
            self.redis_client.sadd(self.inflight_key, *coords_chunk)
