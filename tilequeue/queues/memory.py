from tilequeue.tile import TileMessage

class MemoryQueue(object):

    def __init__(self):
        self.q = []

    def enqueue_tile(self, tile):
        self.q.append(tile)

    def enqueue_tiles(self, tiles):
        self.q.extend(tiles)

    def read_tiles(self, max_tiles=1, timeout_seconds=None):
        self.q, tiles = self.q[max_tiles:], self.q[:max_tiles]
        return [TileMessage(tile, None) for tile in tiles]

    def job_done(self, message):
        pass

    def jobs_done(self, messages):
        pass
