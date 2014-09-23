import unittest

class TestSeedProcess(unittest.TestCase):

    def _call_fut(self, tile_generator, queue):
        from tilequeue.command import queue_seed_process
        return queue_seed_process(tile_generator, queue)

    def _mem_queue(self):
        from tilequeue.queue import MemoryQueue
        return MemoryQueue()

    def test_queue_seed_process(self):
        from functools import partial
        from tilequeue.seed import seed_tiles
        zoom = 3
        tile_generator = seed_tiles(0, zoom)
        queue = self._mem_queue()
        n_tiles = self._call_fut(tile_generator, queue)

        from tilequeue.tile import n_tiles_in_zoom
        expected_num_tiles = n_tiles_in_zoom(zoom)
        self.assertEqual(expected_num_tiles, n_tiles)

        # verify that the mem queue has all the tiles
        self.assertEqual(expected_num_tiles, len(queue.q))
