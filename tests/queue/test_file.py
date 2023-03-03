'''
Unit tests for `tilequeue.queue.file`.
'''
import StringIO
import unittest

from ModestMaps.Core import Coordinate


class TestQueue(unittest.TestCase):

    def setUp(self):
        from tilequeue import tile
        from tilequeue.queue import OutputFileQueue

        self.test_tile_coords = [
            (0, 0, 0),
            (1, 2, 3),
            (4, 5, 6),
            (9, 3, 1),
            (4, 7, 1)
        ]
        self.test_tile_objs = [
            Coordinate(row=r, column=c, zoom=z)
            for (z, c, r) in self.test_tile_coords]
        self.tile_coords_str = '\n'.join(
            map(tile.serialize_coord, self.test_tile_objs)) + '\n'
        self.tiles_fp = StringIO.StringIO()
        self.queue = OutputFileQueue(self.tiles_fp)

    def test_read(self):
        from tilequeue.tile import serialize_coord
        self._write_str_to_file(self.tile_coords_str)

        # Test `.read() for multiple records.`
        actual_coord_strs = [
            msg.payload for msg in self.queue.read()]
        expected = map(serialize_coord, self.test_tile_objs)
        self.assertEqual(
            actual_coord_strs, expected, 'Reading multiple records failed')

    def test_enqueue_and_enqueue_batch(self):
        from tilequeue.tile import serialize_coord
        # Test `.enqueue_batch()`.
        num_to_enqueue = 3
        self.assertEqual(
            self.queue.enqueue_batch(
                map(serialize_coord, self.test_tile_objs[:num_to_enqueue])),
            (num_to_enqueue, 0),
            'Return value of `enqueue_batch()` does not match expected'
        )

        # Test `.enqueue()`.
        for coord in self.test_tile_objs[num_to_enqueue:]:
            self.queue.enqueue(serialize_coord(coord))

        self.assertEqual(
            self.tiles_fp.getvalue(),
            self.tile_coords_str,
            'Contents of file do not match expected')

    def test_clear(self):
        self._write_str_to_file(self.tile_coords_str)
        self.assertEqual(
            self.queue.clear(), -1,
            'Return value of `clear()` does not match expected.')
        self.assertEqual(
            self.tiles_fp.getvalue(), '', '`clear()` did not clear the file!')

    def test_close(self):
        self.assertFalse(
            self.tiles_fp.closed,
            'Sanity check failed: the test runner\'s file pointer appears to '
            'be closed. This shouldn\'t ever happen.')
        self.queue.close()
        self.assertTrue(self.tiles_fp.closed, 'File pointer was not closed!')

    def _write_str_to_file(self, string):
        self.tiles_fp.write(string)
        self.tiles_fp.seek(0)
