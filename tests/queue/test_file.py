'''
Unit tests for `tilequeue.queue.file`.
'''

from tilequeue.queue import OutputFileQueue
from tilequeue import tile
from ModestMaps.Core import Coordinate
import unittest
import StringIO


class TestQueue(unittest.TestCase):

    def setUp(self):
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
        self._write_str_to_file(self.tile_coords_str)

        # Test `.read() for multiple records.`
        num_to_read = 3
        actual_coords = [
            msg.coord for msg in self.queue.read(max_to_read=num_to_read)]
        expected = self.test_tile_objs[:num_to_read]
        self.assertEqual(
            actual_coords, expected, 'Reading multiple records failed')

        # Test `.read()` for just 1 record at a time.
        for expected in self.test_tile_objs[num_to_read:]:
            [actual_msg] = self.queue.read()
            self.assertEqual(
                actual_msg.coord, expected,
                'Reading 1 record failed')

    def test_enqueue_and_enqueue_batch(self):
        # Test `.enqueue_batch()`.
        num_to_enqueue = 3
        self.assertEqual(
            self.queue.enqueue_batch(self.test_tile_objs[:num_to_enqueue]),
            (num_to_enqueue, 0),
            'Return value of `enqueue_batch()` does not match expected'
        )

        # Test `.enqueue()`.
        for coords in self.test_tile_objs[num_to_enqueue:]:
            self.queue.enqueue(coords)

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
