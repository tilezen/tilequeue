'''
Tests for `tilequeue.store`.
'''

import unittest
from tilequeue import store
from tilequeue import format
from ModestMaps.Core import Coordinate
import os


class TestTileDirectory(unittest.TestCase):

    def test_write_tile(self):
        dir_path = 'tests/fixtures/tiles_dir'

        # Verify that the `TileDirectory` directory gets created.
        self.assertFalse(
            os.path.exists(dir_path),
            'The directory path used for testing `TileDirectory` already '
            'exists, but it shouldn\'t')
        tile_dir = store.TileDirectory(dir_path)
        self.assertTrue(
            os.path.isdir(dir_path),
            'The directory path passed to `TileDirectory()` wasn\'t created '
            'during initialization')

        # Verify that tile data is written to the right files.
        tiles_to_write = [
            ('tile1', (1, 2, 3), 'json'),
            ('tile2', (8, 4, 9), 'mvt'),
            ('tile3', (2, 6, 0), 'vtm')
        ]

        for tile_data, (z, c, r), fmt in tiles_to_write:
            coords_obj = Coordinate(row=r, column=c, zoom=z)
            format_obj = format.OutputFormat(fmt, fmt, None, None, None)
            tile_dir.write_tile(tile_data, coords_obj, format_obj)

            expected_filename = '{0}-{1}-{2}.{3}'.format(
                coords_obj.zoom, coords_obj.column, coords_obj.row, fmt)
            expected_path = os.path.join(dir_path, expected_filename)
            self.assertTrue(
                os.path.isfile(expected_path),
                'Tile data must not have been written to the right location, '
                'because the expected file path does not exist')

            with open(expected_path) as tile_fp:
                self.assertEqual(
                    tile_fp.read(), tile_data,
                    'Tile data written to file does not match the input data')

            os.remove(expected_path)

        os.rmdir(dir_path)
