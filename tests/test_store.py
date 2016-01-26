'''
Tests for `tilequeue.store`.
'''

import unittest
from tilequeue import store
from tilequeue import format
from ModestMaps.Core import Coordinate
import os
import shutil
import tempfile


class TestTileDirectory(unittest.TestCase):

    def setUp(self):
        self.dir_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir_path)

    def test_write_tile(self):

        # Verify that the `TileDirectory` directory gets created.
        tile_dir = store.TileDirectory(self.dir_path)
        self.assertTrue(
            os.path.isdir(self.dir_path),
            'The directory path passed to `TileDirectory()` wasn\'t created '
            'during initialization')

        # Verify that tile data is written to the right files.
        tiles_to_write = [
            ('tile1', (1, 2, 3), 'json'),
            ('tile2', (8, 4, 9), 'mvt'),
            ('tile3', (2, 6, 0), 'vtm'),
            ('tile4', (2, 6, 1), 'topojson'),
        ]

        for tile_data, (z, c, r), fmt in tiles_to_write:
            coords_obj = Coordinate(row=r, column=c, zoom=z)
            format_obj = format.OutputFormat(fmt, fmt, None, None, None)
            tile_dir.write_tile(tile_data, coords_obj, format_obj)

            expected_filename = '{0}/{1}/{2}.{3}'.format(
                coords_obj.zoom, coords_obj.column, coords_obj.row, fmt)
            expected_path = os.path.join(self.dir_path, expected_filename)
            self.assertTrue(
                os.path.isfile(expected_path),
                'Tile data must not have been written to the right location, '
                'because the expected file path does not exist')

            with open(expected_path) as tile_fp:
                self.assertEqual(
                    tile_fp.read(), tile_data,
                    'Tile data written to file does not match the input data')

            os.remove(expected_path)


class TestStoreKey(unittest.TestCase):

    def test_example_coord(self):
        from tilequeue.store import s3_tile_key
        from tilequeue.tile import deserialize_coord
        from tilequeue.format import json_format
        coord = deserialize_coord('8/72/105')
        date_str = '20160121'
        path = 'osm'
        layer = 'all'
        tile_key = s3_tile_key(date_str, path, layer, coord,
                               json_format.extension)
        self.assertEqual(tile_key, '/20160121/b707d/osm/all/8/72/105.json')
