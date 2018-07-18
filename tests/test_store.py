'''
Tests for `tilequeue.store`.
'''

import unittest


class TestTileDirectory(unittest.TestCase):

    def setUp(self):
        import tempfile
        self.dir_path = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.dir_path)

    def test_write_tile(self):
        from ModestMaps.Core import Coordinate
        from tilequeue import format
        from tilequeue import store
        import os
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

        layer = 'all'
        for tile_data, (z, c, r), fmt in tiles_to_write:
            coords_obj = Coordinate(row=r, column=c, zoom=z)
            format_obj = format.OutputFormat(fmt, fmt, None, None, None, False)
            tile_dir.write_tile(tile_data, coords_obj, format_obj, layer)

            expected_filename = '{0}/{1}/{2}/{3}.{4}'.format(
                layer, coords_obj.zoom, coords_obj.column, coords_obj.row, fmt)
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
        self.assertEqual(tile_key, '20160121/b707d/osm/all/8/72/105.json')

    def test_no_path(self):
        from tilequeue.store import s3_tile_key
        from tilequeue.tile import deserialize_coord
        from tilequeue.format import json_format
        coord = deserialize_coord('8/72/105')
        date_str = '20160121'
        path = ''
        layer = 'all'
        tile_key = s3_tile_key(date_str, path, layer, coord,
                               json_format.extension)
        self.assertEqual(tile_key, '20160121/cfc61/all/8/72/105.json')


class WriteTileIfChangedTest(unittest.TestCase):

    def setUp(self):
        self._in = None
        self._out = None
        self.store = type(
            'test-store',
            (),
            dict(read_tile=self._read_tile, write_tile=self._write_tile)
        )

    def _read_tile(self, coord, format, layer):
        return self._in

    def _write_tile(self, tile_data, coord, format, layer):
        self._out = tile_data

    def _call_fut(self, tile_data):
        from tilequeue.store import write_tile_if_changed
        coord = format = layer = None
        result = write_tile_if_changed(
            self.store, tile_data, coord, format, layer)
        return result

    def test_no_data(self):
        did_write = self._call_fut('data')
        self.assertTrue(did_write)
        self.assertEquals('data', self._out)

    def test_diff_data(self):
        self._in = 'different data'
        did_write = self._call_fut('data')
        self.assertTrue(did_write)
        self.assertEquals('data', self._out)

    def test_same_data(self):
        self._in = 'data'
        did_write = self._call_fut('data')
        self.assertFalse(did_write)
        self.assertIsNone(self._out)


class S3Test(unittest.TestCase):

    def _make_stub_s3_client(self):
        class stub_s3_client(object):
            def put_object(self, **props):
                self.put_props = props
        return stub_s3_client()

    def test_tags(self):
        from tilequeue.store import S3
        s3_client = self._make_stub_s3_client()
        tags = None
        store = S3(s3_client, 'bucket', 'prefix', 'path', False, 60, None,
                   'public-read', tags)

        tile_data = 'data'
        from tilequeue.tile import deserialize_coord
        coord = deserialize_coord('14/1/2')
        from tilequeue.format import mvt_format
        store.write_tile(tile_data, coord, mvt_format, 'all')
        self.assertIsNone(store.s3_client.put_props.get('Tagging'))

        store.tags = dict(prefix='foo', runid='bar')
        store.write_tile(tile_data, coord, mvt_format, 'all')
        self.assertEquals('prefix=foo&runid=bar',
                          store.s3_client.put_props.get('Tagging'))
