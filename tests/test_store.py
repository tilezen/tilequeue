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

        for tile_data, (z, c, r), fmt in tiles_to_write:
            coords_obj = Coordinate(row=r, column=c, zoom=z)
            format_obj = format.OutputFormat(fmt, fmt, None, None, None, False)
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
        from tilequeue.tile import deserialize_coord
        from tilequeue.format import json_format
        from tilequeue.store import KeyFormatType
        from tilequeue.store import S3TileKeyGenerator
        coord = deserialize_coord('8/72/105')
        prefix = '20160121'
        tile_key_gen = S3TileKeyGenerator(
            key_format_type=KeyFormatType.hash_prefix)
        tile_key = tile_key_gen(prefix, coord, json_format.extension)
        self.assertEqual(tile_key, 'b57e9/20160121/8/72/105.json')


class WriteTileIfChangedTest(unittest.TestCase):

    def setUp(self):
        self._in = None
        self._out = None
        self.store = type(
            'test-store',
            (),
            dict(read_tile=self._read_tile, write_tile=self._write_tile)
        )

    def _read_tile(self, coord, format):
        return self._in

    def _write_tile(self, tile_data, coord, format):
        self._out = tile_data

    def _call_fut(self, tile_data):
        from tilequeue.store import write_tile_if_changed
        coord = format = None
        result = write_tile_if_changed(self.store, tile_data, coord, format)
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
        from tilequeue.store import KeyFormatType
        from tilequeue.store import S3
        from tilequeue.store import S3TileKeyGenerator
        s3_client = self._make_stub_s3_client()
        tags = None
        tile_key_gen = S3TileKeyGenerator(
            key_format_type=KeyFormatType.hash_prefix)
        store = S3(s3_client, 'bucket', 'prefix', False, 60, None,
                   'public-read', tags, tile_key_gen)
        tile_data = 'data'
        from tilequeue.tile import deserialize_coord
        coord = deserialize_coord('14/1/2')
        from tilequeue.format import mvt_format
        store.write_tile(tile_data, coord, mvt_format)
        self.assertIsNone(store.s3_client.put_props.get('Tagging'))
        store.tags = dict(prefix='foo', run_id='bar')
        store.write_tile(tile_data, coord, mvt_format)
        self.assertEquals('prefix=foo&run_id=bar',
                          store.s3_client.put_props.get('Tagging'))


class _LogicalLog(object):
    """
    A logical time description of when things happened. Used for recording that
    one write to S3 happened before or after another.
    """

    def __init__(self):
        self.time = 0
        self.items = []

    def __call__(self, *args):
        self.items.append((self.time,) + args)
        self.time += 1


class _LoggingStore(object):
    """
    A mock store which doesn't store tiles, only logs calls to a logical log.
    """

    def __init__(self, name, log):
        self.name = name
        self.log = log

    def write_tile(self, tile_data, coord, format):
        self.log(self.name, 'write_tile', tile_data, coord, format)

    def read_tile(self, coord, format):
        self.log(self.name, 'read_tile', coord, format)
        return ""

    def delete_tiles(self, coords, format):
        self.log(self.name, 'delete_tiles', coords, format)
        return 0

    def list_tiles(self, format):
        self.log(self.name, 'list_tiles', format)
        return iter(())


class MultiStoreTest(unittest.TestCase):

    def test_multi_write(self):
        from tilequeue.format import json_format
        from tilequeue.store import MultiStore
        from ModestMaps.Core import Coordinate

        coord = Coordinate(zoom=0, column=0, row=0)

        log = _LogicalLog()
        s0 = _LoggingStore("s0", log)
        s1 = _LoggingStore("s1", log)
        m = MultiStore([s0, s1])

        m.write_tile("foo", coord, json_format)

        # multi store should write to both stores.
        self.assertEqual(
            log.items,
            [
                (0, "s0", "write_tile", "foo", coord, json_format),
                (1, "s1", "write_tile", "foo", coord, json_format),
            ])

    def test_multi_read(self):
        from tilequeue.format import json_format
        from tilequeue.store import MultiStore
        from ModestMaps.Core import Coordinate

        coord = Coordinate(zoom=0, column=0, row=0)

        log = _LogicalLog()
        s0 = _LoggingStore("s0", log)
        s1 = _LoggingStore("s1", log)
        m = MultiStore([s0, s1])

        m.read_tile(coord, json_format)

        # multi store should only read from final store.
        self.assertEqual(
            log.items,
            [
                (0, "s1", "read_tile", coord, json_format),
            ])
