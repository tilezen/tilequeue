import unittest


class RawrS3SinkTest(unittest.TestCase):

    def _make_stub_s3_client(self):
        class stub_s3_client(object):
            def put_object(self, **props):
                self.put_props = props
        return stub_s3_client()

    def _make_stub_rawr_tile(self):
        from raw_tiles.tile import Tile
        props = dict(
            all_formatted_data=[],
            tile=Tile(3, 2, 1),
        )
        return type('stub-rawr-tile', (), props)

    def test_tags(self):
        s3_client = self._make_stub_s3_client()
        from tilequeue.rawr import RawrS3Sink
        from tilequeue.store import KeyFormatType
        from tilequeue.store import S3TileKeyGenerator
        tile_key_gen = S3TileKeyGenerator(
            key_format_type=KeyFormatType.hash_prefix)
        sink = RawrS3Sink(
            s3_client, 'bucket', 'prefix', 'extension', tile_key_gen)
        rawr_tile = self._make_stub_rawr_tile()
        sink(rawr_tile)
        self.assertIsNone(sink.s3_client.put_props.get('Tagging'))
        sink.tags = dict(prefix='foo', runid='bar')
        sink(rawr_tile)
        self.assertEquals('prefix=foo&runid=bar',
                          sink.s3_client.put_props.get('Tagging'))


class RawrKeyTest(unittest.TestCase):
    def test_s3_path(self):
        from tilequeue.tile import deserialize_coord
        from tilequeue.store import KeyFormatType
        from tilequeue.store import S3TileKeyGenerator
        coord = deserialize_coord('10/1/2')
        prefix = '19851026'
        extension = 'zip'
        tile_key_gen = S3TileKeyGenerator(
                key_format_type=KeyFormatType.hash_prefix)
        key = tile_key_gen(prefix, coord, extension)
        self.assertEqual('c35b6/19851026/10/1/2.zip', key)
