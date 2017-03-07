from tilequeue.metatile import make_metatiles, extract_metatile
from tilequeue.format import json_format, zip_format, topojson_format
from ModestMaps.Core import Coordinate
import zipfile
import cStringIO as StringIO
import unittest


class TestMetatile(unittest.TestCase):

    def test_make_metatiles_single(self):
        json = "{\"json\":true}"
        tiles = [dict(tile=json, coord=Coordinate(0, 0, 0),
                      format=json_format, layer='all')]
        metatiles = make_metatiles(1, tiles)
        self.assertEqual(1, len(metatiles))
        self.assertEqual(Coordinate(0, 0, 0), metatiles[0]['coord'])
        self.assertEqual('all', metatiles[0]['layer'])
        self.assertEqual(zip_format, metatiles[0]['format'])
        buf = StringIO.StringIO(metatiles[0]['tile'])
        with zipfile.ZipFile(buf, mode='r') as z:
            self.assertEqual(json, z.open('0/0/0.json').read())

    def test_make_metatiles_multiple(self):
        json = "{\"json\":true}"
        tiles = [
            dict(tile=json, coord=Coordinate(0, 0, 0),
                 format=json_format, layer='all'),
            dict(tile=json, coord=Coordinate(0, 0, 0),
                 format=topojson_format, layer='all'),
        ]

        metatiles = make_metatiles(1, tiles)
        self.assertEqual(1, len(metatiles))
        self.assertEqual(Coordinate(0, 0, 0), metatiles[0]['coord'])
        self.assertEqual('all', metatiles[0]['layer'])
        self.assertEqual(zip_format, metatiles[0]['format'])
        buf = StringIO.StringIO(metatiles[0]['tile'])
        with zipfile.ZipFile(buf, mode='r') as z:
            self.assertEqual(json, z.open('0/0/0.json').read())
            self.assertEqual(json, z.open('0/0/0.topojson').read())

    def test_make_metatiles_multiple_coordinates(self):
        # checks that we can make a single metatile which contains multiple
        # coordinates. this is used for "512px" tiles as well as cutting out
        # z17+ tiles and storing them in the z16 (z15 if "512px") metatile.

        json = "{\"json\":true}"
        tiles = [
            # NOTE: coordinates are (y, x, z), possibly the most confusing
            # possible permutation.
            dict(tile=json, coord=Coordinate(123, 456, 17),
                 format=json_format, layer='all'),
            dict(tile=json, coord=Coordinate(123, 457, 17),
                 format=json_format, layer='all'),
        ]

        metatiles = make_metatiles(1, tiles)
        self.assertEqual(1, len(metatiles))
        meta = metatiles[0]

        # NOTE: Coordinate(y, x, z)
        coord = Coordinate(61, 228, 16)
        self.assertEqual(meta['coord'], coord)

        self.assertEqual('all', meta['layer'])
        self.assertEqual(zip_format, meta['format'])
        buf = StringIO.StringIO(meta['tile'])
        with zipfile.ZipFile(buf, mode='r') as z:
            self.assertEqual(json, z.open('1/0/1.json').read())
            self.assertEqual(json, z.open('1/1/1.json').read())

        # test extracting as well
        self.assertEqual(json, extract_metatile(
            buf, json_format, offset=Coordinate(zoom=1, column=0, row=1)))
        self.assertEqual(json, extract_metatile(
            buf, json_format, offset=Coordinate(zoom=1, column=1, row=1)))

    def test_extract_metatiles_single(self):
        json = "{\"json\":true}"
        tile = dict(tile=json, coord=Coordinate(0, 0, 0),
                    format=json_format, layer='all')
        metatiles = make_metatiles(1, [tile])
        self.assertEqual(1, len(metatiles))
        buf = StringIO.StringIO(metatiles[0]['tile'])
        extracted = extract_metatile(buf, json_format)
        self.assertEqual(json, extracted)

    def test_metatile_file_timing(self):
        from time import gmtime, time
        from tilequeue.metatile import metatiles_are_equal

        # tilequeue's "GET before PUT" optimisation relies on being able to
        # fetch a tile from S3 and compare it to the one that was just
        # generated. to do this, we should try to make the tiles as similar
        # as possible across multiple runs.

        json = "{\"json\":true}"
        tiles = [dict(tile=json, coord=Coordinate(0, 0, 0),
                      format=json_format, layer='all')]

        when_will_then_be_now = 10
        t = time()
        now = gmtime(t)[0:6]
        then = gmtime(t - when_will_then_be_now)[0:6]

        metatile_1 = make_metatiles(1, tiles, then)
        metatile_2 = make_metatiles(1, tiles, now)

        self.assertTrue(metatiles_are_equal(
            metatile_1[0]['tile'], metatile_2[0]['tile']))
