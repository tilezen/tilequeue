from tilequeue.metatile import make_metatiles
from tilequeue.format import json_format, zip_format, topojson_format
from ModestMaps.Core import Coordinate
import zipfile
import StringIO
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
