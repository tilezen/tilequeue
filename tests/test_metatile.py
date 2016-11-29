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

    def test_make_metatiles_multiple_coordinates(self):
        # we need to be able to handle this so that we can do "cut out"
        # overzoomed tiles at z>16.

        json = "{\"json\":true}"
        tiles = [
            dict(tile=json, coord=Coordinate(17, 123, 456),
                 format=json_format, layer='all'),
            dict(tile=json, coord=Coordinate(17, 123, 457),
                 format=json_format, layer='all'),
        ]

        metatiles = make_metatiles(1, tiles)
        self.assertEqual(2, len(metatiles))
        coords = set([Coordinate(17, 123, 456), Coordinate(17, 123, 457)])
        for meta in metatiles:
            self.assertTrue(meta['coord'] in coords)
            coords.remove(meta['coord'])

            self.assertEqual('all', meta['layer'])
            self.assertEqual(zip_format, meta['format'])
            buf = StringIO.StringIO(meta['tile'])
            with zipfile.ZipFile(buf, mode='r') as z:
                self.assertEqual(json, z.open('0/0/0.json').read())

        # check all coords were consumed
        self.assertEqual(0, len(coords))
