import unittest


class TestMetroExtractParse(unittest.TestCase):

    def _call_fut(self, fp):
        from tilequeue.metro_extract import parse_metro_extract
        return parse_metro_extract(fp)

    def test_invalid_json(self):
        from cStringIO import StringIO
        from tilequeue.metro_extract import MetroExtractParseError
        fp = StringIO('{"foo": "bar"}')
        try:
            self._call_fut(fp)
        except MetroExtractParseError:
            # expecting error to be raised
            pass
        else:
            self.fail('Expected MetroExtractParseError to be raised')

    def _generate_stub(self):
        return dict(
            regions=dict(
                region1=dict(
                    cities=dict(
                        city1=self._city_bounds(1, 1, 2, 2),
                        city2=self._city_bounds(3, 3, 4, 4),
                    )
                )
            )
        )

    def _city_bounds(self, minx, miny, maxx, maxy):
        return dict(
            bbox=dict(
                left=str(minx),
                right=str(maxx),
                top=str(maxy),
                bottom=str(miny),
            )
        )

    def test_valid_parse(self):
        from json import dumps
        stub = self._generate_stub()
        from cStringIO import StringIO
        fp = StringIO(dumps(stub))
        results = self._call_fut(fp)
        self.assertEqual(2, len(results))
        results.sort(key=lambda x: x.city)
        city1, city2 = results

        self.assertEqual('region1', city1.region)
        self.assertEqual('city1', city1.city)
        self.assertEqual((1, 1, 2, 2), city1.bounds)

        self.assertEqual('region1', city2.region)
        self.assertEqual('city2', city2.city)
        self.assertEqual((3, 3, 4, 4), city2.bounds)

    def test_city_bounds(self):
        from json import dumps
        stub = self._generate_stub()
        from cStringIO import StringIO
        fp = StringIO(dumps(stub))
        results = self._call_fut(fp)
        self.assertEqual(2, len(results))
        results.sort(key=lambda x: x.city)

        from tilequeue.metro_extract import city_bounds
        bounds = city_bounds(results)
        self.assertEqual(2, len(bounds))
        bounds1, bounds2 = bounds
        self.assertEqual((1, 1, 2, 2), bounds1)
        self.assertEqual((3, 3, 4, 4), bounds2)


class TestMetroExtractCoordToBounds(unittest.TestCase):

    def test_convert_coord(self):
        from tilequeue.metro_extract import coord_to_bounds
        from ModestMaps.Core import Coordinate
        coord = Coordinate(zoom=14, column=4824, row=6160)
        bounds = coord_to_bounds(coord)
        exp_bounds = (-74.00390625, 40.69729900863674,
                      -73.98193359375, 40.713955826286046)
        self.assertEqual(exp_bounds, bounds)


class TestTileGenerationFromMetroExtracts(unittest.TestCase):

    def _is_zoom(self, zoom):
        return lambda coord: zoom == coord.zoom

    def test_tiles_for_coord(self):
        from ModestMaps.Core import Coordinate
        from tilequeue.metro_extract import coord_to_bounds
        from tilequeue.metro_extract import tile_generator_for_single_bounds
        coord = Coordinate(1, 1, 1)
        bounds = coord_to_bounds(coord)
        tile_generator = tile_generator_for_single_bounds(bounds, 1, 1)
        tiles = list(tile_generator)
        self.assertEqual(1, len(tiles))

    def test_tiles_for_bounds_firsttile_two_zooms(self):
        from tilequeue.metro_extract import tile_generator_for_single_bounds
        bounds = (-180, 0.1, -0.1, 85)
        tile_generator = tile_generator_for_single_bounds(bounds, 1, 2)
        tiles = list(tile_generator)
        self.assertEqual(5, len(tiles))
        self.assertEqual(1, len(filter(self._is_zoom(1), tiles)))
        self.assertEqual(4, len(filter(self._is_zoom(2), tiles)))

    def test_tiles_for_bounds_lasttile_two_zooms(self):
        from tilequeue.metro_extract import tile_generator_for_single_bounds
        bounds = (0.1, -85, 180, -0.1)
        tile_generator = tile_generator_for_single_bounds(bounds, 1, 2)
        tiles = list(tile_generator)
        self.assertEqual(5, len(tiles))
        self.assertEqual(1, len(filter(self._is_zoom(1), tiles)))
        self.assertEqual(4, len(filter(self._is_zoom(2), tiles)))

    def test_tiles_for_two_bounds_two_zooms(self):
        from tilequeue.metro_extract import tile_generator_for_multiple_bounds
        bounds1 = (-180, 0.1, -0.1, 85)
        bounds2 = (0.1, -85, 180, -0.1)
        tile_generator = tile_generator_for_multiple_bounds(
            (bounds1, bounds2), 1, 2)
        tiles = list(tile_generator)
        self.assertEqual(10, len(tiles))
        self.assertEqual(2, len(filter(self._is_zoom(1), tiles)))
        self.assertEqual(8, len(filter(self._is_zoom(2), tiles)))
