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
                        city1=self._city_bbox(1, 1, 2, 2),
                        city2=self._city_bbox(3, 3, 4, 4),
                    )
                )
            )
        )

    def _city_bbox(self, minx, miny, maxx, maxy):
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
        self.assertEqual((1, 1, 2, 2), city1.bbox.bounds)

        self.assertEqual('region1', city2.region)
        self.assertEqual('city2', city2.city)
        self.assertEqual((3, 3, 4, 4), city2.bbox.bounds)

    def test_city_bboxes(self):
        from json import dumps
        stub = self._generate_stub()
        from cStringIO import StringIO
        fp = StringIO(dumps(stub))
        results = self._call_fut(fp)
        self.assertEqual(2, len(results))
        results.sort(key=lambda x: x.city)

        from tilequeue.metro_extract import city_bboxes
        bboxes = city_bboxes(results)
        self.assertEqual(2, len(bboxes))
        bbox1, bbox2 = bboxes
        self.assertEqual((1, 1, 2, 2), bbox1.bounds)
        self.assertEqual((3, 3, 4, 4), bbox2.bounds)


class TestMetroExtractSpatialIndex(unittest.TestCase):

    def _call_fut(self, spatial_index, bbox):
        from tilequeue.metro_extract import bbox_in_metro_extract
        return bbox_in_metro_extract(spatial_index, bbox)

    def _instance(self, bounds):
        from tilequeue.metro_extract import create_spatial_index
        return create_spatial_index(bounds)

    def test_missing_lookup(self):
        from shapely.geometry import box
        si = self._instance([box(1, 1, 3, 3)])
        from tilequeue.metro_extract import bbox_in_metro_extract
        self.failIf(bbox_in_metro_extract(si, box(4, 4, 4, 4)))

    def test_valid_lookup(self):
        from shapely.geometry import box
        from tilequeue.metro_extract import bbox_in_metro_extract
        si = self._instance([box(1, 1, 3, 3)])
        self.failUnless(bbox_in_metro_extract(si, box(2, 2, 5, 5)))


class TestMetroExtractCoordToBbox(unittest.TestCase):

    def test_convert_coord(self):
        from tilequeue.metro_extract import coord_to_bbox
        from ModestMaps.Core import Coordinate
        coord = Coordinate(zoom=14, column=4824, row=6160)
        bbox = coord_to_bbox(coord)
        exp_bounds = (-74.00390625, 40.69729900863674,
                      -73.98193359375, 40.713955826286046)
        self.assertEqual(exp_bounds, bbox.bounds)


class StubSpatialIndex(object):

    def __init__(self, bbox):
        self.bbox = bbox

    def intersection(self, bounds):
        from shapely.geometry import box
        bbox = box(*bounds)
        return [1] if self.bbox.intersects(bbox) else []


class TestTileGenerationWithMetroExtractFilter(unittest.TestCase):

    def test_spatial_filter(self):
        from tilequeue.metro_extract import coord_to_bbox
        from tilequeue.metro_extract import make_metro_extract_predicate
        from tilequeue.seed import seed_tiles
        from ModestMaps.Core import Coordinate
        from itertools import ifilter
        zoom = 1
        coord = Coordinate(zoom=zoom, column=1, row=2)
        bbox = coord_to_bbox(coord)
        stub_spatial_index = StubSpatialIndex(bbox)
        tile_generator = seed_tiles(zoom_start=zoom, zoom_until=zoom)
        predicate = make_metro_extract_predicate(stub_spatial_index, zoom)
        tiles = list(ifilter(predicate, tile_generator))

        # expecting 2 tiles two match, because they match on the border
        self.assertEqual(2, len(tiles))


class TestTileGenerationFromMetroExtracts(unittest.TestCase):

    def _is_zoom(self, zoom):
        return lambda coord: zoom == coord.zoom

    def test_tiles_for_coord(self):
        from ModestMaps.Core import Coordinate
        from tilequeue.metro_extract import coord_to_bbox
        from tilequeue.metro_extract import tile_generator_for_bbox
        coord = Coordinate(1, 1, 1)
        bbox = coord_to_bbox(coord)
        tile_generator = tile_generator_for_bbox(bbox, 1, 1)
        tiles = list(tile_generator)
        self.assertEqual(1, len(tiles))

    def test_tiles_for_bbox_firsttile_two_zooms(self):
        from shapely.geometry import box
        from tilequeue.metro_extract import tile_generator_for_bbox
        bbox = box(-180, 0.1, -0.1, 85)
        tile_generator = tile_generator_for_bbox(bbox, 1, 2)
        tiles = list(tile_generator)
        self.assertEqual(5, len(tiles))
        self.assertEqual(1, len(filter(self._is_zoom(1), tiles)))
        self.assertEqual(4, len(filter(self._is_zoom(2), tiles)))

    def test_tiles_for_bbox_lasttile_two_zooms(self):
        from shapely.geometry import box
        from tilequeue.metro_extract import tile_generator_for_bbox
        bbox = box(0.1, -85, 180, -0.1)
        tile_generator = tile_generator_for_bbox(bbox, 1, 2)
        tiles = list(tile_generator)
        self.assertEqual(5, len(tiles))
        self.assertEqual(1, len(filter(self._is_zoom(1), tiles)))
        self.assertEqual(4, len(filter(self._is_zoom(2), tiles)))

    def test_tiles_for_two_bboxes_two_zooms(self):
        from shapely.geometry import box
        from tilequeue.metro_extract import tile_generator_for_bboxes
        bbox1 = box(-180, 0.1, -0.1, 85)
        bbox2 = box(0.1, -85, 180, -0.1)
        tile_generator = tile_generator_for_bboxes((bbox1, bbox2), 1, 2)
        tiles = list(tile_generator)
        self.assertEqual(10, len(tiles))
        self.assertEqual(2, len(filter(self._is_zoom(1), tiles)))
        self.assertEqual(8, len(filter(self._is_zoom(2), tiles)))
