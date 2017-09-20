import unittest


class TestGetTable(object):

    def __init__(self, tables):
        self.tables = tables

    def __call__(self, table_name):
        return self.tables.get(table_name, [])


class TestQueryRawr(unittest.TestCase):

    def _make(self, min_zoom_fn, props_fn, tables, tile_pyramid,
              layer_name='testlayer'):
        from tilequeue.query.fixture import LayerInfo
        from tilequeue.query.rawr import make_rawr_data_fetcher
        from tilequeue.tile import deg2num

        layers = {layer_name: LayerInfo(min_zoom_fn, props_fn)}
        return make_rawr_data_fetcher(layers, tables, tile_pyramid)

    def test_query_simple(self):
        from shapely.geometry import Point
        from tilequeue.query.rawr import TilePyramid
        from tilequeue.tile import coord_to_mercator_bounds
        from tilequeue.tile import mercator_point_to_coord

        feature_min_zoom = 11

        def min_zoom_fn(shape, props, fid, meta):
            return feature_min_zoom

        shape = Point(0, 0)
        # get_table(table_name) should return a generator of rows.
        tables = TestGetTable({
            'planet_osm_point': [(0, shape.wkb, {})],
        })

        zoom = 10
        max_zoom = zoom + 5
        coord = mercator_point_to_coord(zoom, shape.x, shape.y)
        tile_pyramid = TilePyramid(zoom, coord.column, coord.row, max_zoom)

        fetch = self._make(min_zoom_fn, None, tables, tile_pyramid)

        # first, check that it can get the original item back when both the
        # min zoom filter and geometry filter are okay.
        feature_coord = mercator_point_to_coord(
            feature_min_zoom, shape.x, shape.y)
        read_rows = fetch(
            feature_min_zoom, coord_to_mercator_bounds(feature_coord))

        self.assertEquals(1, len(read_rows))
        read_row = read_rows[0]
        self.assertEquals(0, read_row.get('__id__'))
        # query processing code expects WKB bytes in the __geometry__ column
        self.assertEquals(shape.wkb, read_row.get('__geometry__'))
        self.assertEquals({'min_zoom': 11},
                          read_row.get('__testlayer_properties__'))

        # now, check that if the min zoom or geometry filters would exclude
        # the feature then it isn't returned.
        read_rows = fetch(zoom, coord_to_mercator_bounds(coord))
        self.assertEquals(0, len(read_rows))

        read_rows = fetch(
            feature_min_zoom, coord_to_mercator_bounds(feature_coord.left()))
        self.assertEquals(0, len(read_rows))
