import unittest


class TestGetTable(object):

    def __init__(self, tables):
        self.tables = tables

    def __call__(self, table_name):
        return self.tables.get(table_name, [])


class RawrTestCase(unittest.TestCase):

    def _make(self, min_zoom_fn, props_fn, tables, tile_pyramid,
              layer_name='testlayer', label_placement_layers={}):
        from tilequeue.query.common import LayerInfo
        from tilequeue.query.rawr import make_rawr_data_fetcher

        layers = {layer_name: LayerInfo(min_zoom_fn, props_fn)}
        return make_rawr_data_fetcher(
            layers, tables, tile_pyramid,
            label_placement_layers=label_placement_layers)


class TestQueryRawr(RawrTestCase):

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

    def test_query_min_zoom_fraction(self):
        from shapely.geometry import Point
        from tilequeue.query.rawr import TilePyramid
        from tilequeue.tile import coord_to_mercator_bounds
        from tilequeue.tile import mercator_point_to_coord

        def min_zoom_fn(shape, props, fid, meta):
            return 11.999

        shape = Point(0, 0)
        tables = TestGetTable({
            'planet_osm_point': [(0, shape.wkb, {})]
        })

        zoom = 10
        max_zoom = zoom + 5
        coord = mercator_point_to_coord(zoom, shape.x, shape.y)
        tile_pyramid = TilePyramid(zoom, coord.column, coord.row, max_zoom)

        fetch = self._make(min_zoom_fn, None, tables, tile_pyramid)

        # check that the fractional zoom of 11.999 means that it's included in
        # the zoom 11 tile, but not the zoom 10 one.
        feature_coord = mercator_point_to_coord(11, shape.x, shape.y)
        read_rows = fetch(11, coord_to_mercator_bounds(feature_coord))
        self.assertEquals(1, len(read_rows))

        feature_coord = feature_coord.zoomBy(-1).container()
        read_rows = fetch(10, coord_to_mercator_bounds(feature_coord))
        self.assertEquals(0, len(read_rows))

    def test_query_past_max_zoom(self):
        from shapely.geometry import Point
        from tilequeue.query.rawr import TilePyramid
        from tilequeue.tile import coord_to_mercator_bounds
        from tilequeue.tile import mercator_point_to_coord

        def min_zoom_fn(shape, props, fid, meta):
            return 20

        shape = Point(0, 0)
        tables = TestGetTable({
            'planet_osm_point': [(0, shape.wkb, {})]
        })

        zoom = 10
        max_zoom = zoom + 6
        coord = mercator_point_to_coord(zoom, shape.x, shape.y)
        tile_pyramid = TilePyramid(zoom, coord.column, coord.row, max_zoom)

        fetch = self._make(min_zoom_fn, None, tables, tile_pyramid)

        # the min_zoom of 20 should mean that the feature is included at zoom
        # 16, even though 16<20, because 16 is the "max zoom" at which all the
        # data is included.
        feature_coord = mercator_point_to_coord(16, shape.x, shape.y)
        read_rows = fetch(16, coord_to_mercator_bounds(feature_coord))
        self.assertEquals(1, len(read_rows))

        # but it should not exist at zoom 15
        feature_coord = feature_coord.zoomBy(-1).container()
        read_rows = fetch(10, coord_to_mercator_bounds(feature_coord))
        self.assertEquals(0, len(read_rows))

    def test_root_relation_id(self):
        from shapely.geometry import Point
        from tilequeue.query.rawr import TilePyramid
        from tilequeue.tile import coord_to_mercator_bounds
        from tilequeue.tile import mercator_point_to_coord

        def min_zoom_fn(shape, props, fid, meta):
            return 10

        def _test(rels, expected_root_id):
            shape = Point(0, 0)
            props = {
                'railway': 'station',
                'name': 'Foo Station',
            }
            tables = TestGetTable({
                'planet_osm_point': [(1, shape.wkb, props)],
                'planet_osm_rels': rels,
            })

            zoom = 10
            max_zoom = zoom + 6
            coord = mercator_point_to_coord(zoom, shape.x, shape.y)
            tile_pyramid = TilePyramid(zoom, coord.column, coord.row, max_zoom)

            fetch = self._make(min_zoom_fn, None, tables, tile_pyramid,
                               layer_name='pois')

            feature_coord = mercator_point_to_coord(16, shape.x, shape.y)
            read_rows = fetch(16, coord_to_mercator_bounds(feature_coord))
            self.assertEquals(1, len(read_rows))

            props = read_rows[0]['__pois_properties__']
            self.assertEquals(expected_root_id,
                              props.get('mz_transit_root_relation_id'))

        # the fixture code expects "raw" relations as if they come straight
        # from osm2pgsql. the structure is a little cumbersome, so this
        # utility function constructs it from a more readable function call.
        def _rel(id, nodes=None, ways=None, rels=None):
            way_off = len(nodes) if nodes else 0
            rel_off = way_off + (len(ways) if ways else 0)
            parts = (nodes or []) + (ways or []) + (rels or [])
            members = [""] * len(parts)
            tags = ['type', 'site']
            return (id, way_off, rel_off, parts, members, tags)

        # one level of relations - this one directly contains the station
        # node.
        _test([_rel(2, nodes=[1])], 2)

        # two levels of relations r3 contains r2 contains n1.
        _test([_rel(2, nodes=[1]), _rel(3, rels=[2])], 3)

        # asymmetric diamond pattern. r2 and r3 both contain n1, r4 contains
        # r3 and r5 contains both r4 and r2, making it the "top" relation.
        _test([
            _rel(2, nodes=[1]),
            _rel(3, nodes=[1]),
            _rel(4, rels=[3]),
            _rel(5, rels=[2, 4]),
        ], 5)


class TestLabelPlacement(RawrTestCase):

    def _test(self, layer_name, props):
        from ModestMaps.Core import Coordinate
        from tilequeue.tile import coord_to_mercator_bounds
        from shapely.geometry import box
        from tilequeue.query.rawr import TilePyramid

        top_zoom = 10
        max_zoom = top_zoom + 6

        def min_zoom_fn(shape, props, fid, meta):
            return top_zoom

        tile = Coordinate(zoom=15, column=0, row=0)
        top_tile = tile.zoomTo(top_zoom).container()
        tile_pyramid = TilePyramid(
            top_zoom, top_tile.column, top_tile.row, max_zoom)

        bounds = coord_to_mercator_bounds(tile)
        shape = box(*bounds)
        tables = TestGetTable({
            'planet_osm_polygon': [
                (1, shape.wkb, props),
            ]
        })

        label_placement_layers = {
            'polygon': set([layer_name]),
        }
        fetch = self._make(
            min_zoom_fn, None, tables, tile_pyramid, layer_name=layer_name,
            label_placement_layers=label_placement_layers)

        read_rows = fetch(tile.zoom, bounds)
        return read_rows

    def test_named_item(self):
        from shapely import wkb

        layer_name = 'testlayer'
        read_rows = self._test(layer_name, {'name': 'Foo'})

        self.assertEquals(1, len(read_rows))

        label_prop = '__label__'
        self.assertTrue(label_prop in read_rows[0])
        point = wkb.loads(read_rows[0][label_prop])
        self.assertEqual(point.geom_type, 'Point')


class TestGeometryClipping(RawrTestCase):

    def _test(self, layer_name, tile, factor):
        from shapely.geometry import box
        from tilequeue.query.rawr import TilePyramid
        from tilequeue.tile import coord_to_mercator_bounds

        top_zoom = 10
        max_zoom = top_zoom + 6

        def min_zoom_fn(shape, props, fid, meta):
            return top_zoom

        top_tile = tile.zoomTo(top_zoom).container()
        tile_pyramid = TilePyramid(
            top_zoom, top_tile.column, top_tile.row, max_zoom)

        bounds = coord_to_mercator_bounds(tile)
        boxwidth = bounds[2] - bounds[0]
        boxheight = bounds[3] - bounds[1]
        # make shape overlap the edges of the bounds. that way we can check to
        # see if the shape gets clipped.
        shape = box(bounds[0] - factor * boxwidth,
                    bounds[1] - factor * boxheight,
                    bounds[2] + factor * boxwidth,
                    bounds[3] + factor * boxheight)

        props = {'name': 'Foo'}

        tables = TestGetTable({
            'planet_osm_polygon': [
                (1, shape.wkb, props),
            ],
        })

        fetch = self._make(
            min_zoom_fn, None, tables, tile_pyramid, layer_name=layer_name)

        read_rows = fetch(tile.zoom, bounds)
        self.assertEqual(1, len(read_rows))
        return read_rows[0]

    def test_normal_layer(self):
        from ModestMaps.Core import Coordinate
        from tilequeue.tile import coord_to_mercator_bounds
        from shapely import wkb

        tile = Coordinate(zoom=15, column=10, row=10)
        bounds = coord_to_mercator_bounds(tile)

        read_row = self._test('testlayer', tile, 1.0)
        clipped_shape = wkb.loads(read_row['__geometry__'])
        # for normal layers, clipped shape is inside the bounds of the tile.
        x_factor = ((clipped_shape.bounds[2] - clipped_shape.bounds[0]) /
                    (bounds[2] - bounds[0]))
        y_factor = ((clipped_shape.bounds[2] - clipped_shape.bounds[0]) /
                    (bounds[2] - bounds[0]))
        self.assertAlmostEqual(1.0, x_factor)
        self.assertAlmostEqual(1.0, y_factor)

    def test_water_layer(self):
        # water layer should be expanded by 10% on each side.
        from ModestMaps.Core import Coordinate
        from tilequeue.tile import coord_to_mercator_bounds
        from shapely import wkb

        tile = Coordinate(zoom=15, column=10, row=10)
        bounds = coord_to_mercator_bounds(tile)

        read_row = self._test('water', tile, 1.0)
        clipped_shape = wkb.loads(read_row['__geometry__'])
        # for water layer, the geometry should be 10% larger than the tile
        # bounds.
        x_factor = ((clipped_shape.bounds[2] - clipped_shape.bounds[0]) /
                    (bounds[2] - bounds[0]))
        y_factor = ((clipped_shape.bounds[2] - clipped_shape.bounds[0]) /
                    (bounds[2] - bounds[0]))
        self.assertAlmostEqual(1.1, x_factor)
        self.assertAlmostEqual(1.1, y_factor)
