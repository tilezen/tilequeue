import unittest


class FixtureTestCase(unittest.TestCase):

    def _make(self, rows, min_zoom_fn, props_fn, relations=[],
              layer_name='testlayer', label_placement_layers={}):
        from tilequeue.query.common import LayerInfo
        from tilequeue.query.fixture import make_fixture_data_fetcher
        layers = {layer_name: LayerInfo(min_zoom_fn, props_fn)}
        return make_fixture_data_fetcher(
            layers, rows, label_placement_layers=label_placement_layers,
            relations=relations)


class TestQueryFixture(FixtureTestCase):

    def test_query_simple(self):
        from shapely.geometry import Point

        def min_zoom_fn(shape, props, fid, meta):
            return 5

        shape = Point(0, 0)
        rows = [
            (0, shape, {})
        ]

        fetch = self._make(rows, min_zoom_fn, None)

        # first, check that it can get the original item back when both the
        # min zoom filter and geometry filter are okay.
        read_rows = fetch(5, (-1, -1, 1, 1))

        self.assertEquals(1, len(read_rows))
        read_row = read_rows[0]
        self.assertEquals(0, read_row.get('__id__'))
        # query processing code expects WKB bytes in the __geometry__ column
        self.assertEquals(shape.wkb, read_row.get('__geometry__'))
        self.assertEquals({'min_zoom': 5},
                          read_row.get('__testlayer_properties__'))

        # now, check that if the min zoom or geometry filters would exclude
        # the feature then it isn't returned.
        read_rows = fetch(4, (-1, -1, 1, 1))
        self.assertEquals(0, len(read_rows))

        read_rows = fetch(5, (1, 1, 2, 2))
        self.assertEquals(0, len(read_rows))

    def test_query_min_zoom_fraction(self):
        from shapely.geometry import Point

        def min_zoom_fn(shape, props, fid, meta):
            return 4.999

        shape = Point(0, 0)
        rows = [
            (0, shape, {})
        ]

        fetch = self._make(rows, min_zoom_fn, None)

        # check that the fractional zoom of 4.999 means that it's included in
        # the zoom 4 tile, but not the zoom 3 one.
        read_rows = fetch(4, (-1, -1, 1, 1))
        self.assertEquals(1, len(read_rows))

        read_rows = fetch(3, (-1, -1, 1, 1))
        self.assertEquals(0, len(read_rows))

    def test_query_past_max_zoom(self):
        from shapely.geometry import Point

        def min_zoom_fn(shape, props, fid, meta):
            return 20

        shape = Point(0, 0)
        rows = [
            (0, shape, {})
        ]

        fetch = self._make(rows, min_zoom_fn, None)

        # the min_zoom of 20 should mean that the feature is included at zoom
        # 16, even though 16<20, because 16 is the "max zoom" at which all the
        # data is included.
        read_rows = fetch(16, (-1, -1, 1, 1))
        self.assertEquals(1, len(read_rows))

        # but it should not exist at zoom 15
        read_rows = fetch(15, (-1, -1, 1, 1))
        self.assertEquals(0, len(read_rows))

    def test_root_relation_id(self):
        from shapely.geometry import Point, Polygon

        def min_zoom_fn(shape, props, fid, meta):
            return 0

        def _test(rels, expected_root_id, shape=None, feature_id=None):
            if shape is None:
                shape = Point(0, 0)
            if feature_id is None:
                feature_id = 1
            rows = [
                (feature_id, shape, {
                    'railway': 'station',
                    'name': 'Foo Station',
                }),
            ]

            fetch = self._make(rows, min_zoom_fn, None, relations=rels,
                               layer_name='pois')

            read_rows = fetch(16, (-1, -1, 1, 1))
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
            return {
                'id': id,
                'tags': ['type', 'site'],
                'way_off': way_off,
                'rel_off': rel_off,
                'parts': (nodes or []) + (ways or []) + (rels or []),
            }

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

        # test that a polygonal way feature is also traversed.
        polygon = Polygon([(-1, -1), (-1, 1), (1, 1), (1, -1), (-1, -1)])
        _test([_rel(2, ways=[1])], 2, shape=polygon)

        # test that a multipolygon relation feature is also traversed.
        _test([_rel(1), _rel(2, rels=[1])], 2, shape=polygon, feature_id=-1)

    def test_query_source(self):
        # check that the 'source' property is preserved for output features.
        from shapely.geometry import Point

        def min_zoom_fn(shape, props, fid, meta):
            return 5

        shape = Point(0, 0)
        rows = [
            (0, shape, {'source': 'testdata'})
        ]

        fetch = self._make(rows, min_zoom_fn, None)

        # first, check that it can get the original item back when both the
        # min zoom filter and geometry filter are okay.
        read_rows = fetch(5, (-1, -1, 1, 1))

        self.assertEquals(1, len(read_rows))
        read_row = read_rows[0]
        self.assertEquals(0, read_row.get('__id__'))
        # query processing code expects WKB bytes in the __geometry__ column
        self.assertEquals(shape.wkb, read_row.get('__geometry__'))
        self.assertEquals({'min_zoom': 5, 'source': 'testdata'},
                          read_row.get('__testlayer_properties__'))


class TestLabelPlacement(FixtureTestCase):

    def _test(self, layer_name, props):
        from ModestMaps.Core import Coordinate
        from tilequeue.tile import coord_to_mercator_bounds
        from shapely.geometry import box

        def min_zoom_fn(shape, props, fid, meta):
            return 0

        tile = Coordinate(zoom=15, column=0, row=0)
        bounds = coord_to_mercator_bounds(tile)
        shape = box(*bounds)

        rows = [
            (1, shape, props),
        ]

        label_placement_layers = {
            'polygon': set([layer_name]),
        }
        fetch = self._make(
            rows, min_zoom_fn, None, relations=[], layer_name=layer_name,
            label_placement_layers=label_placement_layers)

        read_rows = fetch(16, bounds)
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


class TestGeometryClipping(FixtureTestCase):

    def _test(self, layer_name, bounds, factor):
        from shapely.geometry import box

        def min_zoom_fn(shape, props, fid, meta):
            return 0

        boxwidth = bounds[2] - bounds[0]
        boxheight = bounds[3] - bounds[1]
        # make shape overlap the edges of the bounds. that way we can check to
        # see if the shape gets clipped.
        shape = box(bounds[0] - factor * boxwidth,
                    bounds[1] - factor * boxheight,
                    bounds[2] + factor * boxwidth,
                    bounds[3] + factor * boxheight)

        props = {'name': 'Foo'}

        rows = [
            (1, shape, props),
        ]

        fetch = self._make(
            rows, min_zoom_fn, None, relations=[], layer_name=layer_name)

        read_rows = fetch(16, bounds)
        self.assertEqual(1, len(read_rows))
        return read_rows[0]

    def test_normal_layer(self):
        from ModestMaps.Core import Coordinate
        from tilequeue.tile import coord_to_mercator_bounds
        from shapely import wkb

        tile = Coordinate(zoom=15, column=10, row=10)
        bounds = coord_to_mercator_bounds(tile)

        read_row = self._test('testlayer', bounds, 1.0)
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

        read_row = self._test('water', bounds, 1.0)
        clipped_shape = wkb.loads(read_row['__geometry__'])
        # for water layer, the geometry should be 10% larger than the tile
        # bounds.
        x_factor = ((clipped_shape.bounds[2] - clipped_shape.bounds[0]) /
                    (bounds[2] - bounds[0]))
        y_factor = ((clipped_shape.bounds[2] - clipped_shape.bounds[0]) /
                    (bounds[2] - bounds[0]))
        self.assertAlmostEqual(1.1, x_factor)
        self.assertAlmostEqual(1.1, y_factor)


class TestNameHandling(FixtureTestCase):

    def _test(self, input_layer_names, expected_layer_names):
        from shapely.geometry import Point
        from tilequeue.query.common import LayerInfo
        from tilequeue.query.fixture import make_fixture_data_fetcher
        from tilequeue.tile import coord_to_mercator_bounds
        from tilequeue.tile import mercator_point_to_coord

        def min_zoom_fn(shape, props, fid, meta):
            return 0

        def props_fn(shape, props, fid, meta):
            return {}

        shape = Point(0, 0)
        props = {'name': 'Foo', 'name:en': 'Bar'}

        rows = [
            (1, shape, props),
        ]

        layers = {}
        for name in input_layer_names:
            layers[name] = LayerInfo(min_zoom_fn, props_fn)
        fetch = make_fixture_data_fetcher(layers, rows)

        feature_coord = mercator_point_to_coord(16, shape.x, shape.y)
        read_rows = fetch(16, coord_to_mercator_bounds(feature_coord))
        self.assertEqual(1, len(read_rows))

        all_layer_names = set(expected_layer_names) | set(input_layer_names)
        for layer_name in all_layer_names:
            properties_name = '__%s_properties__' % layer_name
            self.assertTrue(properties_name in read_rows[0])
            for key in props.keys():
                actual_name = read_rows[0][properties_name].get(key)
                if layer_name in expected_layer_names:
                    expected_name = props.get(key)
                    self.assertEquals(
                        expected_name, actual_name,
                        msg=('expected=%r, actual=%r for key=%r'
                             % (expected_name, actual_name, key)))
                else:
                    # check the name doesn't appear anywhere else
                    self.assertEquals(
                        None, actual_name,
                        msg=('got actual=%r for key=%r, expected no value'
                             % (actual_name, key)))

    def test_name_single_layer(self):
        # in any oone of the pois, landuse or buildings layers, a name
        # by itself will be output in the same layer.
        for layer_name in ('pois', 'landuse', 'buildings'):
            self._test([layer_name], [layer_name])

    def test_precedence(self):
        # if the feature is in the pois layer, then that should get the name
        # and the other layers should not.
        self._test(['pois', 'landuse'], ['pois'])
        self._test(['pois', 'buildings'], ['pois'])
        self._test(['pois', 'landuse', 'buildings'], ['pois'])
        # otherwise, landuse should take precedence over buildings.
        self._test(['landuse', 'buildings'], ['landuse'])
