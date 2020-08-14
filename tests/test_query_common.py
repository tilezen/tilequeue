import unittest


class TestCommon(unittest.TestCase):

    def test_parse_shape_types(self):
        from tilequeue.query.common import ShapeType

        def _test(expects, inputs):
            self.assertEqual(set(expects), ShapeType.parse_set(inputs))

        # basic types
        _test([ShapeType.point], ['point'])
        _test([ShapeType.line], ['line'])
        _test([ShapeType.polygon], ['polygon'])

        # should be case insensitive
        _test([ShapeType.point], ['Point'])
        _test([ShapeType.line], ['LINE'])
        _test([ShapeType.polygon], ['Polygon'])

        # should handle OGC-style names, including Multi*
        _test([ShapeType.point], ['MultiPoint'])
        _test([ShapeType.line], ['LineString'])
        _test([ShapeType.line], ['MultiLineString'])
        _test([ShapeType.polygon], ['MultiPolygon'])

        # should handle multiple, repeated names
        _test([ShapeType.point], ['Point', 'MultiPoint'])
        _test([
            ShapeType.point,
            ShapeType.line,
            ShapeType.polygon,
        ], [
            'Point', 'MultiPoint',
            'Line', 'LineString', 'MultiLineString',
            'Polygon', 'MultiPolygon'
        ])

        # should return None rather than an empty set.
        self.assertEqual(None, ShapeType.parse_set([]))

        # should throw KeyError if the name isn't recognised
        with self.assertRaises(KeyError):
            ShapeType.parse_set(['MegaPolygon'])

    def _route_layer_properties(self, route_tags):
        from tilequeue.query.common import layer_properties
        from shapely.geometry.linestring import LineString

        class FakeOsm(object):
            def __init__(self, test, tags):
                tag_list = []
                for k, v in tags.items():
                    tag_list.append(k)
                    tag_list.append(v)

                self.test = test
                self.tag_list = tag_list

            def relations_using_way(self, way_id):
                self.test.assertEqual(1, way_id)
                return [2]

            def relation(self, rel_id):
                from tilequeue.query.common import Relation
                self.test.assertEqual(2, rel_id)
                return Relation(dict(
                    id=2, way_off=0, rel_off=1,
                    tags=self.tag_list,
                    parts=[1]
                ))

        fid = 1
        shape = LineString([(0, 0), (1, 0)])
        props = {}
        layer_name = 'roads'
        zoom = 16
        osm = FakeOsm(self, route_tags)

        layer_props = layer_properties(
            fid, shape, props, layer_name, zoom, osm)

        return layer_props

    def test_business_and_spur_routes(self):
        # check that we extend the network specifier for US:I into
        # US:I:Business when there's a modifier set to business on the
        # route relation.

        layer_props = self._route_layer_properties(dict(
            type='route', route='road', network='US:I', ref='70',
            modifier='business'))

        self.assertEqual(['road', 'US:I:Business', '70'],
                          layer_props.get('mz_networks'))

    def test_business_and_spur_routes_existing(self):
        # check that, if the network is _already_ a US:I:Business, we don't
        # duplicate the suffix.

        layer_props = self._route_layer_properties(dict(
            type='route', route='road', network='US:I:Business', ref='70',
            modifier='business'))

        self.assertEqual(['road', 'US:I:Business', '70'],
                          layer_props.get('mz_networks'))

    def test_business_not_at_end(self):
        # check that, if the network contains 'Business', but it's not at the
        # end, then we still don't append it.

        layer_props = self._route_layer_properties(dict(
            type='route', route='road', network='US:I:Business:Loop', ref='70',
            modifier='business'))

        self.assertEqual(['road', 'US:I:Business:Loop', '70'],
                          layer_props.get('mz_networks'))
