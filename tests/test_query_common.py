import unittest


class TestCommon(unittest.TestCase):

    def test_parse_shape_types(self):
        from tilequeue.query.common import ShapeType

        def _test(expects, inputs):
            self.assertEquals(set(expects), ShapeType.parse_set(inputs))

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
        self.assertEquals(None, ShapeType.parse_set([]))

        # should throw KeyError if the name isn't recognised
        with self.assertRaises(KeyError):
            ShapeType.parse_set(['MegaPolygon'])
