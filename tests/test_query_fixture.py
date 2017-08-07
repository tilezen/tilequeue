import unittest


class TestQueryFixture(unittest.TestCase):

    def _make(self, rows, min_zoom_fn, props_fn):
        from tilequeue.query.fixture import LayerInfo, DataFetcher
        layers = {'testlayer': LayerInfo(min_zoom_fn, props_fn)}
        return DataFetcher(layers, rows)

    def test_query_simple(self):
        from shapely.geometry import Point

        def min_zoom_fn(shape, props, fid, meta):
            return 5

        def props_fn(shape, props, fid, meta):
            return {'test': 'property'}

        shape = Point(0, 0)
        rows = [
            (0, shape, {})
        ]

        fetch = self._make(rows, min_zoom_fn, props_fn)

        # first, check that it can get the original item back when both the
        # min zoom filter and geometry filter are okay.
        read_rows = fetch(5, (-1, -1, 1, 1))

        self.assertEquals(1, len(read_rows))
        read_row = read_rows[0]
        self.assertEquals(0, read_row.get('__id__'))
        self.assertEquals(shape, read_row.get('__geometry__'))
        self.assertEquals({'test': 'property', 'min_zoom': 5},
                          read_row.get('__testlayer_properties__'))

        # now, check that if the min zoom or geometry filters would exclude
        # the feature then it isn't returned.
        read_rows = fetch(4, (-1, -1, 1, 1))
        self.assertEquals(0, len(read_rows))

        read_rows = fetch(5, (1, 1, 2, 2))
        self.assertEquals(0, len(read_rows))