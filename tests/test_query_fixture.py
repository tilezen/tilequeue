import unittest


class TestQueryFixture(unittest.TestCase):

    def _make(self, rows, min_zoom_fn, props_fn):
        from tilequeue.query.fixture import LayerInfo
        from tilequeue.query.fixture import make_fixture_data_fetcher
        layers = {'testlayer': LayerInfo(min_zoom_fn, props_fn)}
        return make_fixture_data_fetcher(layers, rows)

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
