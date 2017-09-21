import unittest


class TestQueryFixture(unittest.TestCase):

    def _make(self, rows, min_zoom_fn, props_fn, relations=[],
              layer_name='testlayer'):
        from tilequeue.query.common import LayerInfo
        from tilequeue.query.fixture import make_fixture_data_fetcher
        layers = {layer_name: LayerInfo(min_zoom_fn, props_fn)}
        return make_fixture_data_fetcher(layers, rows, relations=relations)

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
        from shapely.geometry import Point

        def min_zoom_fn(shape, props, fid, meta):
            return 0

        def _test(rels, expected_root_id):
            shape = Point(0, 0)
            rows = [
                (1, shape, {
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
