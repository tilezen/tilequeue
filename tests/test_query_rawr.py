import unittest


class TestGetTable(object):

    def __init__(self, tables):
        self.tables = tables

    def __call__(self, table_name):
        return self.tables.get(table_name, [])


class TestQueryRawr(unittest.TestCase):

    def _make(self, min_zoom_fn, props_fn, tables, tile_pyramid,
              layer_name='testlayer'):
        from tilequeue.query.common import LayerInfo
        from tilequeue.query.rawr import make_rawr_data_fetcher

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

    # TODO!
    # this isn't ready yet! need to implement OsmRawrLookup to use the RAWR
    # tile indexes.
    def _test_root_relation_id(self):
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
