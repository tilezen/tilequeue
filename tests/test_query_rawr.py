import unittest


class TestGetTable(object):
    """
    Mocks the interface expected by raw_tiles.index.index.index_table,
    which provides "table lookup". Here, we just return static stuff
    previously set up in the test.
    """

    def __init__(self, tables, source='test'):
        from tilequeue.process import lookup_source, Source
        self.tables = tables
        # first look up source, in case it's a real one that we're testing.
        # if not, then set it to a test value
        self.source = lookup_source(source) or Source(source, source)
        assert isinstance(self.source, Source)

    def __call__(self, table_name):
        from tilequeue.query.common import Table
        return Table(self.source, self.tables.get(table_name, []))


class ConstantStorage(object):

    def __init__(self, tables):
        self.tables = tables

    def __call__(self, top_tile):
        return self.tables


class RawrTestCase(unittest.TestCase):
    """
    Base layer of the tests, providing a utility function to create a data
    fetcher with a set of mocked data.
    """

    def _make(self, min_zoom_fn, props_fn, tables, tile_pyramid,
              layer_name='testlayer', label_placement_layers={},
              min_z=10, max_z=16):
        from tilequeue.query.common import LayerInfo
        from tilequeue.query.rawr import make_rawr_data_fetcher

        layers = {layer_name: LayerInfo(min_zoom_fn, props_fn)}
        storage = ConstantStorage(tables)
        return make_rawr_data_fetcher(
            min_z, max_z, storage, layers,
            label_placement_layers=label_placement_layers)


# the call to DataFetcher.fetch_tiles wants a list of "data" dictionaries,
# each with a 'coord' key. this utility function just repackages a single
# coordinate in the way it wants.
def _wrap(coord):
    data = dict(coord=coord)
    return [data]


class TestQueryRawr(RawrTestCase):

    def test_query_simple(self):
        # just check that we can get back the mock data we put into a tile,
        # and that the indexing/fetching code respects the tile boundary and
        # min_zoom function.

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
        for fetcher, _ in fetch.fetch_tiles(_wrap(coord)):
            read_rows = fetcher(
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
        for fetcher, _ in fetch.fetch_tiles(_wrap(coord)):
            read_rows = fetcher(zoom, coord_to_mercator_bounds(coord))
        self.assertEquals(0, len(read_rows))

        for fetcher, _ in fetch.fetch_tiles(_wrap(coord)):
            read_rows = fetcher(
                feature_min_zoom, coord_to_mercator_bounds(
                    feature_coord.left()))
        self.assertEquals(0, len(read_rows))

    def test_query_min_zoom_fraction(self):
        # test that fractional min zooms are included in their "floor" zoom
        # tile. this is to allow over-zooming of a zoom N tile until N+1,
        # where the next zoom tile kicks in.

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

        fetcher = self._make(min_zoom_fn, None, tables, tile_pyramid)

        # check that the fractional zoom of 11.999 means that it's included in
        # the zoom 11 tile, but not the zoom 10 one.
        feature_coord = mercator_point_to_coord(11, shape.x, shape.y)
        for fetch, _ in fetcher.fetch_tiles(_wrap(coord)):
            read_rows = fetch(11, coord_to_mercator_bounds(feature_coord))
        self.assertEquals(1, len(read_rows))

        feature_coord = feature_coord.zoomBy(-1).container()
        for fetch, _ in fetcher.fetch_tiles(_wrap(coord)):
            read_rows = fetch(10, coord_to_mercator_bounds(feature_coord))
        self.assertEquals(0, len(read_rows))

    def test_query_past_max_zoom(self):
        # check that features with a min_zoom beyond the maximum zoom are still
        # included at the maximum zoom. since this is the last zoom level we
        # generate, it must include everything.

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
        for fetcher, _ in fetch.fetch_tiles(_wrap(coord)):
            read_rows = fetcher(16, coord_to_mercator_bounds(feature_coord))
        self.assertEquals(1, len(read_rows))

        # but it should not exist at zoom 15
        feature_coord = feature_coord.zoomBy(-1).container()
        for fetcher, _ in fetch.fetch_tiles(_wrap(coord)):
            read_rows = fetcher(10, coord_to_mercator_bounds(feature_coord))
        self.assertEquals(0, len(read_rows))

    def test_root_relation_id(self):
        # check the logic for finding a root relation ID for station complexes.

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
            for fetcher, _ in fetch.fetch_tiles(_wrap(coord)):
                read_rows = fetcher(16, coord_to_mercator_bounds(
                    feature_coord))
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

    def test_query_source(self):
        # check that the source is added to the result properties, and that
        # it overrides any existing source.

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
            'planet_osm_point': [(0, shape.wkb,
                                  {'source': 'originalrowsource'})],
        }, source='testingquerysource')

        zoom = 10
        max_zoom = zoom + 5
        coord = mercator_point_to_coord(zoom, shape.x, shape.y)
        tile_pyramid = TilePyramid(zoom, coord.column, coord.row, max_zoom)

        fetch = self._make(min_zoom_fn, None, tables, tile_pyramid)

        # first, check that it can get the original item back when both the
        # min zoom filter and geometry filter are okay.
        feature_coord = mercator_point_to_coord(
            feature_min_zoom, shape.x, shape.y)
        for fetcher, _ in fetch.fetch_tiles(_wrap(coord)):
            read_rows = fetcher(
                feature_min_zoom, coord_to_mercator_bounds(feature_coord))

        self.assertEquals(1, len(read_rows))
        read_row = read_rows[0]
        self.assertEquals(0, read_row.get('__id__'))
        # query processing code expects WKB bytes in the __geometry__ column
        self.assertEquals(shape.wkb, read_row.get('__geometry__'))
        self.assertEquals({'min_zoom': 11},
                          read_row.get('__testlayer_properties__'))
        self.assertEquals({'source': 'testingquerysource'},
                          read_row.get('__properties__'))


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

        for fetcher, _ in fetch.fetch_tiles(_wrap(top_tile)):
            read_rows = fetcher(tile.zoom, bounds)
        return read_rows

    def test_named_item(self):
        # check that a label is generated for features in label placement
        # layers.

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

        for fetcher, _ in fetch.fetch_tiles(_wrap(top_tile)):
            read_rows = fetcher(tile.zoom, bounds)
        self.assertEqual(1, len(read_rows))
        return read_rows[0]

    def test_normal_layer(self):
        # check that normal layer geometries are clipped to the bounding box of
        # the tile.

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
        # water layer should be clipped to the tile bounds expanded by 10%.

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


class TestNameHandling(RawrTestCase):

    def _test(self, input_layer_names, expected_layer_names):
        from shapely.geometry import Point
        from tilequeue.query.common import LayerInfo
        from tilequeue.query.rawr import make_rawr_data_fetcher
        from tilequeue.tile import coord_to_mercator_bounds
        from tilequeue.tile import mercator_point_to_coord

        top_zoom = 10
        max_zoom = top_zoom + 6

        def min_zoom_fn(shape, props, fid, meta):
            return top_zoom

        def props_fn(shape, props, fid, meta):
            return {}

        shape = Point(0, 0)
        props = {'name': 'Foo', 'name:en': 'Bar'}

        source = 'test'
        tables = TestGetTable({
            'planet_osm_point': [
                (1, shape.wkb, props),
            ],
        }, source)

        tile = mercator_point_to_coord(max_zoom, shape.x, shape.y)
        top_tile = tile.zoomTo(top_zoom).container()

        layers = {}
        for name in input_layer_names:
            layers[name] = LayerInfo(min_zoom_fn, props_fn)
        storage = ConstantStorage(tables)
        fetch = make_rawr_data_fetcher(
            top_zoom, max_zoom, storage, layers)

        for fetcher, _ in fetch.fetch_tiles(_wrap(top_tile)):
            read_rows = fetcher(tile.zoom, coord_to_mercator_bounds(tile))
        # the RAWR query goes over features multiple times because of the
        # indexing, so we can't rely on all the properties for one feature to
        # be all together in the same place. this loops over all the features,
        # checking that there's only really one of them and gathering together
        # all the __%s_properties__ from all the rows for further testing.
        all_props = {}
        for row in read_rows:
            self.assertEquals(1, row['__id__'])
            self.assertEquals(shape.wkb, row['__geometry__'])
            for key, val in row.items():
                if key.endswith('_properties__'):
                    self.assertFalse(key in all_props)
                    all_props[key] = val

        all_layer_names = set(expected_layer_names) | set(input_layer_names)
        for layer_name in all_layer_names:
            properties_name = '__%s_properties__' % layer_name
            self.assertTrue(properties_name in all_props)
            for key in props.keys():
                actual_name = all_props[properties_name].get(key)
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


class TestMeta(RawrTestCase):

    def test_meta_gate(self):
        # test that the meta is passed to the min zoom function, and that we
        # can use it to get information about the highways that a gate is
        # part of.

        from shapely.geometry import Point, LineString
        from tilequeue.query.rawr import TilePyramid
        from tilequeue.tile import coord_to_mercator_bounds
        from tilequeue.tile import mercator_point_to_coord

        feature_min_zoom = 11

        def min_zoom_fn(shape, props, fid, meta):
            self.assertIsNotNone(meta)

            # expect meta to have a source, which is a string name for the
            # source of the data.
            self.assertEquals('test', meta.source)

            # expect meta to have a list of relations, which is empty for this
            # test.
            self.assertEquals(0, len(meta.relations))

            # only do this for the node
            if fid == 0:
                # expect meta to have a list of ways, each of which is a (fid,
                # shape, props) tuple, of which only props is used.
                self.assertEquals(1, len(meta.ways))
                way_fid, way_shape, way_props = meta.ways[0]
                self.assertEquals(1, way_fid)
                self.assertEquals({'highway': 'secondary'}, way_props)

            # only set a min zoom for the node - this just simplifies the
            # checking later, as there'll only be one feature.
            return feature_min_zoom if fid == 0 else None

        shape = Point(0, 0)
        way_shape = LineString([[0, 0], [1, 1]])
        # get_table(table_name) should return a generator of rows.
        tables = TestGetTable({
            'planet_osm_point': [(0, shape.wkb, {'barrier': 'gate'})],
            'planet_osm_line': [(1, way_shape.wkb, {'highway': 'secondary'})],
            'planet_osm_ways': [(1, [0], ['highway', 'secondary'])],
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
        for fetcher, _ in fetch.fetch_tiles(_wrap(coord)):
            read_rows = fetcher(
                feature_min_zoom, coord_to_mercator_bounds(feature_coord))

        self.assertEquals(1, len(read_rows))
        read_row = read_rows[0]
        self.assertEquals(0, read_row.get('__id__'))
        # query processing code expects WKB bytes in the __geometry__ column
        self.assertEquals(shape.wkb, read_row.get('__geometry__'))
        self.assertEquals({'min_zoom': 11, 'barrier': 'gate'},
                          read_row.get('__testlayer_properties__'))

    def test_meta_route(self):
        # test that we can use meta in the min zoom function to find out which
        # route(s) a road is part of.

        from shapely.geometry import LineString
        from tilequeue.query.rawr import TilePyramid
        from tilequeue.tile import coord_to_mercator_bounds
        from tilequeue.tile import mercator_point_to_coord
        from tilequeue.query.common import deassoc

        feature_min_zoom = 11

        rel_tags = [
            'type', 'route',
            'route', 'road',
            'ref', '101',
        ]

        def min_zoom_fn(shape, props, fid, meta):
            self.assertIsNotNone(meta)

            # expect meta to have a source, which is a string name for the
            # source of the data.
            self.assertEquals('test', meta.source)

            # expect meta to have a list of ways, but empty for this test.
            self.assertEquals(0, len(meta.ways))

            # expect meta to have a list of relations, each of which is a dict
            # containing at least the key 'tags' mapped to a list of
            # alternating k, v suitable for passing into deassoc().
            self.assertEquals(1, len(meta.relations))
            rel = meta.relations[0]
            self.assertIsInstance(rel, dict)
            self.assertIn('tags', rel)
            self.assertEquals(deassoc(rel_tags), deassoc(rel['tags']))

            return feature_min_zoom

        shape = LineString([[0, 0], [1, 1]])
        # get_table(table_name) should return a generator of rows.
        tables = TestGetTable({
            'planet_osm_line': [(1, shape.wkb, {'highway': 'secondary'})],
            'planet_osm_rels': [(2, 0, 1, [1], [''], rel_tags)],
        })

        zoom = 10
        max_zoom = zoom + 5
        coord = mercator_point_to_coord(zoom, *shape.coords[0])
        tile_pyramid = TilePyramid(zoom, coord.column, coord.row, max_zoom)

        fetch = self._make(min_zoom_fn, None, tables, tile_pyramid)

        # first, check that it can get the original item back when both the
        # min zoom filter and geometry filter are okay.
        feature_coord = mercator_point_to_coord(
            feature_min_zoom, *shape.coords[0])
        for fetcher, _ in fetch.fetch_tiles(_wrap(coord)):
            read_rows = fetcher(
                feature_min_zoom, coord_to_mercator_bounds(feature_coord))

        self.assertEquals(1, len(read_rows))
        read_row = read_rows[0]
        self.assertEquals(1, read_row.get('__id__'))
        self.assertEquals({'min_zoom': 11, 'highway': 'secondary'},
                          read_row.get('__testlayer_properties__'))


class TestTileFootprint(unittest.TestCase):

    def test_single_tile(self):
        from tilequeue.query.rawr import _tiles
        from raw_tiles.tile import Tile
        from raw_tiles.util import bbox_for_tile

        tile = Tile(15, 5241, 12665)

        zoom = tile.z
        unpadded_bounds = bbox_for_tile(tile.z, tile.x, tile.y)
        tiles = _tiles(zoom, unpadded_bounds)

        self.assertEquals([tile], list(tiles))

    def test_multiple_tiles(self):
        from tilequeue.query.rawr import _tiles
        from raw_tiles.tile import Tile
        from raw_tiles.util import bbox_for_tile

        tile = Tile(15, 5241, 12665)

        # query at one zoom higher - should get 4 child tiles.
        zoom = tile.z + 1
        unpadded_bounds = bbox_for_tile(tile.z, tile.x, tile.y)
        tiles = list(_tiles(zoom, unpadded_bounds))

        self.assertEquals(4, len(tiles))
        for child in tiles:
            self.assertEquals(tile, child.parent())

    def test_corner_overlap(self):
        # a box around the corner of a tile should return the four
        # neighbours of that tile.
        from tilequeue.query.rawr import _tiles
        from raw_tiles.tile import Tile
        from raw_tiles.util import bbox_for_tile

        tile = Tile(15, 5241, 12665)

        zoom = tile.z
        tile_bbox = bbox_for_tile(tile.z, tile.x, tile.y)

        # extract the top left corner
        x = tile_bbox[0]
        y = tile_bbox[3]
        # make a small bounding box around that
        w = 10
        unpadded_bounds = (x - w, y - w,
                           x + w, y + w)
        tiles = set(_tiles(zoom, unpadded_bounds))

        expected = set()
        for dx in (0, -1):
            for dy in (0, -1):
                expected.add(Tile(tile.z, tile.x + dx, tile.y + dy))

        self.assertEquals(expected, tiles)
