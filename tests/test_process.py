from ModestMaps.Core import Coordinate
import unittest

from tilequeue.process import remove_wrong_zoomed_features


class TestProcess(unittest.TestCase):

    def _make_json_tiles(
            self, coord, post_process_data={}, db_features=[], cut_coords=[],
            buffer_cfg={}):
        from tilequeue.process import process_coord
        from tilequeue.tile import coord_to_mercator_bounds
        from tilequeue.format import json_format

        unpadded_bounds = coord_to_mercator_bounds(coord)
        feature_layers = [dict(
            layer_datum=dict(
                name='fake_layer',
                geometry_types=['Point'],
                transform_fn_names=[],
                sort_fn_name=None,
                is_clipped=False
            ),
            padded_bounds=dict(point=unpadded_bounds),
            features=db_features
        )]
        formats = [json_format]

        def _test_output_fn(*args):
            return dict(foo='bar', min_zoom=0)

        output_calc_mapping = dict(fake_layer=_test_output_fn)
        all_coords = [coord] + cut_coords
        tiles, extra = process_coord(
            coord, coord.zoom, feature_layers, post_process_data, formats,
            unpadded_bounds, all_coords, buffer_cfg, output_calc_mapping)

        return tiles

    def _make_json_tile(self, coord, **kwargs):
        from tilequeue.format import json_format
        import json

        tiles = self._make_json_tiles(coord, **kwargs)

        self.assertEqual(1, len(tiles))
        tile = tiles[0]
        self.assertEqual(coord, tile['coord'])
        self.assertEqual(json_format, tile['format'])
        self.assertEqual('all', tile['layer'])
        return json.loads(tile['tile'])

    def test_process_coord_empty(self):
        from tilequeue.process import process_coord
        from tilequeue.tile import coord_to_mercator_bounds

        coord = Coordinate(0, 0, 0)
        feature_layers = []
        post_process_data = {}
        formats = []
        unpadded_bounds = coord_to_mercator_bounds(coord)
        cut_coords = [coord]
        buffer_cfg = {}

        def _test_output_fn(*args):
            return dict(foo='bar')

        output_calc_mapping = dict(fake_layer=_test_output_fn)
        tiles, extra = process_coord(
            coord, coord.zoom, feature_layers, post_process_data, formats,
            unpadded_bounds, cut_coords, buffer_cfg, output_calc_mapping)

        self.assertEqual([], tiles)
        self.assertEqual({'size': {}}, extra)

    def test_process_coord_single_layer(self):
        self.maxDiff = 10000

        def _check(coord, post_process_name, should_have_point):
            features = [dict(
                __id__=1,
                # this is a point at (90, 40) in mercator
                __geometry__='\x01\x01\x00\x00\x00\xd7\xa3pE\xf8\x1b' + \
                'cA\x1f\x85\xeb\x91\xe5\x8fRA',
                __properties__=dict(foo='bar'),
            )]
            post_process_data = [
                dict(
                    fn_name=('tests.test_process.%s' % post_process_name),
                    params={},
                    resources={}
                )
            ]
            json_data = {
                'type': 'FeatureCollection',
                'features': []
            }
            if should_have_point:
                json_data['features'] = [{
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [90.0, 40.0]
                    },
                    'type': 'Feature',
                    'properties': {
                        'foo': 'bar',
                        'min_zoom': 0,
                        'tags': dict(foo='bar'),
                    },
                    'id': 1
                }]

            tile = self._make_json_tile(
                coord, post_process_data=post_process_data,
                db_features=features)
            self.assertEqual(json_data, tile)

        _check(Coordinate(0, 0, 0), '_only_zoom_zero', True)
        _check(Coordinate(0, 0, 0), '_only_zoom_one', False)
        _check(Coordinate(0, 1, 1), '_only_zoom_one', True)
        _check(Coordinate(0, 1, 1), '_only_zoom_zero', False)

    def test_process_coord_cut_coords(self):
        import json

        self.maxDiff = 10000

        coord = Coordinate(0, 0, 0)
        cut_coord = Coordinate(0, 1, 1)

        features = [dict(
            __id__=1,
            # this is a point at (90, 40) in mercator
            __geometry__='\x01\x01\x00\x00\x00\xd7\xa3pE\xf8\x1b' + \
            'cA\x1f\x85\xeb\x91\xe5\x8fRA',
            __properties__=dict(foo='bar'),
        )]
        post_process_data = [
            dict(
                fn_name='tests.test_process._only_zoom_zero',
                params={},
                resources={}
            )
        ]

        tiles = self._make_json_tiles(
            coord, post_process_data=post_process_data,
            db_features=features, cut_coords=[cut_coord])

        tiles_0 = [t for t in tiles if t['coord'] == coord]
        self.assertEqual(1, len(tiles_0))
        tile_0 = json.loads(tiles_0[0]['tile'])
        self.assertEqual(1, len(tile_0['features']))
        self.assertEqual([90.0, 40.0],
                         tile_0['features'][0]['geometry']['coordinates'])

        # cut coord at zoom 1 is currently implemented as being re-processed
        # from the original feature data, so will run the post-processor stuff
        # at a different zoom level, and drop the point.
        tiles_1 = [t for t in tiles if t['coord'] == cut_coord]
        self.assertEqual(1, len(tiles_1))
        tile_1 = json.loads(tiles_1[0]['tile'])
        self.assertEqual(1, len(tile_1['features']))
        self.assertEqual([90.0, 40.0],
                         tile_1['features'][0]['geometry']['coordinates'])

    def test_cut_coord_exclusive(self):
        # test that cut coords are the only ones in the response, and that
        # the coordinate itself can be omitted.
        from tilequeue.process import process_coord
        from tilequeue.tile import coord_to_mercator_bounds
        from tilequeue.format import json_format

        coord = Coordinate(0, 0, 0)
        db_features = []
        cut_coords = [
            Coordinate(zoom=1, column=0, row=0),
            Coordinate(zoom=1, column=1, row=0),
            Coordinate(zoom=1, column=0, row=1),
        ]
        buffer_cfg = {}
        post_process_data = {}

        unpadded_bounds = coord_to_mercator_bounds(coord)
        feature_layers = [dict(
            layer_datum=dict(
                name='fake_layer',
                geometry_types=['Point'],
                transform_fn_names=[],
                sort_fn_name=None,
                is_clipped=False
            ),
            padded_bounds=dict(point=unpadded_bounds),
            features=db_features
        )]
        formats = [json_format]

        def _test_output_fn(*args):
            return dict(foo='bar', min_zoom=0)

        output_calc_mapping = dict(fake_layer=_test_output_fn)
        tiles, extra = process_coord(
            coord, coord.zoom, feature_layers, post_process_data, formats,
            unpadded_bounds, cut_coords, buffer_cfg, output_calc_mapping)

        self.assertEqual(len(cut_coords), len(tiles))
        self.assertNotIn(coord, [t['coord'] for t in tiles])


class TestCalculateCutZooms(unittest.TestCase):

    def test_max_zoom(self):
        from tilequeue.process import calculate_sizes_by_zoom
        from tilequeue.tile import metatile_zoom_from_size

        def _calc(metatile_size, tile_sizes, max_zoom):
            metatile_zoom = metatile_zoom_from_size(metatile_size)
            coord = Coordinate(zoom=max_zoom - metatile_zoom, row=0, column=0)

            return calculate_sizes_by_zoom(
                coord, metatile_zoom, tile_sizes, max_zoom - metatile_zoom)

        # sweep max zoom to check the output is the same nominal max zoom.
        self.assertEqual({16: [256]}, _calc(8, [256], 16))
        self.assertEqual({15: [256]}, _calc(8, [256], 15))
        self.assertEqual({14: [256]}, _calc(8, [256], 14))

        # check we get 256 tiles as well as 512 at max zoom, even when the
        # configured tile size is only 512.
        self.assertEqual({16: [512, 256]}, _calc(8, [512], 16))

        # we should get _both_ 512 and 256 tiles if we've configured to only
        # have 1024 tiles at mid zooms.
        self.assertEqual({16: [1024, 512, 256]}, _calc(8, [1024], 16))

    def test_only_overzoom_at_max_zoom(self):
        from tilequeue.process import calculate_sizes_by_zoom

        # constants
        metatile_zoom = 3
        cfg_tile_sizes = [512]
        max_zoom = 13

        # zoom 13 (nominal 16) tile should contain everything
        sizes = calculate_sizes_by_zoom(
            Coordinate(zoom=13, column=0, row=0),
            metatile_zoom, cfg_tile_sizes, max_zoom)
        self.assertEquals(sizes, {16: [512, 256]})

        # zoom 12 (nominal 15) should be 512 only
        sizes = calculate_sizes_by_zoom(
            Coordinate(zoom=12, column=0, row=0),
            metatile_zoom, cfg_tile_sizes, max_zoom)
        self.assertEquals(sizes, {15: [512]})

    def test_mid_zoom(self):
        from tilequeue.process import calculate_sizes_by_zoom
        from tilequeue.tile import metatile_zoom_from_size

        tile_sizes = [512]
        metatile_size = 8
        metatile_zoom = metatile_zoom_from_size(metatile_size)
        max_zoom = 16 - metatile_zoom

        for zoom in range(1, max_zoom - metatile_zoom):
            coord = Coordinate(zoom=zoom, row=0, column=0)
            sizes_by_zoom = calculate_sizes_by_zoom(
                coord, metatile_zoom, tile_sizes, max_zoom)
            nominal_zoom = zoom + metatile_zoom
            self.assertEqual({nominal_zoom: tile_sizes}, sizes_by_zoom)

    def test_zoom_zero(self):
        from tilequeue.process import calculate_sizes_by_zoom
        from tilequeue.tile import metatile_zoom_from_size

        def _calc(metatile_size, tile_sizes):
            coord = Coordinate(zoom=0, row=0, column=0)
            metatile_zoom = metatile_zoom_from_size(metatile_size)
            max_zoom = 16 - metatile_zoom

            return calculate_sizes_by_zoom(
                coord, metatile_zoom, tile_sizes, max_zoom)

        # for an 8x8 metatile configured for 512 tiles, then by default we
        # would get a 0/0/0 metatile with 4x4 nominal zoom 3 512px tiles. we
        # want to extend that "upwards" towards nominal zoom 0, so we should
        # also get: 2x2 nominal zoom 2 512px tiles plus 1x1 nominal zoom 1
        # 512px tile.
        self.assertEqual({
            1: [512],
            2: [512],
            3: [512],
        }, _calc(8, [512]))

        # when we do the same with 256px tiles, we should get a nominal zoom
        # zero tile.
        self.assertEqual({
            0: [256],
            1: [256],
            2: [256],
            3: [256],
        }, _calc(8, [256]))

        # when we configure both 256 and 512px tiles, we should only get the
        # 256 ones at the largest nominal zoom.
        self.assertEqual({
            1: [512],
            2: [512],
            3: [512, 256],
        }, _calc(8, [512, 256]))

        self.assertEqual({
            2: [1024],
            3: [1024, 512, 256],
        }, _calc(8, [1024, 512, 256]))

        # with a smaller metatile, we just get fewer nominal zooms in the range
        # inside the metatile.
        self.assertEqual({
            1: [512],
            2: [512, 256],
        }, _calc(4, [512, 256]))

        # with a 1x1 metatile (i.e: not really a metatile) then we just get
        # the configured size.
        for z in xrange(0, 3):
            meta_sz = 1 << z
            tile_sz = 256 * meta_sz
            self.assertEqual({z: [tile_sz]}, _calc(meta_sz, [tile_sz]))


class TestMetatileChildrenWithSize(unittest.TestCase):

    def test_single_tile(self):
        from tilequeue.process import metatile_children_with_size
        coord = Coordinate(zoom=0, column=0, row=0)
        result = metatile_children_with_size(coord, 0, 0, 256)
        self.assertEqual([coord], result)

    def test_2x2_tile(self):
        from tilequeue.process import metatile_children_with_size
        coord = Coordinate(zoom=0, column=0, row=0)
        result = metatile_children_with_size(coord, 1, 1, 256)
        self.assertEqual(set([
            Coordinate(zoom=1, column=0, row=0),
            Coordinate(zoom=1, column=1, row=0),
            Coordinate(zoom=1, column=0, row=1),
            Coordinate(zoom=1, column=1, row=1),
        ]), set(result))

    def test_8x8_512_tile(self):
        from tilequeue.process import metatile_children_with_size
        coord = Coordinate(zoom=0, column=0, row=0)
        result = metatile_children_with_size(coord, 3, 3, 512)
        self.assertEqual(set([
            Coordinate(zoom=2, column=0, row=0),
            Coordinate(zoom=2, column=1, row=0),
            Coordinate(zoom=2, column=2, row=0),
            Coordinate(zoom=2, column=3, row=0),
            Coordinate(zoom=2, column=0, row=1),
            Coordinate(zoom=2, column=1, row=1),
            Coordinate(zoom=2, column=2, row=1),
            Coordinate(zoom=2, column=3, row=1),
            Coordinate(zoom=2, column=0, row=2),
            Coordinate(zoom=2, column=1, row=2),
            Coordinate(zoom=2, column=2, row=2),
            Coordinate(zoom=2, column=3, row=2),
            Coordinate(zoom=2, column=0, row=3),
            Coordinate(zoom=2, column=1, row=3),
            Coordinate(zoom=2, column=2, row=3),
            Coordinate(zoom=2, column=3, row=3),
        ]), set(result))

    def test_2x2_tile_nominal_1(self):
        from tilequeue.process import metatile_children_with_size
        coord = Coordinate(zoom=0, column=0, row=0)
        result = metatile_children_with_size(coord, 1, 0, 256)
        self.assertEqual(set([
            Coordinate(zoom=0, column=0, row=0),
        ]), set(result))


class TestCalculateCutCoords(unittest.TestCase):

    def test_1x1(self):
        from tilequeue.process import calculate_cut_coords_by_zoom

        # note! not using zoom level 0 because that has special properties!
        coord = Coordinate(zoom=1, column=0, row=0)
        cut_coords = calculate_cut_coords_by_zoom(
            coord, 0, [256], 16)
        self.assertEqual({1: [coord]}, cut_coords)

    def test_2x2_256(self):
        from tilequeue.process import calculate_cut_coords_by_zoom

        def _c(z, x, y):
            return Coordinate(zoom=z, column=x, row=y)

        # note! not using zoom level 0 because that has special properties!
        cut_coords = calculate_cut_coords_by_zoom(
            _c(1, 0, 0), 1, [256], 16)
        self.assertEqual({
            2: [
                _c(2, 0, 0),
                _c(2, 0, 1),
                _c(2, 1, 0),
                _c(2, 1, 1),
            ]
        }, cut_coords)

    def test_4x4_512(self):
        from tilequeue.process import calculate_cut_coords_by_zoom

        def _c(z, x, y):
            return Coordinate(zoom=z, column=x, row=y)

        # note! not using zoom level 0 because that has special properties!
        cut_coords = calculate_cut_coords_by_zoom(
            _c(1, 0, 0), 2, [512], 16)
        self.assertEqual({
            3: [  # <- note nominal zoom is _3_ here.
                _c(2, 0, 0),
                _c(2, 0, 1),
                _c(2, 1, 0),
                _c(2, 1, 1),
            ]
        }, cut_coords)

    def test_4x4_512_max(self):
        from tilequeue.process import calculate_cut_coords_by_zoom

        def _c(z, x, y):
            return Coordinate(zoom=z, column=x, row=y)

        # even though we only configured 512 tiles, we get 256 ones as well at
        # max zoom.
        max_zoom = 16
        metatile_zoom = 2
        cut_coords = calculate_cut_coords_by_zoom(
            _c(max_zoom - metatile_zoom, 0, 0), metatile_zoom, [512],
            max_zoom - metatile_zoom)
        self.assertEqual([max_zoom], cut_coords.keys())
        self.assertEqual(set([
            # some 512 tiles
            _c(max_zoom - 1, 0, 0),
            _c(max_zoom - 1, 0, 1),
            _c(max_zoom - 1, 1, 0),
            _c(max_zoom - 1, 1, 1),

            # some 256 tiles
            _c(max_zoom, 0, 0),
            _c(max_zoom, 1, 0),
            _c(max_zoom, 2, 0),
            _c(max_zoom, 3, 0),
            _c(max_zoom, 0, 1),
            _c(max_zoom, 1, 1),
            _c(max_zoom, 2, 1),
            _c(max_zoom, 3, 1),
            _c(max_zoom, 0, 2),
            _c(max_zoom, 1, 2),
            _c(max_zoom, 2, 2),
            _c(max_zoom, 3, 2),
            _c(max_zoom, 0, 3),
            _c(max_zoom, 1, 3),
            _c(max_zoom, 2, 3),
            _c(max_zoom, 3, 3),
        ]), set(cut_coords[max_zoom]))

    def test_8x8_512_min(self):
        from tilequeue.process import calculate_cut_coords_by_zoom

        def _c(z, x, y):
            return Coordinate(zoom=z, column=x, row=y)

        # we get the 512px tiles at nominal zoom 3, plus additional ones at 2
        # & 1.
        metatile_zoom = 3
        cut_coords = calculate_cut_coords_by_zoom(
            _c(0, 0, 0), metatile_zoom, [512], 16 - metatile_zoom)
        self.assertEqual([1, 2, 3], cut_coords.keys())

        # we get 1x1 nominal zoom 1 tile
        self.assertEqual(set([
            _c(0, 0, 0),
        ]), set(cut_coords[1]))

        # we get 2x2 nominal zoom 2 tiles
        self.assertEqual(set([
            _c(1, 0, 0),
            _c(1, 0, 1),
            _c(1, 1, 0),
            _c(1, 1, 1),
        ]), set(cut_coords[2]))

        # we get 4x4 nominal zoom 3 tiles
        self.assertEqual(set([
            _c(2, 0, 0),
            _c(2, 0, 1),
            _c(2, 0, 2),
            _c(2, 0, 3),
            _c(2, 1, 0),
            _c(2, 1, 1),
            _c(2, 1, 2),
            _c(2, 1, 3),
            _c(2, 2, 0),
            _c(2, 2, 1),
            _c(2, 2, 2),
            _c(2, 2, 3),
            _c(2, 3, 0),
            _c(2, 3, 1),
            _c(2, 3, 2),
            _c(2, 3, 3),
        ]), set(cut_coords[3]))


class TestRemoveWrongZoomedFeatures(unittest.TestCase):
    def get_test_layers(self):
        return [dict(
            name="things",
            layer_datum="I am a datum",
            features=[
                (None, dict(name="big thing", min_zoom=10), 123),
                (None, dict(name="small thing", min_zoom=16), 234),
                (None, dict(name="tiny thing", min_zoom=18), 345),
                (None, dict(name="wow so tiny thing", min_zoom=19), 456)
            ],
            padded_bounds="I am padded bounds"
        ), dict(
            name="items",
            layer_datum="Yet another datum",
            features=[
                (None, dict(name="big item", min_zoom=13), 123),
                (None, dict(name="small item", min_zoom=15), 234),
                (None, dict(name="tiny item", min_zoom=16.999), 345),
                (None, dict(name="tiniest item", min_zoom=17), 456)
            ],
            padded_bounds="Yet another instance of padded bounds"
        )]

    def test_nominal_zoom_under_max_untouched(self):
        expected = self.get_test_layers()

        self.assertEqual(expected, remove_wrong_zoomed_features(self.get_test_layers(), 14, 15, 16))

    def test_coord_zoom_at_max_zoom_untouched(self):
        expected = self.get_test_layers()

        self.assertEqual(expected, remove_wrong_zoomed_features(self.get_test_layers(), 16, 16, 16))

    def test_higher_min_zoom_features_removed_when(self):
        expected = self.get_test_layers()

        # remove the last two items from the things layer
        expected[0]['features']=expected[0]['features'][0:2]
        # remove the last item from the items layer
        expected[1]['features']=expected[1]['features'][0:3]

        self.assertEqual(expected, remove_wrong_zoomed_features(self.get_test_layers(), 15, 16, 16))


def _only_zoom(ctx, zoom):
    layer = ctx.feature_layers[0]

    if ctx.nominal_zoom != zoom:
        layer['features'] = []

    return layer


# a "post process" function which deletes all data except at zoom zero. this
# is used to check that the nominal zoom passed in the context is the same as
# what we expect.
def _only_zoom_zero(ctx):
    return _only_zoom(ctx, 0)


def _only_zoom_one(ctx):
    return _only_zoom(ctx, 1)
