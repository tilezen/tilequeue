from ModestMaps.Core import Coordinate
import unittest


class TestProcess(unittest.TestCase):

    def _make_json_tile(self, coord, post_process_data, db_features):
        from tilequeue.process import process_coord
        from tilequeue.tile import coord_to_mercator_bounds
        from tilequeue.format import json_format
        import json

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
        cut_coords = []
        buffer_cfg = {}

        tiles, extra = process_coord(
            coord, feature_layers, post_process_data, formats, unpadded_bounds,
            cut_coords, buffer_cfg)

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
        cut_coords = []
        buffer_cfg = {}

        tiles, extra = process_coord(
            coord, feature_layers, post_process_data, formats, unpadded_bounds,
            cut_coords, buffer_cfg)

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
                foo="bar"
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
                        'foo': 'bar'
                    },
                    'id': 1
                }]

            tile = self._make_json_tile(coord, post_process_data, features)
            self.assertEqual(json_data, tile)

        _check(Coordinate(0, 0, 0), '_only_zoom_zero', True)
        _check(Coordinate(0, 0, 0), '_only_zoom_one', False)
        _check(Coordinate(0, 1, 1), '_only_zoom_one', True)
        _check(Coordinate(0, 1, 1), '_only_zoom_zero', False)


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
