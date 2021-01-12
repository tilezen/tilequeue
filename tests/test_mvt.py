import unittest


class MapboxVectorTileTest(unittest.TestCase):

    def _make_tiles(self, shape, coord, metatile_zoom):
        from tilequeue.format import mvt_format
        from tilequeue.process import process_coord
        from tilequeue.tile import coord_children_range
        from tilequeue.tile import coord_to_mercator_bounds

        db_features = [dict(
            __id__=1,
            __geometry__=shape.wkb,
            __properties__={},
        )]

        nominal_zoom = coord.zoom + metatile_zoom
        unpadded_bounds = coord_to_mercator_bounds(coord)
        feature_layers = [dict(
            layer_datum=dict(
                name='fake_layer',
                geometry_types=[shape.geom_type],
                transform_fn_names=[],
                sort_fn_name=None,
                is_clipped=False
            ),
            padded_bounds={shape.geom_type.lower(): unpadded_bounds},
            features=db_features
        )]
        formats = [mvt_format]

        post_process_data = {}
        buffer_cfg = {}
        cut_coords = [coord]
        if nominal_zoom > coord.zoom:
            cut_coords.extend(coord_children_range(coord, nominal_zoom))

        def _output_fn(shape, props, fid, meta):
            return dict(fake='data', min_zoom=0)

        output_calc_mapping = dict(fake_layer=_output_fn)
        tiles, extra = process_coord(
            coord, nominal_zoom, feature_layers, post_process_data, formats,
            unpadded_bounds, cut_coords, buffer_cfg, output_calc_mapping)

        self.assertEqual(len(cut_coords), len(tiles))
        return tiles, cut_coords

    def _check_metatile(self, metatile_size):
        from mock import patch
        from shapely.geometry import box
        from ModestMaps.Core import Coordinate
        from tilequeue.tile import coord_to_mercator_bounds
        from tilequeue.tile import metatile_zoom_from_size

        name = 'tilequeue.format.mvt.mvt_encode'
        with patch(name, return_value='') as encode:
            coord = Coordinate(0, 0, 0)
            bounds = coord_to_mercator_bounds(coord)
            pixel_fraction = 1.0 / 4096.0
            box_width = pixel_fraction * (bounds[2] - bounds[0])
            box_height = pixel_fraction * (bounds[3] - bounds[1])
            shape = box(bounds[0], bounds[1],
                        bounds[0] + box_width,
                        bounds[1] + box_height)

            metatile_zoom = metatile_zoom_from_size(metatile_size)
            tiles, tile_coords = self._make_tiles(shape, coord, metatile_zoom)

            num_tiles = 0
            for z in range(0, metatile_zoom + 1):
                num_tiles += 4**z

            # resolution should be 4096 at 256px, which is metatile_zoom
            # levels down from the extent of the world.
            resolution = (bounds[2] - bounds[0]) / (4096 * 2**metatile_zoom)

            self.assertEqual(num_tiles, len(tiles))
            self.assertEqual(num_tiles, encode.call_count)
            for (posargs, kwargs), coord in zip(encode.call_args_list,
                                                tile_coords):
                self.assertIn('quantize_bounds', kwargs)
                quantize_bounds = kwargs['quantize_bounds']
                extent = int(round((quantize_bounds[2] - quantize_bounds[0]) /
                                   resolution))
                self.assertIn('extents', kwargs)
                actual_extent = kwargs['extents']
                self.assertEqual(extent, actual_extent,
                                 "Expected %r, not %r, for coord %r" %
                                 (extent, actual_extent, coord))

    def test_single_tile(self):
        self._check_metatile(1)

    def test_metatile_size_2(self):
        self._check_metatile(2)

    def test_metatile_size_4(self):
        self._check_metatile(4)
