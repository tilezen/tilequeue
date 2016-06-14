import unittest


class QueryBoundsTest(unittest.TestCase):

    def _call_fut(self, bounds, layer_name, buffer_cfg):
        from tilequeue.command import _create_query_bounds_pad_fn
        fn = _create_query_bounds_pad_fn(buffer_cfg, layer_name)
        result = fn(bounds, 1)
        return result

    def test_no_bounds(self):
        bounds = (0, 0, 1, 1)
        result = self._call_fut(bounds, 'foo', {})
        self.assertEquals(bounds, result)

    def test_layer_not_configured(self):
        bounds = (0, 0, 1, 1)
        buffer_cfg = dict(foo={})
        result = self._call_fut(bounds, 'baz', buffer_cfg)
        self.assertEquals(bounds, result)

    def test_layer_configured(self):
        bounds = (1, 1, 2, 2)
        buffer_cfg = {
            'fmt': {
                'layer': {
                    'foo': {
                        'point': 1
                    }
                }
            }
        }
        result = self._call_fut(bounds, 'foo', buffer_cfg)
        exp_bounds = (0, 0, 3, 3)
        self.assertEquals(result, exp_bounds)

    def test_geometry_configured(self):
        bounds = (1, 1, 2, 2)
        buffer_cfg = {
            'fmt': {
                'geometry': {
                    'line': 1
                }
            }
        }
        result = self._call_fut(bounds, 'foo', buffer_cfg)
        exp_bounds = (0, 0, 3, 3)
        self.assertEquals(result, exp_bounds)

    def test_layer_trumps_geometry(self):
        bounds = (2, 2, 3, 3)
        buffer_cfg = {
            'fmt': {
                'layer': {
                    'foo': {
                        'polygon': 2
                    }
                },
                'geometry': {
                    'line': 1
                }
            }
        }
        result = self._call_fut(bounds, 'foo', buffer_cfg)
        exp_bounds = (0, 0, 5, 5)
        self.assertEquals(result, exp_bounds)

    def test_max_value_used(self):
        bounds = (2, 2, 3, 3)
        buffer_cfg = {
            'fmt': {
                'layer': {
                    'foo': {
                        'point': 1,
                        'polygon': 2,
                    }
                }
            }
        }
        result = self._call_fut(bounds, 'foo', buffer_cfg)
        exp_bounds = (0, 0, 5, 5)
        self.assertEquals(result, exp_bounds)


class ClipBoundsTest(unittest.TestCase):

    def _call_fut(
            self, bounds, ext, layer_name, geometry_type, meters_per_pixel,
            buffer_cfg):
        from tilequeue.transform import calc_buffered_bounds
        format = type(ext, (), dict(extension=ext))
        result = calc_buffered_bounds(
            format, bounds, meters_per_pixel, layer_name, geometry_type,
            buffer_cfg)
        return result

    def test_empty(self):
        bounds = (1, 1, 2, 2)
        result = self._call_fut(bounds, 'foo', 'bar', 'point', 1, {})
        self.assertEquals(result, bounds)

    def test_diff_format(self):
        bounds = (1, 1, 2, 2)
        ext = 'quux'
        result = self._call_fut(bounds, ext, 'bar', 'point', 1, dict(foo=42))
        self.assertEquals(result, bounds)

    def test_layer_match(self):
        bounds = (1, 1, 2, 2)
        ext = 'fmt'
        layer_name = 'foo'
        buffer_cfg = {
            ext: {
                'layer': {
                    layer_name: {
                        'line': 1
                    }
                }
            }
        }
        result = self._call_fut(bounds, ext, layer_name, 'line', 1, buffer_cfg)
        exp_bounds = (0, 0, 3, 3)
        self.assertEquals(result, exp_bounds)

    def test_geometry_match(self):
        bounds = (1, 1, 2, 2)
        ext = 'fmt'
        layer_name = 'foo'
        buffer_cfg = {
            ext: {
                'geometry': {
                    'line': 1
                }
            }
        }
        result = self._call_fut(bounds, ext, layer_name, 'line', 1, buffer_cfg)
        exp_bounds = (0, 0, 3, 3)
        self.assertEquals(result, exp_bounds)

    def test_layer_trumps_geometry(self):
        bounds = (2, 2, 3, 3)
        ext = 'fmt'
        layer_name = 'foo'
        buffer_cfg = {
            ext: {
                'layer': {
                    layer_name: {
                        'line': 2
                    }
                },
                'geometry': {
                    'line': 1
                }
            }
        }
        result = self._call_fut(bounds, ext, layer_name, 'line', 1, buffer_cfg)
        exp_bounds = (0, 0, 5, 5)
        self.assertEquals(result, exp_bounds)

    def test_multiple_layer_geometry_types(self):
        bounds = (2, 2, 3, 3)
        ext = 'fmt'
        layer_name = 'foo'
        buffer_cfg = {
            ext: {
                'layer': {
                    layer_name: {
                        'point': 1,
                        'line': 2,
                        'polygon': 3,
                    }
                }
            }
        }
        result = self._call_fut(bounds, ext, layer_name, 'line', 1, buffer_cfg)
        exp_bounds = (0, 0, 5, 5)
        self.assertEquals(result, exp_bounds)

    def test_multi_geometry(self):
        bounds = (1, 1, 2, 2)
        ext = 'fmt'
        layer_name = 'foo'
        buffer_cfg = {
            ext: {
                'geometry': {
                    'polygon': 1
                }
            }
        }
        result = self._call_fut(bounds, ext, layer_name, 'MultiPolygon', 1,
                                buffer_cfg)
        exp_bounds = (0, 0, 3, 3)
        self.assertEquals(result, exp_bounds)

    def test_geometry_no_match(self):
        bounds = (1, 1, 2, 2)
        ext = 'fmt'
        layer_name = 'foo'
        buffer_cfg = {
            ext: {
                'geometry': {
                    'polygon': 1
                }
            }
        }
        result = self._call_fut(bounds, ext, layer_name, 'line', 1, buffer_cfg)
        self.assertEquals(result, bounds)

    def test_meters_per_pixel(self):
        bounds = (2, 2, 3, 3)
        ext = 'fmt'
        layer_name = 'foo'
        meters_per_pixel = 2
        buffer_cfg = {
            ext: {
                'geometry': {
                    'line': 1
                }
            }
        }
        result = self._call_fut(
            bounds, ext, layer_name, 'line', meters_per_pixel, buffer_cfg)
        exp_bounds = (0, 0, 5, 5)
        self.assertEquals(result, exp_bounds)
