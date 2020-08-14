import unittest


class TestConfigMerge(unittest.TestCase):

    def _call_fut(self, destcfg, srccfg):
        from tilequeue.config import merge_cfg
        return merge_cfg(destcfg, srccfg)

    def test_both_empty(self):
        self.assertEqual({}, self._call_fut({}, {}))

    def test_complementary_scalar(self):
        src = dict(foo='bar')
        dest = dict(quux='morx')
        self.assertEqual(dict(foo='bar', quux='morx'),
                         self._call_fut(dest, src))

    def test_nested_complementary(self):
        src = dict(foo=dict(bar='baz'))
        dest = dict(quux=dict(morx='fleem'))
        self.assertEqual(
            dict(foo=dict(bar='baz'),
                 quux=dict(morx='fleem')),
            self._call_fut(dest, src))

    def test_merge_complementary(self):
        src = dict(foo=dict(bar='baz'))
        dest = dict(foo=dict(morx='fleem'))
        self.assertEqual(
            dict(foo=dict(bar='baz', morx='fleem')),
            self._call_fut(dest, src))

    def test_merge_override(self):
        src = dict(foo=dict(bar='baz'))
        dest = dict(foo=dict(bar='fleem'))
        self.assertEqual(
            dict(foo=dict(bar='baz')),
            self._call_fut(dest, src))


class TestCliConfiguration(unittest.TestCase):

    def _call_fut(self, config_dict):
        from tilequeue.config import make_config_from_argparse
        from yaml import dump
        from io import StringIO
        raw_yaml = dump(config_dict)
        raw_yaml_file_obj = StringIO(raw_yaml)
        return make_config_from_argparse(raw_yaml_file_obj)

    def _assert_cfg(self, cfg, to_check):
        # cfg is the config object to validate
        # to_check is a dict of key, values to check in cfg
        for k, v in to_check.items():
            cfg_val = getattr(cfg, k)
            self.assertEqual(v, cfg_val)

    def test_no_config(self):
        cfg = self._call_fut(dict(config=None))
        # just assert some of the defaults are set
        self._assert_cfg(cfg,
                         dict(s3_path='osm',
                              output_formats=['json'],
                              seed_all_zoom_start=None,
                              seed_all_zoom_until=None))

    def test_config_osm_path_modified(self):
        cfg = self._call_fut(
            dict(store=dict(path='custompath')))
        self._assert_cfg(cfg,
                         dict(s3_path='custompath',
                              output_formats=['json'],
                              seed_all_zoom_start=None,
                              seed_all_zoom_until=None))


class TestMetatileConfiguration(unittest.TestCase):

    def _call_fut(self, config_dict):
        from tilequeue.config import make_config_from_argparse
        from yaml import dump
        from io import StringIO
        raw_yaml = dump(config_dict)
        raw_yaml_file_obj = StringIO(raw_yaml)
        return make_config_from_argparse(raw_yaml_file_obj)

    def test_metatile_size_default(self):
        config_dict = {}
        cfg = self._call_fut(config_dict)
        self.assertIsNone(cfg.metatile_size)
        self.assertEqual(cfg.metatile_zoom, 0)
        self.assertEqual(cfg.tile_sizes, [256])

    def test_metatile_size_1(self):
        config_dict = dict(metatile=dict(size=1))
        cfg = self._call_fut(config_dict)
        self.assertEqual(cfg.metatile_size, 1)
        self.assertEqual(cfg.metatile_zoom, 0)
        self.assertEqual(cfg.tile_sizes, [256])

    def test_metatile_size_2(self):
        config_dict = dict(metatile=dict(size=2))
        cfg = self._call_fut(config_dict)
        self.assertEqual(cfg.metatile_size, 2)
        self.assertEqual(cfg.metatile_zoom, 1)
        self.assertEqual(cfg.tile_sizes, [512, 256])

    def test_metatile_size_4(self):
        config_dict = dict(metatile=dict(size=4))
        cfg = self._call_fut(config_dict)
        self.assertEqual(cfg.metatile_size, 4)
        self.assertEqual(cfg.metatile_zoom, 2)
        self.assertEqual(cfg.tile_sizes, [1024, 512, 256])

    def test_max_zoom(self):
        config_dict = dict(metatile=dict(size=2))
        cfg = self._call_fut(config_dict)
        self.assertEqual(cfg.max_zoom, 15)
