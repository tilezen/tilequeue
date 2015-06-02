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

    def _call_fut(self, args, config_dict):
        from tilequeue.config import make_config_from_argparse
        from yaml import dump
        raw_yaml = dump(config_dict)
        return make_config_from_argparse(
            args,
            opencfg=self._fp(raw_yaml))

    def _fp(self, raw_yaml):
        # stub out a file object that has __enter__, __exit__ methods
        from StringIO import StringIO
        from contextlib import closing
        return lambda filename: closing(StringIO(raw_yaml))

    def _args(self, data):
        # create an object with data as the attributes
        return type('mock-args', (object,), data)

    def _assert_cfg(self, cfg, to_check):
        # cfg is the config object to validate
        # to_check is a dict of key, values to check in cfg
        for k, v in to_check.items():
            cfg_val = getattr(cfg, k)
            self.assertEqual(v, cfg_val)

    def test_no_config(self):
        cfg = self._call_fut(
            self._args(dict(config=None)), {})
        # just assert some of the defaults are set
        self._assert_cfg(cfg,
                         dict(s3_path='osm',
                              output_formats=('json', 'vtm'),
                              seed_all_zoom_start=None,
                              seed_all_zoom_until=None))

    def test_config_osm_path_modified(self):
        cfg = self._call_fut(
            self._args(dict(config='config')),
            dict(store=dict(path='custompath')))
        self._assert_cfg(cfg,
                         dict(s3_path='custompath',
                              output_formats=('json', 'vtm'),
                              seed_all_zoom_start=None,
                              seed_all_zoom_until=None))
