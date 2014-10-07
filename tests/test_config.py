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
