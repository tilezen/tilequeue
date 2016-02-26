import unittest


class RoadSortKeyTest(unittest.TestCase):

    def _call_fut(self, props, zoom=15, shape=None, fid=None):
        from TileStache.Goodies.VecTiles.transform import road_sort_key
        _, newprops, fid = road_sort_key(shape, props, fid, zoom)
        sort_key = newprops['sort_key']
        return sort_key

    def _call_fut_with_prop(self, keyval, zoom=15, shape=None,
                            fid=None):
        key, val = keyval.split('=')
        return self._call_fut({key: val}, zoom, shape, fid)

    def _call_fut_with_float_prop(self, keyval, zoom=15, shape=None,
                                  fid=None):
        key, val = keyval.split('=')
        val = float(val)
        return self._call_fut({key: val}, zoom, shape, fid)

    def test_layer_low(self):
        sort_key = self._call_fut_with_float_prop('layer=-5')
        self.assertEqual(300, sort_key)

    def test_tunnel(self):
        sort_key = self._call_fut_with_prop('tunnel=yes')
        self.assertEqual(315, sort_key)

    def test_empty(self):
        sort_key = self._call_fut({})
        self.assertEqual(325, sort_key)

    def test_railway_service_highway(self):
        props = dict(
            railway='rail',
            service='unknown',
            highway='service'
        )
        sort_key = self._call_fut(props)
        self.assertEqual(334, sort_key)

    def test_link(self):
        sort_key = self._call_fut_with_prop('highway=primary_link')
        self.assertEqual(338, sort_key)

    def test_railway(self):
        sort_key = self._call_fut_with_prop('railway=rail')
        self.assertEqual(343, sort_key)

    def test_motorway(self):
        sort_key = self._call_fut_with_prop('highway=motorway')
        self.assertEqual(344, sort_key)

    def test_bridge(self):
        sort_key = self._call_fut_with_prop('bridge=yes')
        self.assertEqual(365, sort_key)

    def test_aerialway(self):
        sort_key = self._call_fut_with_prop('aerialway=gondola')
        self.assertEqual(377, sort_key)

    def test_layer_high(self):
        sort_key = self._call_fut_with_float_prop('layer=5')
        self.assertEqual(385, sort_key)
