import unittest


class TestNeighbourhoodDiff(unittest.TestCase):

    def _call_fut(self, xs, ys):
        from tilequeue.wof import diff_neighbourhoods
        return diff_neighbourhoods(xs, ys)

    def test_no_neighbourhoods(self):
        diffs = self._call_fut([], [])
        self.assertFalse(diffs)

    def _n(self, wof_id, name, hash):
        from tilequeue.wof import NeighbourhoodMeta
        placetype = None
        label_position = None
        neighbourhood = NeighbourhoodMeta(
            wof_id, placetype, name, hash, label_position)
        return neighbourhood

    def test_neighbourhood_added(self):
        n = self._n(1, 'foo', 'hash')
        diffs = self._call_fut([], [n])
        self.assertTrue(diffs)
        self.assertEqual(len(diffs), 1)
        x, y = diffs[0]
        self.assertIsNone(x)
        self.assertEqual(y, n)

    def test_neighbourhood_removed(self):
        n = self._n(1, 'foo', 'hash')
        diffs = self._call_fut([n], [])
        self.assertTrue(diffs)
        self.assertEqual(len(diffs), 1)
        x, y = diffs[0]
        self.assertIsNone(y)
        self.assertEqual(x, n)

    def test_neighbourhoods_equal(self):
        ns = [self._n(i, 'foo', 'hash-%d' % i) for i in range(32)]
        diffs = self._call_fut(ns, ns)
        self.assertFalse(diffs)

    def test_neighbourhoods_id_mismatch_cases(self):
        def _make(wof_id):
            return self._n(wof_id, 'foo', 'hash')
        xs = [_make(x) for x in (1, 2, 4, 5)]
        ys = [_make(x) for x in (2, 3, 4, 6)]
        diffs = self._call_fut(xs, ys)

        def assert_id(n, wof_id):
            self.assertTrue(n)
            self.assertEqual(wof_id, n.wof_id)

        self.assertEqual(len(diffs), 4)
        d1, d2, d3, d4 = diffs

        d1x, d1y = d1
        self.assertIsNone(d1y)
        assert_id(d1x, 1)

        d2x, d2y = d2
        self.assertIsNone(d2x)
        assert_id(d2y, 3)

        d3x, d3y = d3
        self.assertIsNone(d3y)
        assert_id(d3x, 5)

        d3x, d3y = d3
        self.assertIsNone(d3y)
        assert_id(d3x, 5)

        d4x, d4y = d4
        self.assertIsNone(d4x)
        assert_id(d4y, 6)

    def test_neighbourhoods_different_hashes(self):
        x = self._n(42, 'foo', 'hash-1')
        y = self._n(42, 'foo', 'hash-2')
        diffs = self._call_fut([x], [y])
        self.assertEqual(len(diffs), 1)
        dx, dy = diffs[0]
        self.assertEqual(dx, x)
        self.assertEqual(dy, y)


class TestNeighbourhoodSupersededBy(unittest.TestCase):

    def _check_is_superseded(self, json, wof_id, superseded_by):
        from tilequeue.wof import create_neighbourhood_from_json, \
            NeighbourhoodFailure, NeighbourhoodMeta

        meta = NeighbourhoodMeta(
            wof_id, 'neighbourhood', '', '', None
        )
        n = create_neighbourhood_from_json(json, meta)
        self.assertIsInstance(n, NeighbourhoodFailure)
        self.assertEqual(n.wof_id, wof_id, "%s: %s" % (n.reason, n.message))
        self.assertTrue(n.superseded, "%s: %s" % (n.reason, n.message))

    def test_neighbourhood_superseded_by(self):
        self._check_is_superseded(
            {'properties': {
                'wof:id': 12345,
                'wof:superseded_by': [12346]
            }},
            12345,
            [12346])

    def test_neighbourhood_superseded_by_multiple(self):
        self._check_is_superseded(
            {'properties': {
                'wof:id': 12345,
                'wof:superseded_by': [12346, 12347]
            }},
            12345,
            [12346, 12347])


class TestEmptyDates(unittest.TestCase):

    def _create_neighbourhood_and_meta(
            self, inception_date_str, cessation_date_str):
        from tilequeue.wof import NeighbourhoodMeta
        import shapely.geometry
        props = {
            'geom:area': 0.0,
            'geom:bbox': '0.0,0.0,0.0,0.0',
            'geom:latitude': 0.0,
            'geom:longitude': 0.0,
            'iso:country': 'XN',
            'lbl:latitude': 0.0,
            'lbl:longitude': 0.0,
            'mz:hierarchy_label': 1,
            'mz:is_funky': 0,
            'mz:is_hard_boundary': 0,
            'mz:is_landuse_aoi': 0,
            'mz:is_official': 0,
            'mz:max_zoom': 18,
            'mz:min_zoom': 8,
            'mz:tier_locality': 1,
            'src:geom': 'mapzen',
            'src:geom_alt': [],
            'src:lbl:centroid': 'mapzen',
            'wof:belongsto': [1],
            'wof:breaches': [],
            'wof:concordances': {},
            'wof:country': 'XN',
            'wof:geomhash': 'fc4d4085e55d16b479f231dbf54d3cfb',
            'wof:hierarchy': [{'country_id': 1,
                               'neighbourhood_id': 874397665}],
            'wof:id': 874397665,
            'wof:lastmodified': 1468006253,
            'wof:name': 'Null Island',
            'wof:parent_id': -2,
            'wof:placetype': 'neighbourhood',
            'wof:repo': 'whosonfirst-data',
            'wof:superseded_by': [],
            'wof:supersedes': [],
            'wof:tags': []
        }
        props['edtf:inception'] = inception_date_str
        props['edtf:cessation'] = cessation_date_str
        json_data = {
            'bbox': [0.0, 0.0, 0.0, 0.0],
            'geometry': {'coordinates': [0.0, 0.0], 'type': 'Point'},
            'id': 874397665,
            'properties': props,
            'type': 'Fetaure'
        }
        label_position = shapely.geometry.Point(0, 0)
        meta = NeighbourhoodMeta(
            wof_id=874397665,
            placetype='neighbourhood',
            name='Null Island',
            hash='722e68cdcba2cd514e8ad2492cab61fb',
            label_position=label_position,
        )
        return json_data, meta

    def _call_fut(self, inception_date_str, cessation_date_str):
        from tilequeue.wof import create_neighbourhood_from_json
        json_data, meta = self._create_neighbourhood_and_meta(
            inception_date_str, cessation_date_str)
        result = create_neighbourhood_from_json(json_data, meta)
        return result

    def test_empty_dates(self):
        from datetime import date
        result = self._call_fut('', '')
        self.assertEqual(date(1, 1, 1), result.inception)
        self.assertEqual(date(9999, 12, 31), result.cessation)

    def test_u_dates(self):
        from datetime import date
        result = self._call_fut('u', 'u')
        self.assertEqual(date(1, 1, 1), result.inception)
        self.assertEqual(date(9999, 12, 31), result.cessation)

    def test_uuuu_dates(self):
        from datetime import date
        result = self._call_fut('uuuu', 'uuuu')
        self.assertEqual(date(1, 1, 1), result.inception)
        self.assertEqual(date(9999, 12, 31), result.cessation)

    def test_None_dates(self):
        from datetime import date
        result = self._call_fut(None, None)
        self.assertEqual(date(1, 1, 1), result.inception)
        self.assertEqual(date(9999, 12, 31), result.cessation)

    def test_valid_dates(self):
        from datetime import date
        result = self._call_fut('1985-10-26', '1985-10-26')
        self.assertEqual(date(1985, 10, 26), result.inception)
        self.assertEqual(date(1985, 10, 26), result.cessation)


class MinMaxZoomFloatTest(unittest.TestCase):

    def _create_neighbourhood_and_meta(self, min_zoom, max_zoom):
        from tilequeue.wof import NeighbourhoodMeta
        import shapely.geometry
        props = {
            'geom:area': 0.0,
            'geom:bbox': '0.0,0.0,0.0,0.0',
            'geom:latitude': 0.0,
            'geom:longitude': 0.0,
            'iso:country': 'XN',
            'lbl:latitude': 0.0,
            'lbl:longitude': 0.0,
            'mz:hierarchy_label': 1,
            'mz:is_funky': 0,
            'mz:is_hard_boundary': 0,
            'mz:is_landuse_aoi': 0,
            'mz:is_official': 0,
            'mz:tier_locality': 1,
            'src:geom': 'mapzen',
            'src:geom_alt': [],
            'src:lbl:centroid': 'mapzen',
            'wof:belongsto': [1],
            'wof:breaches': [],
            'wof:concordances': {},
            'wof:country': 'XN',
            'wof:geomhash': 'fc4d4085e55d16b479f231dbf54d3cfb',
            'wof:hierarchy': [{'country_id': 1,
                               'neighbourhood_id': 874397665}],
            'wof:id': 874397665,
            'wof:lastmodified': 1468006253,
            'wof:name': 'Null Island',
            'wof:parent_id': -2,
            'wof:placetype': 'neighbourhood',
            'wof:repo': 'whosonfirst-data',
            'wof:superseded_by': [],
            'wof:supersedes': [],
            'wof:tags': [],
            'edtf:inception': 'uuuu',
            'edtf:cessation': 'uuuu',
            'mz:min_zoom': min_zoom,
            'mz:max_zoom': max_zoom,
        }
        json_data = {
            'bbox': [0.0, 0.0, 0.0, 0.0],
            'geometry': {'coordinates': [0.0, 0.0], 'type': 'Point'},
            'id': 874397665,
            'properties': props,
            'type': 'Fetaure'
        }
        label_position = shapely.geometry.Point(0, 0)
        meta = NeighbourhoodMeta(
            wof_id=874397665,
            placetype='neighbourhood',
            name='Null Island',
            hash='722e68cdcba2cd514e8ad2492cab61fb',
            label_position=label_position,
        )
        return json_data, meta

    def _call_fut(self, min_zoom, max_zoom):
        from tilequeue.wof import create_neighbourhood_from_json
        json_data, meta = self._create_neighbourhood_and_meta(
            min_zoom, max_zoom)
        result = create_neighbourhood_from_json(json_data, meta)
        return result

    def test_integer_min_max_zoom(self):
        neighbourhood = self._call_fut(14, 16)
        self.assertEqual(neighbourhood.min_zoom, 14.0)
        self.assertEqual(neighbourhood.max_zoom, 16.0)

    def test_float_min_max_zoom(self):
        neighbourhood = self._call_fut(14.2, 16.8)
        self.assertEqual(neighbourhood.min_zoom, 14.2)
        self.assertEqual(neighbourhood.max_zoom, 16.8)
