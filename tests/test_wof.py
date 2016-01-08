import unittest


class TestNeighbourhoodDiff(unittest.TestCase):

    def _call_fut(self, xs, ys):
        from tilequeue.wof import diff_neighbourhoods
        return diff_neighbourhoods(xs, ys)

    def test_no_neighbourhoods(self):
        diffs = self._call_fut([], [])
        self.failIf(diffs)

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
        self.failUnless(diffs)
        self.assertEqual(len(diffs), 1)
        x, y = diffs[0]
        self.assertIsNone(x)
        self.assertEqual(y, n)

    def test_neighbourhood_removed(self):
        n = self._n(1, 'foo', 'hash')
        diffs = self._call_fut([n], [])
        self.failUnless(diffs)
        self.assertEqual(len(diffs), 1)
        x, y = diffs[0]
        self.assertIsNone(y)
        self.assertEqual(x, n)

    def test_neighbourhoods_equal(self):
        ns = [self._n(i, 'foo', 'hash-%d' % i) for i in xrange(32)]
        diffs = self._call_fut(ns, ns)
        self.failIf(diffs)

    def test_neighbourhoods_id_mismatch_cases(self):
        def _make(wof_id):
            return self._n(wof_id, 'foo', 'hash')
        xs = [_make(x) for x in (1, 2, 4, 5)]
        ys = [_make(x) for x in (2, 3, 4, 6)]
        diffs = self._call_fut(xs, ys)

        def assert_id(n, wof_id):
            self.failUnless(n)
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
            { 'properties': {
                'wof:id': 12345,
                'wof:superseded_by': [12346]
            } },
            12345,
            [12346])

    def test_neighbourhood_superseded_by_multiple(self):
        self._check_is_superseded(
            { 'properties': {
                'wof:id': 12345,
                'wof:superseded_by': [12346, 12347]
            } },
            12345,
            [12346, 12347])
