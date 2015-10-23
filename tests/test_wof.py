import unittest


class TestNeighbourhoodDiff(unittest.TestCase):

    def _call_fut(self, xs, ys):
        from tilequeue.wof import diff_neighbourhoods
        return diff_neighbourhoods(xs, ys)

    def test_no_neighbourhoods(self):
        diffs = self._call_fut([], [])
        self.failIf(diffs)

    def _n(self, wof_id, name, x, y):
        from tilequeue.wof import Neighbourhood
        neighbourhood = Neighbourhood(wof_id, name, x, y)
        return neighbourhood

    def test_neighbourhood_added(self):
        n = self._n(1, 'foo', 1, 2)
        diffs = self._call_fut([], [n])
        self.failUnless(diffs)
        self.assertEqual(len(diffs), 1)
        x, y = diffs[0]
        self.assertIsNone(x)
        self.assertEqual(y, n)

    def test_neighbourhood_removed(self):
        n = self._n(1, 'foo', 1, 2)
        diffs = self._call_fut([n], [])
        self.failUnless(diffs)
        self.assertEqual(len(diffs), 1)
        x, y = diffs[0]
        self.assertIsNone(y)
        self.assertEqual(x, n)

    def test_neighbourhoods_equal(self):
        ns = [self._n(i, 'foo', 1.324, 2.23987) for i in xrange(32)]
        diffs = self._call_fut(ns, ns)
        self.failIf(diffs)

    def test_neighbourhoods_id_mismatch_cases(self):
        def _make(wof_id):
            return self._n(wof_id, 'foo', 12938.123, -238.329)
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

    def test_neighbourhoods_different_names(self):
        x = self._n(42, 'foo', 2, 1)
        y = self._n(42, 'bar', 2, 1)
        diffs = self._call_fut([x], [y])
        self.assertEqual(len(diffs), 1)
        dx, dy = diffs[0]
        self.assertEqual(dx, x)
        self.assertEqual(dy, y)

    def test_neighbourhood_delta_x(self):
        x = self._n(42, 'foo', 2.00001, 1)
        y = self._n(42, 'foo', 2.000001, 1)
        diffs = self._call_fut([x], [y])
        self.assertEqual(len(diffs), 1)
        dx, dy = diffs[0]
        self.assertEqual(dx, x)
        self.assertEqual(dy, y)

    def test_neighbourhood_delta_y(self):
        x = self._n(42, 'foo', 2, 1.00000001)
        y = self._n(42, 'foo', 2, 1.000000001)
        diffs = self._call_fut([x], [y])
        self.assertEqual(len(diffs), 1)
        dx, dy = diffs[0]
        self.assertEqual(dx, x)
        self.assertEqual(dy, y)

    def test_neighbourhood_small_delta_no_diff(self):
        x = self._n(42, 'foo', 2.00000000001, 1.00000000009)
        y = self._n(42, 'foo', 2.00000000002, 1.00000000008)
        diffs = self._call_fut([x], [y])
        self.assertEqual(len(diffs), 0)
