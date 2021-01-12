import unittest


class _NullFetcher(object):

    def fetch_tiles(self, data):
        for datum in data:
            yield self, datum


def _c(z, x, y):
    from ModestMaps.Core import Coordinate
    return Coordinate(zoom=z, column=x, row=y)


class TestQuerySplit(unittest.TestCase):

    def test_splits_jobs(self):
        from tilequeue.query.split import make_split_data_fetcher

        above = _NullFetcher()
        below = _NullFetcher()

        above_data = dict(coord=_c(9, 0, 0))
        below_data = dict(coord=_c(10, 0, 0))

        splitter = make_split_data_fetcher(10, above, below)

        all_data = [above_data, below_data]
        result = list(splitter.fetch_tiles(all_data))
        expected = [(above, above_data), (below, below_data)]

        self.assertEqual(len(expected), len(result))
        self.assertEqual(
            sorted(expected, key=lambda i: i[1]['coord'].zoom),
            sorted(result, key=lambda i: i[1]['coord'].zoom)
        )
