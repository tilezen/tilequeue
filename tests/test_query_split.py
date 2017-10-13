import unittest


class _NullFetcher(object):

    def start(self, data):
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
        result = splitter.start(all_data)
        expected = [(above, above_data), (below, below_data)]

        # sadly, dicts aren't hashable and frozendict isn't a thing in the
        # standard library, so seems easier to just sort the lists - although
        # a defined sort order isn't available on these objects, they should
        # be the same objects in memory, so even an id() based sorting should
        # work.
        self.assertEquals(sorted(expected), sorted(result))
