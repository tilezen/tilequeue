from itertools import chain


class DataFetcher(object):

    """
    Splits requests between two data fetchers depending on whether the zoom of
    the coordinate is above or below (or equal) the "split zoom".

    This is used to choose between going to the database for low zooms or
    fetching a RAWR tile for high zooms.
    """

    def __init__(self, split_zoom, below_fetcher, above_fetcher):
        self.split_zoom = split_zoom
        self.below_fetcher = below_fetcher
        self.above_fetcher = above_fetcher

    def fetch_tiles(self, all_data):
        below_data = []
        above_data = []

        for data in all_data:
            coord = data['coord']
            if coord.zoom < self.split_zoom:
                below_data.append(data)
            else:
                above_data.append(data)

        return chain(self.above_fetcher.fetch_tiles(above_data),
                     self.below_fetcher.fetch_tiles(below_data))


def make_split_data_fetcher(split_zoom, below_fetcher, above_fetcher):
    return DataFetcher(split_zoom, below_fetcher, above_fetcher)
