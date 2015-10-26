from itertools import imap
from tilequeue.tile import coord_marshall_int


class StubIndex(object):

    def __init__(self, initial_tiles_of_interest=None):
        if initial_tiles_of_interest is not None:
            self.toi = initial_tiles_of_interest
        else:
            self.toi = []

    def intersect(coords, tiles_of_interest=None):
        for coord in coords:
            serialized_coord = coord_marshall_int(coord)
            if serialized_coord in tiles_of_interest:
                yield coord

    def fetch_tiles_of_interest(self):
        return self.toi

    def index_coord(self, coord):
        self.index_coords([coord])

    def index_coords(self, coords):
        self.toi.extend(imap(coord_marshall_int, coords))

    def is_coord_int_in_tiles_of_interest(self, coord_int):
        return coord_int in self.toi
