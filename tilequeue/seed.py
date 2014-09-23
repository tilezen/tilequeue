from math import pow
from ModestMaps.Core import Coordinate

def seed_tiles(zoom_start=0, zoom_until=10):
    for zoom in xrange(zoom_start, zoom_until + 1):
        limit = int(pow(2, zoom))
        for col in xrange(limit):
            for row in xrange(limit):
                yield Coordinate(zoom=zoom, column=col, row=row)
