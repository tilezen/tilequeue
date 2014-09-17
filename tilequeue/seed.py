from math import pow
from ModestMaps.Core import Coordinate

def seed_tiles(until_zoom=10):
    for zoom in xrange(until_zoom + 1):
        limit = int(pow(2, zoom))
        for col in xrange(limit):
            for row in xrange(limit):
                yield Coordinate(zoom=zoom, column=col, row=row)
