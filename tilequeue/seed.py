from math import pow
from ModestMaps.Core import Coordinate

def seed_tiles(start_zoom=0, until_zoom=10):
    for zoom in xrange(start_zoom, until_zoom + 1):
        limit = int(pow(2, zoom))
        for col in xrange(limit):
            for row in xrange(limit):
                yield Coordinate(zoom=zoom, column=col, row=row)
