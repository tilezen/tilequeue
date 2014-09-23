from math import pow
from ModestMaps.Core import Coordinate


class CoordMessage(object):

    def __init__(self, coord, message_handle):
        self.coord = coord
        self.message_handle = message_handle

    def __str__(self):
        return 'Message %s: %s' % (str(self.message_handle),
                                   str(self.coord))


def serialize_coord(coord):
    return '%d/%d/%d' % (coord.zoom, coord.column, coord.row)


def deserialize_coord(coord_string):
    fields = coord_string.split('/')
    if len(fields) != 3:
        return None
    # z/x/y -> /zoom/col/row
    try:
        zoom, col, row = map(int, fields)
    except ValueError:
        return None
    coord = Coordinate(row=row, column=col, zoom=zoom)
    return coord


def parse_expired_coord_string(coord_string):
    # we use the same format in the queue as the expired tile list from
    # osm2pgsql
    return deserialize_coord(coord_string)


def generate_parents(coord):
    c = coord
    while c.zoom > 0:
        c = c.zoomTo(c.zoom-1).container()
        yield c


def explode_with_parents(coords):
    s = set()
    for coord in coords:
        s.add(coord)
        for parent in generate_parents(coord):
            if parent in s:
                break
            s.add(parent)
    return s


def n_tiles_in_zoom(zoom):
    assert zoom >= 0
    n = 0
    for i in xrange(zoom + 1):
        n += pow(4, i)
    return int(n)
