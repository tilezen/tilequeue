from ModestMaps.Core import Coordinate

class CoordMessage(object):

    def __init__(self, coord, message_handle):
        self.coord = coord
        self.message_handle = message_handle

    def __str__(self):
        return 'Message %s: %s' % (str(self.message_handle),
                                   str(self.coord))

def parse_expired_coord_string(coord_string):
    fields = coord_string.split('/')
    if len(fields) != 3:
        return None
    # z/x/y -> /zoom/col/row
    zoom, col, row = fields
    coord = Coordinate(column=col, row=row, zoom=zoom)
    return coord

def serialize_coord(coord):
    return '%s/%s/%s' % (coord.zoom, coord.column, coord.row)

def deserialize_coord(coord_string):
    fields = coord_string.split('/')
    if len(fields) != 3:
        return None
    zoom, col, row = map(int, fields)
    coord = Coordinate(zoom=zoom, column=col, row=row)
    return coord

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
