class Tile(object):

    def __init__(self, zoom, col, row):
        self.zoom = zoom
        self.col = col
        self.row = row

    def __str__(self):
        return '%s/%s/%s' % (self.zoom, self.col, self.row)

class TileMessage(object):

    def __init__(self, tile, message_handle):
        self.tile = tile
        self.message_handle = message_handle

    def __str__(self):
        return 'Message %s: %s' % (str(self.message_handle),
                                   str(self.tile))

def parse_expired_tile_string(tile_string):
    fields = tile_string.split('/')
    if len(fields) != 3:
        return None
    # z/x/y -> /zoom/col/row
    zoom, col, row = fields
    tile = Tile(col=col, row=row, zoom=zoom)
    return tile

def serialize_tile(tile):
    return '%s/%s/%s' % (tile.zoom, tile.col, tile.row)

def deserialize_tile(tile_string):
    fields = tile_string.split('/')
    if len(fields) != 3:
        return None
    zoom, col, row = fields
    tile = Tile(zoom=zoom, col=col, row=row)
    return tile
