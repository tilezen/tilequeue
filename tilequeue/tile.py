class Tile(object):

    def __init__(self, zoom, col, row):
        self.zoom = zoom
        self.col = col
        self.row = row

    def __str__(self):
        return '%s/%s/%s' % (self.zoom, self.col, self.row)

class TileJob(object):

    def __init__(self, tile, format):
        self.tile = tile
        self.format = format

    def __str__(self):
        return '%s.%s' % (str(self.tile), self.format)

class TileJobMessage(object):

    def __init__(self, tile_job, message_handle):
        self.tile_job = tile_job
        self.message_handle = message_handle

    def __str__(self):
        return 'Message %s: %s' % (str(self.message_handle),
                                   str(self.tile_job))

def parse_expired_tile_string(tile_string):
    fields = tile_string.split('/')
    if len(fields) != 3:
        return None
    # z/x/y -> /zoom/col/row
    zoom, col, row = fields
    tile = Tile(col=col, row=row, zoom=zoom)
    return tile

def serialize_tile_job(tile_job):
    return '%s/%s/%s/%s' % (
            tile_job.tile.zoom, tile_job.tile.col, tile_job.tile.row,
            tile_job.format)

def deserialize_tile_job(tile_job_string):
    fields = tile_job_string.split('/')
    if len(fields) != 4:
        return None
    zoom, col, row, format = fields
    tile = Tile(zoom=zoom, col=col, row=row)
    tile_job = TileJob(tile, format)
    return tile_job
