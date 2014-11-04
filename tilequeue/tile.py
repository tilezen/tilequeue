from itertools import chain
from ModestMaps.Core import Coordinate
import math


class CoordMessage(object):

    def __init__(self, coord, message_handle):
        self.coord = coord
        self.message_handle = message_handle

    def __repr__(self):
        return 'CoordMessage(%s, %s)' % (self.coord, self.message_handle)


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


def explode_serialized_coords(serialized_coords, until,
                              serialize_fn, deserialize_fn):
    next_serialized_coords = serialized_coords
    serialized_parents = set()
    while True:
        for serialized_coord in next_serialized_coords:
            yield serialized_coord
            coord = deserialize_fn(serialized_coord)
            if coord.zoom > until:
                parent_coord = coord.zoomTo(coord.zoom - 1).container()
                serialized_parent_coord = serialize_fn(parent_coord)
                serialized_parents.add(serialized_parent_coord)
        if not serialized_parents:
            return
        next_serialized_coords = serialized_parents
        serialized_parents = set()


def explode_with_parents(coords, until=0):
    next_coords = coords
    coords_at_parent_zoom = set()
    while True:
        for coord in next_coords:
            yield coord
            if coord.zoom > until:
                parent_coord = coord.zoomTo(coord.zoom - 1).container()
                coords_at_parent_zoom.add(parent_coord)
        if not coords_at_parent_zoom:
            return
        next_coords = coords_at_parent_zoom
        coords_at_parent_zoom = set()


def explode_with_parents_non_unique(coords, until=0):
    for coord in coords:
        while coord.zoom >= until:
            yield coord
            coord = coord.zoomTo(coord.zoom - 1).container()


def n_tiles_in_zoom(zoom):
    assert zoom >= 0
    n = 0
    for i in xrange(zoom + 1):
        n += math.pow(4, i)
    return int(n)


def seed_tiles(zoom_start=0, zoom_until=10):
    for zoom in xrange(zoom_start, zoom_until + 1):
        limit = int(math.pow(2, zoom))
        for col in xrange(limit):
            for row in xrange(limit):
                yield Coordinate(zoom=zoom, column=col, row=row)


# http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
def num2deg(xtile, ytile, zoom):
    n = 2.0 ** zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg = math.degrees(lat_rad)
    return (lat_deg, lon_deg)


# http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
def deg2num(lat_deg, lon_deg, zoom):
    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int(
        (1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi)
        / 2.0 * n)
    return (xtile, ytile)


def coord_to_bounds(coord):
    topleft_lat, topleft_lng = num2deg(coord.column, coord.row, coord.zoom)
    bottomright_lat, bottomright_lng = num2deg(
        coord.column + 1, coord.row + 1, coord.zoom)
    minx = topleft_lng
    miny = bottomright_lat
    maxx = bottomright_lng
    maxy = topleft_lat

    # coord_to_bounds is used to calculate boxes that could be off the grid
    # clamp the max values in that scenario
    maxx = min(180, maxx)
    maxy = min(90, maxy)

    bounds = (minx, miny, maxx, maxy)
    return bounds


def bounds_to_coords(bounds, zoom):
    minx, miny, maxx, maxy = bounds
    topleft_lng = minx
    topleft_lat = maxy
    bottomright_lat = miny
    bottomright_lng = maxx

    topleftx, toplefty = deg2num(topleft_lat, topleft_lng, zoom)
    bottomrightx, bottomrighty = deg2num(
        bottomright_lat, bottomright_lng, zoom)

    # clamp max values
    maxval = int(math.pow(2, zoom) - 1)
    bottomrightx = min(maxval, bottomrightx)
    bottomrighty = min(maxval, bottomrighty)

    topleftcoord = Coordinate(row=toplefty, column=topleftx, zoom=zoom)
    # check if one coordinate subsumes the whole bounds at this zoom
    if topleftx == bottomrightx and toplefty == bottomrighty:
        return [topleftcoord]

    # we have two inclusive coordinates representing the range
    bottomrightcoord = Coordinate(
        row=bottomrighty, column=bottomrightx, zoom=zoom)
    return topleftcoord, bottomrightcoord


def tile_generator_for_single_bounds(bounds, zoom_start, zoom_until):
    coords = bounds_to_coords(bounds, zoom_start)
    assert len(coords) in (1, 2)
    if len(coords) == 1:
        coord = coords[0]
        start_col = coord.column
        start_row = coord.row
        end_col = start_col
        end_row = start_row
    else:
        topleftcoord, bottomrightcoord = coords
        start_col = topleftcoord.column
        start_row = topleftcoord.row
        end_col = bottomrightcoord.column
        end_row = bottomrightcoord.row

    return tile_generator_for_range(
        start_col, start_row, end_col, end_row, zoom_start, zoom_until)


def tile_generator_for_range(
        start_col, start_row,
        end_col, end_row,
        zoom_start, zoom_until):
    zoom_multiplier = 1
    # all the "end" parameters are inclusive
    # bump them all up here to make them exclusive for range
    end_col += 1
    end_row += 1
    zoom_until += 1
    for zoom in xrange(zoom_start, zoom_until):
        for col in xrange(start_col * zoom_multiplier,
                          end_col * zoom_multiplier):
            for row in xrange(start_row * zoom_multiplier,
                              end_row * zoom_multiplier):
                yield Coordinate(row=row, column=col, zoom=zoom)
        zoom_multiplier *= 2


def tile_generator_for_multiple_bounds(bounds, zoom_start, zoom_until):
    return chain.from_iterable(
        tile_generator_for_single_bounds(bounds, zoom_start, zoom_until)
        for bounds in bounds)
