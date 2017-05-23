from itertools import chain
from ModestMaps.Core import Coordinate
import math
import pyproj


merc_proj = pyproj.Proj(init='epsg:3857')
latlng_proj = pyproj.Proj(proj='latlong')


class CoordMessage(object):

    def __init__(self, coord, message_handle, metadata=None):
        self.coord = coord
        self.message_handle = message_handle
        self.metadata = metadata


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


def create_coord(x, y, z):
    return Coordinate(row=y, column=x, zoom=z)


def parse_expired_coord_string(coord_string):
    # we use the same format in the queue as the expired tile list from
    # osm2pgsql
    return deserialize_coord(coord_string)


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
        (1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) /
         math.pi) / 2.0 * n)
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


def reproject_lnglat_to_mercator(x, y, *unused_coords):
    return pyproj.transform(latlng_proj, merc_proj, x, y)


# mercator <-> point conversions ported from tilestache
earth_radius = 6378137
earth_circum = 2 * math.pi * earth_radius
coord_mercator_point_zoom = math.log(earth_circum) / math.log(2)
half_earth_circum = earth_circum / 2


def mercator_point_to_coord(z, x, y):
    coord = Coordinate(
        column=x + half_earth_circum,
        row=half_earth_circum - y,
        zoom=coord_mercator_point_zoom,
    )
    coord = coord.zoomTo(z).container()
    return coord


def coord_to_mercator_point(coord):
    coord = coord.zoomTo(coord_mercator_point_zoom)
    x = coord.column - half_earth_circum
    y = half_earth_circum - coord.row
    return x, y


def coord_to_mercator_bounds(coord):
    ul_x, ul_y = coord_to_mercator_point(coord)
    lr_x, lr_y = coord_to_mercator_point(coord.down().right())
    minx = min(ul_x, lr_x)
    miny = min(ul_y, lr_y)
    maxx = max(ul_x, lr_x)
    maxy = max(ul_y, lr_y)
    return minx, miny, maxx, maxy


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
    for zoom in xrange(zoom_start, zoom_until + 1):
        coords = bounds_to_coords(bounds, zoom)
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

        for tile in tile_generator_for_range(
                start_col, start_row,
                end_col, end_row,
                zoom, zoom):
            yield tile


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


# The tiles will get encoded into integers suitable for redis to store. When
# redis is given integers, it is able to store them efficiently. Note that the
# integers are sent over to redis as a string. Another format was tried which
# packed the data into 6 bytes and then sent those 6 bytes as a string, but
# that actually took more memory in redis, presumably because raw integers can
# be stored more efficiently.

# This is how the data is encoded into a 64 bit integer:
# 1 bit unused | 29 bits column | 29 bits row | 5 bits zoom
zoom_bits = 5
row_bits = 29
col_bits = 29
zoom_mask = int('1' * zoom_bits, 2)
row_mask = int(('1' * row_bits), 2)
col_mask = row_mask
row_offset = zoom_bits
col_offset = zoom_bits + row_bits

# some additional masks to help with efficient zoom up operations
all_but_zoom_mask = int('1' * 64, 2) << zoom_bits
high_row_mask = int(('1' * (1 + col_bits)) +
                    '0' +
                    ('1' * (row_bits - 1 + zoom_bits)), 2)


def coord_marshall_int(coord):
    zoom = int(coord.zoom)
    column = int(coord.column)
    row = int(coord.row)
    val = zoom | (row << row_offset) | (column << col_offset)
    return val


def coord_unmarshall_int(coord_int):
    if isinstance(coord_int, (str, unicode)):
        coord_int = int(coord_int)
    zoom = zoom_mask & coord_int
    row = row_mask & (coord_int >> row_offset)
    column = col_mask & (coord_int >> col_offset)
    return Coordinate(column=column, row=row, zoom=zoom)


# perform an efficient zoom up operation via the integer directly
def coord_int_zoom_up(coord_int):
    # First we'll update the row/col values both simultaneously by
    # shifting all bits to the right in an attempt to divide both by
    # 2. This is *almost* correct; we just need to account for the
    # fact that the lowest bit of the column value can "leak" into the
    # high bit of the row, which we do by zero'ing out just that bit
    # via the high_row_mask.
    coord_int_shifted = (coord_int >> 1) & high_row_mask

    zoom = zoom_mask & coord_int
    # Given that the row/col bits are now set correctly, all that
    # remains is to update the zoom bits. This is done by applying a
    # mask to zero out all the zoom bits, and then or'ing the new
    # parent zoom bits into place
    parent_coord_int = (coord_int_shifted & all_but_zoom_mask) | (zoom - 1)
    return parent_coord_int


def coord_children(coord):
    first_child = coord.zoomBy(1)
    return (
        first_child,
        first_child.down(),
        first_child.right(),
        first_child.right().down())


def coord_children_range(coord, zoom_until):
    assert zoom_until > coord.zoom
    children_to_process = [coord]
    cur_zoom = coord.zoom
    while cur_zoom < zoom_until:
        next_children = []
        for child_to_process in children_to_process:
            children = coord_children(child_to_process)
            for child in children:
                yield child
                next_children.append(child)
        children_to_process = next_children
        cur_zoom += 1


tolerances = [6378137 * 2 * math.pi / (2 ** (zoom + 8)) for zoom in range(22)]


def tolerance_for_zoom(zoom):
    tol_idx = zoom if 0 <= zoom < len(tolerances) else -1
    tolerance = tolerances[tol_idx]
    return tolerance


def bounds_buffer(bounds, buf_size):
    return (
        bounds[0] - buf_size, bounds[1] - buf_size,
        bounds[2] + buf_size, bounds[3] + buf_size,
    )


# radius from http://wiki.openstreetmap.org/wiki/Zoom_levels
earth_equatorial_radius_meters = 6372798.2
earth_equatorial_circumference_meters = 40041472.01586051


def calc_meters_per_pixel_dim(zoom):
    meters_in_dimension = (earth_equatorial_circumference_meters /
                           (2 ** (zoom + 8)))
    return meters_in_dimension


def calc_meters_per_pixel_area(zoom):
    meters_per_pixel_dim = calc_meters_per_pixel_dim(zoom)
    meters_per_pixel_area = meters_per_pixel_dim * meters_per_pixel_dim
    return meters_per_pixel_area


_geom_type_lookup = {
    'Point': 'point',
    'MultiPoint': 'point',
    'LineString': 'line',
    'MultiLineString': 'line',
    'Polygon': 'polygon',
    'MultiPolygon': 'polygon',
}


def normalize_geometry_type(geom_type):
    result = _geom_type_lookup.get(geom_type)
    assert result, \
        'normalize_geometry_type: unknown geometry %s' % geom_type
    return result


def coord_is_valid(coord, max_zoom=20):
    if coord.zoom < 0 or coord.zoom > max_zoom:
        return False
    if coord.column < 0 or coord.row < 0:
        return False
    max_colrow = int(math.pow(2, coord.zoom))
    if coord.column >= max_colrow or coord.row >= max_colrow:
        return False
    return True
