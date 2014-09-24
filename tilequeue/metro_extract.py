from itertools import chain
from json import load
from ModestMaps.Core import Coordinate
import math


class MetroExtractCity(object):

    def __init__(self, region, city, bounds):
        self.region = region
        self.city = city
        self.bounds = bounds


class MetroExtractParseError(Exception):

    def __init__(self, cause):
        self.cause = cause

    def __str__(self):
        return 'MetroExtractParseError: %s: %s' % (
            self.cause.__class__.__name__, str(self.cause))


def parse_metro_extract(metro_extract_fp):
    json_data = load(metro_extract_fp)
    metros = []
    try:
        regions = json_data[u'regions']
        for region_name, region_data in regions.iteritems():
            cities = region_data[u'cities']
            for city_name, city_data in cities.iteritems():
                city_json_bounds = city_data[u'bbox']
                minx = float(city_json_bounds[u'left'])
                miny = float(city_json_bounds[u'bottom'])
                maxx = float(city_json_bounds[u'right'])
                maxy = float(city_json_bounds[u'top'])
                city_bounds = (minx, miny, maxx, maxy)
                metro = MetroExtractCity(region_name, city_name, city_bounds)
                metros.append(metro)
    except (KeyError, ValueError), e:
        raise MetroExtractParseError(e)
    return metros


def city_bounds(metro_extract_cities):
    return [city.bounds for city in metro_extract_cities]


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
