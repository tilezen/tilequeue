from json import load
from rtree import index
from shapely.geometry import box
import math

class MetroExtractCity(object):

    def __init__(self, region, city, bbox):
        self.region = region
        self.city = city
        self.bbox = bbox


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
                city_json_bbox = city_data[u'bbox']
                minx = float(city_json_bbox[u'left'])
                miny = float(city_json_bbox[u'bottom'])
                maxx = float(city_json_bbox[u'right'])
                maxy = float(city_json_bbox[u'top'])
                city_bbox = box(minx, miny, maxx, maxy)
                metro = MetroExtractCity(region_name, city_name, city_bbox)
                metros.append(metro)
    except (KeyError, ValueError), e:
        raise MetroExtractParseError(e)
    return metros

def city_bboxes(metro_extract_cities):
    return [city.bbox for city in metro_extract_cities]

def create_spatial_index(bboxes):
    idx = index.Index()
    for i, bbox in enumerate(bboxes):
        idx.insert(i, bbox.bounds)
    return idx

def bbox_in_metro_extract(spatial_index, bbox):
    for _ in spatial_index.intersection(bbox.bounds):
        return True
    return False

def coord_in_metro_extract(spatial_index, coord):
    bbox = coord_to_bbox(coord)
    return bbox_in_metro_extract(spatial_index, bbox)

# from http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Tile_numbers_to_lon..2Flat._2
def num2deg(xtile, ytile, zoom):
    n = 2.0 ** zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg = math.degrees(lat_rad)
    return (lat_deg, lon_deg)

def coord_to_bbox(coord):
    topleft_lat, topleft_lng = num2deg(coord.column, coord.row, coord.zoom)
    bottomright_lat, bottomright_lng = num2deg(
            coord.column + 1, coord.row + 1, coord.zoom)
    minx = topleft_lng
    miny = bottomright_lat
    maxx = bottomright_lng
    maxy = topleft_lat

    # coord_to_bbox is used to calculate boxes that could be off the grid
    # clamp the max values in that scenario
    maxx = min(180, maxx)
    maxy = min(90, maxy)

    bbox = box(minx, miny, maxx, maxy)
    return bbox

def make_metro_extract_predicate(spatial_index, starting_zoom):
    def predicate(coord):
        if coord.zoom < starting_zoom:
            return True
        return coord_in_metro_extract(spatial_index, coord)
    return predicate
