from json import load


class MetroExtractCity(object):

    def __init__(self, region, city, bounds):
        self.region = region
        self.city = city
        self.bounds = bounds

    def __repr__(self):
        return 'MetroExtractCity(%s, %s, %s)' % \
            (self.region, self.city, self.bounds)


class MetroExtractParseError(Exception):

    def __init__(self, cause):
        self.cause = cause

    def __repr__(self):
        return 'MetroExtractParseError(%s: %s)' % (
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
