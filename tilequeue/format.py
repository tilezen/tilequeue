from cStringIO import StringIO
from json import loads
from TileStache.Goodies.VecTiles.geojson import encode as json_encode
from TileStache.Goodies.VecTiles.geojson import merge as json_merge
from TileStache.Goodies.VecTiles.mapbox import merge as mapbox_merge
from TileStache.Goodies.VecTiles.oscimap import merge as vtm_merge
from TileStache.Goodies.VecTiles.topojson import encode as topojson_encode
from TileStache.Goodies.VecTiles.topojson import merge as topojson_merge


class OutputFormat(object):

    def __init__(self, name, extension, mimetype):
        self.name = name
        self.extension = extension
        self.mimetype = mimetype

    def __repr__(self):
        return 'OutputFormat(%s, %s, %s)' % \
            (self.name, self.extension, self.mimetype)

    def __hash__(self):
        return hash(self.extension)

    def __lt__(self, other):
        return self.extension < other.extension

    def __eq__(self, other):
        return self.extension == other.extension

json_format = OutputFormat('JSON', 'json', 'application/json')
topojson_format = OutputFormat('TopoJSON', 'topojson', 'application/json')
# TODO image/png mimetype? app doesn't work unless image/png?
vtm_format = OutputFormat('OpenScienceMap', 'vtm', 'image/png')
mapbox_format = OutputFormat('Mapbox', 'mapbox', 'application/x-protobuf')

extension_to_format = dict(
    json=json_format,
    topojson=topojson_format,
    vtm=vtm_format,
    mapbox=mapbox_format,
)

name_to_format = {
    'JSON': json_format,
    'OpenScienceMap': vtm_format,
    'TopoJSON': topojson_format,
    'Mapbox': mapbox_format,
}


def lookup_format_by_extension(extension):
    return extension_to_format.get(extension)


def lookup_format_by_name(name):
    return name_to_format.get(name)


# consistent facade around all tilestache formatters that we use
def format_json(fp, feature_layers, coord, bounds):
    # TODO a lot of serializing/deserializing can be reduced here
    # this is a faithful port for how it's done in tilestache now
    names = []
    layers = []
    for feature_layer in feature_layers:
        names.append(feature_layer['name'])
        out = StringIO()
        is_clipped = feature_layer['layer_datum']['is_clipped']
        json_encode(out, feature_layer['features'], coord.zoom, is_clipped)
        # out now contains a json serialized result
        # now we deserialize it, so that it can be combined with the
        # merge function
        deserialized_features = loads(out.getvalue())
        layers.append(deserialized_features)
    json_merge(fp, names, layers, None, coord)


def format_topojson(fp, feature_layers, coord, bounds):
    # TODO ditto on the serialization as in format_json
    names = []
    layers = []
    for feature_layer in feature_layers:
        names.append(feature_layer['name'])
        out = StringIO()
        is_clipped = feature_layer['layer_datum']['is_clipped']
        topojson_encode(out, feature_layer['features'], bounds, is_clipped)
        # out now contains a json serialized result
        # now we deserialize it, so that it can be combined with the
        # merge function
        deserialized_features = loads(out.getvalue())
        layers.append(deserialized_features)
    topojson_merge(fp, names, layers, None, coord)


def format_mapbox(fp, feature_layers, coord, bounds):
    mapbox_merge(fp, feature_layers, coord)


def format_vtm(fp, feature_layers, coord, bounds):
    vtm_merge(fp, feature_layers, coord)


format_to_formatter = {
    json_format: format_json,
    vtm_format: format_vtm,
    topojson_format: format_topojson,
    mapbox_format: format_mapbox,
}


def lookup_formatter(format):
    format_fn = format_to_formatter.get(format)
    assert format_fn is not None, 'Unknown format: %s' % format
    return format_fn
