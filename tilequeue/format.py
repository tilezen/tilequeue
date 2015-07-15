from cStringIO import StringIO
from json import loads
from TileStache.Goodies.VecTiles.geojson import encode as json_encode
from TileStache.Goodies.VecTiles.geojson import merge as json_merge
from TileStache.Goodies.VecTiles.mvt import merge as mvt_merge
from TileStache.Goodies.VecTiles.oscimap import merge as vtm_merge
from TileStache.Goodies.VecTiles.topojson import encode as topojson_encode
from TileStache.Goodies.VecTiles.topojson import merge as topojson_merge


class OutputFormat(object):

    def __init__(self, name, extension, mimetype, format_fn, sort_key):
        self.name = name
        self.extension = extension
        self.mimetype = mimetype
        self.format_fn = format_fn
        self.sort_key = sort_key

    def __repr__(self):
        return 'OutputFormat(%s, %s, %s)' % \
            (self.name, self.extension, self.mimetype)

    def __hash__(self):
        return hash(self.extension)

    def __lt__(self, other):
        return self.extension < other.extension

    def __eq__(self, other):
        return self.extension == other.extension

    def format_tile(self, tile_data_file, feature_layers, coord, bounds_merc,
                    bounds_wgs84):
        self.format_fn(tile_data_file, feature_layers, coord, bounds_merc,
                       bounds_wgs84)


# consistent facade around all tilestache formatters that we use
def format_json(fp, feature_layers, coord, bounds_merc, bounds_wgs84):
    # TODO a lot of serializing/deserializing can be reduced here
    # this is a faithful port for how it's done in tilestache now
    if len(feature_layers) == 1:
        json_encode(fp, feature_layers[0]['features'], coord.zoom)
        return
    names = []
    layers = []
    for feature_layer in feature_layers:
        names.append(feature_layer['name'])
        out = StringIO()
        json_encode(out, feature_layer['features'], coord.zoom)
        # out now contains a json serialized result
        # now we deserialize it, so that it can be combined with the
        # merge function
        deserialized_features = loads(out.getvalue())
        layers.append(deserialized_features)
    json_merge(fp, names, layers, None, coord)


def format_topojson(fp, feature_layers, coord, bounds_merc, bounds_wgs84):
    # TODO ditto on the serialization as in format_json
    if len(feature_layers) == 1:
        topojson_encode(fp, feature_layers[0]['features'], bounds_wgs84)
        return
    names = []
    layers = []
    for feature_layer in feature_layers:
        names.append(feature_layer['name'])
        out = StringIO()
        topojson_encode(out, feature_layer['features'], bounds_wgs84)
        # out now contains a json serialized result
        # now we deserialize it, so that it can be combined with the
        # merge function
        deserialized_features = loads(out.getvalue())
        layers.append(deserialized_features)
    topojson_merge(fp, names, layers, None, coord)


def format_mvt(fp, feature_layers, coord, bounds_merc, bounds_wgs84):
    mvt_merge(fp, feature_layers, coord)


def format_vtm(fp, feature_layers, coord, bounds_merc, bounds_wgs84):
    vtm_merge(fp, feature_layers, coord)


json_format = OutputFormat('JSON', 'json', 'application/json', format_json, 1)
topojson_format = OutputFormat('TopoJSON', 'topojson', 'application/json',
                               format_topojson, 2)
# TODO image/png mimetype? app doesn't work unless image/png?
vtm_format = OutputFormat('OpenScienceMap', 'vtm', 'image/png', format_vtm, 3)
mvt_format = OutputFormat('MVT', 'mvt', 'application/x-protobuf',
                          format_mvt, 4)

extension_to_format = dict(
    json=json_format,
    topojson=topojson_format,
    vtm=vtm_format,
    mvt=mvt_format,
)

name_to_format = {
    'JSON': json_format,
    'OpenScienceMap': vtm_format,
    'TopoJSON': topojson_format,
    'MVT': mvt_format,
}


def lookup_format_by_extension(extension):
    return extension_to_format.get(extension)


def lookup_format_by_name(name):
    return name_to_format.get(name)
