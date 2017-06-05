from tilequeue.format.geojson import encode_multiple_layers as json_encode_multiple_layers  # noqa
from tilequeue.format.geojson import encode_single_layer as json_encode_single_layer  # noqa
from tilequeue.format.mvt import encode as mvt_encode
from tilequeue.format.topojson import encode as topojson_encode
from tilequeue.format.vtm import merge as vtm_encode


class OutputFormat(object):

    def __init__(self, name, extension, mimetype, format_fn, sort_key,
                 supports_shapely_geometry):
        self.name = name
        self.extension = extension
        self.mimetype = mimetype
        self.format_fn = format_fn
        self.sort_key = sort_key
        self.supports_shapely_geometry = supports_shapely_geometry

    def __repr__(self):
        return 'OutputFormat(%s, %s, %s)' % \
            (self.name, self.extension, self.mimetype)

    def __hash__(self):
        return hash(self.extension)

    def __lt__(self, other):
        return self.extension < other.extension

    def __eq__(self, other):
        return self.extension == other.extension

    def format_tile(self, tile_data_file, feature_layers, zoom, bounds_merc,
                    bounds_lnglat):
        self.format_fn(tile_data_file, feature_layers, zoom, bounds_merc,
                       bounds_lnglat)


def convert_feature_layers_to_dict(feature_layers):
    """takes a list of 'feature_layer' objects and converts to a dict
       keyed by the layer name"""
    features_by_layer = {}
    for feature_layer in feature_layers:
        layer_name = feature_layer['name']
        features = feature_layer['features']
        features_by_layer[layer_name] = features
    return features_by_layer


# consistent facade around all formatters that we use
def format_json(fp, feature_layers, zoom, bounds_merc, bounds_lnglat):
    if len(feature_layers) == 1:
        json_encode_single_layer(fp, feature_layers[0]['features'], zoom)
        return
    else:
        features_by_layer = convert_feature_layers_to_dict(feature_layers)
        json_encode_multiple_layers(fp, features_by_layer, zoom)


def format_topojson(fp, feature_layers, zoom, bounds_merc, bounds_lnglat):
    features_by_layer = convert_feature_layers_to_dict(feature_layers)
    topojson_encode(fp, features_by_layer, bounds_lnglat)


def format_mvt(fp, feature_layers, zoom, bounds_merc, bounds_lnglat):
    mvt_layers = []
    for feature_layer in feature_layers:
        mvt_features = []
        for shape, props, feature_id in feature_layer['features']:
            mvt_feature = dict(
                geometry=shape,
                properties=props,
                id=feature_id,
            )
            mvt_features.append(mvt_feature)
        mvt_layer = dict(
            name=feature_layer['name'],
            features=mvt_features,
        )
        mvt_layers.append(mvt_layer)
    mvt_encode(fp, mvt_layers, bounds_merc)


def format_vtm(fp, feature_layers, zoom, bounds_merc, bounds_lnglat):
    vtm_encode(fp, feature_layers)


supports_shapely_geom = True
json_format = OutputFormat('JSON', 'json', 'application/json', format_json, 1,
                           supports_shapely_geom)
topojson_format = OutputFormat('TopoJSON', 'topojson', 'application/json',
                               format_topojson, 2, supports_shapely_geom)
# TODO image/png mimetype? app doesn't work unless image/png?
vtm_format = OutputFormat('OpenScienceMap', 'vtm', 'image/png', format_vtm, 3,
                          not supports_shapely_geom)
mvt_format = OutputFormat('MVT', 'mvt', 'application/x-protobuf',
                          format_mvt, 4, supports_shapely_geom)
# buffered mvt - same exact format as mvt, exception for extension and
# also has separate buffer config
mvtb_format = OutputFormat('MVT Buffered', 'mvtb', 'application/x-protobuf',
                           format_mvt, 4, supports_shapely_geom)
# package of tiles as a metatile zip
zip_format = OutputFormat('ZIP Metatile', 'zip', 'application/zip',
                          None, None, None)

extension_to_format = dict(
    json=json_format,
    topojson=topojson_format,
    vtm=vtm_format,
    mvt=mvt_format,
    mvtb=mvtb_format,
    zip=zip_format
)

name_to_format = {
    'JSON': json_format,
    'OpenScienceMap': vtm_format,
    'TopoJSON': topojson_format,
    'MVT': mvt_format,
    'MVT Buffered': mvtb_format,
    'ZIP Metatile': zip_format
}


def lookup_format_by_extension(extension):
    return extension_to_format.get(extension)


def lookup_format_by_name(name):
    return name_to_format.get(name)
