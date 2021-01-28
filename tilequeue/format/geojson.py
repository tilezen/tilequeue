from math import ceil
from math import log
import ujson as json
import shapely.geometry
import shapely.ops
import shapely.wkb

precisions = [int(ceil(log(1 << zoom + 8+2) / log(10)) - 2)
              for zoom in range(17)]
# at z16, need to be more precise for metatiling
precisions[16] = 8


class JsonFeatureCreator(object):

    def __init__(self, precision=None):
        self.precision = precision

    def _trim_precision(self, x, y, z=None):
        return round(x, self.precision), round(y, self.precision)

    def __call__(self, feature):
        assert len(feature) == 3
        wkb_or_shape, props, fid = feature
        if isinstance(wkb_or_shape, shapely.geometry.base.BaseGeometry):
            shape = wkb_or_shape
        else:
            shape = shapely.wkb.loads(wkb_or_shape)

        if self.precision:
            truncated_precision_shape = shapely.ops.transform(
                self._trim_precision, shape)
            if truncated_precision_shape.is_valid:
                shape = truncated_precision_shape

        geometry = shape.__geo_interface__
        result = dict(type='Feature', properties=props, geometry=geometry)
        if fid is not None:
            result['id'] = fid
        return result


def create_layer_feature_collection(features, precision):
    create_json_feature = JsonFeatureCreator(precision)
    fs = map(create_json_feature, features)
    feature_collection = dict(type='FeatureCollection', features=fs)
    return feature_collection


def precision_for_zoom(zoom):
    precision_idx = zoom if 0 <= zoom < len(precisions) else -1
    precision = precisions[precision_idx]
    return precision


def encode_single_layer(out, features, zoom):
    """
    Encode a list of (WKB|shapely, property dict, id) features into a
    GeoJSON stream.

    If no id is available, pass in None

    Geometries in the features list are assumed to be lon, lats.
    """
    precision = precision_for_zoom(zoom)
    fs = create_layer_feature_collection(features, precision)
    json.dump(fs, out)


def encode_multiple_layers(out, features_by_layer, zoom):
    """
    features_by_layer should be a dict: layer_name -> feature tuples
    """
    precision = precision_for_zoom(zoom)
    geojson = {}
    for layer_name, features in features_by_layer.items():
        fs = create_layer_feature_collection(features, precision)
        geojson[layer_name] = fs
    json.dump(geojson, out)
