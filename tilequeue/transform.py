from shapely import geometry
from shapely.wkb import dumps
from tilequeue.format import json_format
from tilequeue.format import mvt_format
from tilequeue.format import topojson_format
from tilequeue.format import vtm_format
from TileStache.Goodies.VecTiles.ops import transform
import math


half_circumference_meters = 20037508.342789244


def mercator_point_to_wgs84(point):
    x, y = point

    x /= half_circumference_meters
    y /= half_circumference_meters

    y = (2 * math.atan(math.exp(y * math.pi)) - (math.pi / 2)) / math.pi

    x *= 180
    y *= 180

    return x, y


def rescale_point(bounds, scale):
    minx, miny, maxx, maxy = bounds

    def fn(point):
        x, y = point

        xfac = scale / (maxx - minx)
        yfac = scale / (maxy - miny)
        x = xfac * (x - minx)
        y = yfac * (y - miny)

        return x, y

    return fn


def apply_to_all_coords(fn):
    return lambda shape: transform(shape, fn)


# returns a geometry which is the given bounds expanded by `factor`. that is,
# if the original shape was a 1x1 box, the new one will be `factor`x`factor`
# box, with the same centroid as the original box.
def calculate_padded_bounds(factor, bounds):
    min_x, min_y, max_x, max_y = bounds
    dx = 0.5 * (max_x - min_x) * (factor - 1.0)
    dy = 0.5 * (max_y - min_y) * (factor - 1.0)
    return geometry.box(min_x - dx, min_y - dy, max_x + dx, max_y + dy)


def transform_feature_layers_shape(feature_layers, format, scale,
                                   unpadded_bounds, padded_bounds, coord):
    if format in (json_format, topojson_format):
        transform_fn = apply_to_all_coords(mercator_point_to_wgs84)
    elif format in (mvt_format, vtm_format):
        transform_fn = apply_to_all_coords(
            rescale_point(unpadded_bounds, scale))
    else:
        # in case we add a new format, default to no transformation
        transform_fn = lambda shape: shape

    shape_unpadded_bounds = geometry.box(*unpadded_bounds)
    is_vtm_format = format == vtm_format

    transformed_feature_layers = []
    for feature_layer in feature_layers:
        transformed_features = []
        layer_datum = feature_layer['layer_datum']
        is_clipped = layer_datum['is_clipped']
        clip_factor = layer_datum.get('clip_factor', 1.0)
        layer_padded_bounds = \
            calculate_padded_bounds(clip_factor, unpadded_bounds)

        for shape, props, feature_id in feature_layer['features']:

            if not is_vtm_format:
                # for non vtm formats, we need to explicitly check if
                # the geometry intersects with the unpadded bounds
                if not shape_unpadded_bounds.intersects(shape):
                    continue
                # now we know that we should include the geometry, but
                # if the geometry should be clipped, we'll clip to the
                # layer-specific padded bounds
                if is_clipped:
                    shape = shape.intersection(layer_padded_bounds)

            # perform the format specific geometry transformations
            shape = transform_fn(shape)

            # the formatters all expect wkb
            wkb = dumps(shape)

            transformed_features.append((wkb, props, feature_id))

        transformed_feature_layer = dict(
            name=feature_layer['name'],
            features=transformed_features,
            layer_datum=layer_datum,
        )
        transformed_feature_layers.append(transformed_feature_layer)

    return transformed_feature_layers
