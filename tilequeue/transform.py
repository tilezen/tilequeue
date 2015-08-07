from shapely import geometry
from shapely.wkb import dumps
from tilequeue.format import json_format
from tilequeue.format import mvt_format
from tilequeue.format import topojson_format
from tilequeue.format import vtm_format
from tilequeue.tile import tolerance_for_zoom
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


def make_valid_if_necessary(shape):
    """
    attempt to correct invalid shapes if necessary

    After simplification, even when preserving topology, invalid
    shapes can be returned. This appears to only occur with polygon
    types. As an optimization, we only check if the polygon types are
    valid.
    """
    if shape.type in ('Polygon', 'MultiPolygon') and not shape.is_valid:
        shape = shape.buffer(0)
    return shape


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

    is_vtm_format = format == vtm_format
    format_padded_bounds = geometry.box(
        *(padded_bounds if is_vtm_format else unpadded_bounds))

    transformed_feature_layers = []
    for feature_layer in feature_layers:
        features = feature_layer['features']
        transformed_features = []

        layer_datum = feature_layer['layer_datum']
        is_clipped = layer_datum['is_clipped']

        # The logic behind simplifying before intersecting rather than the
        # other way around is extensively explained here:
        # https://github.com/mapzen/TileStache/blob/d52e54975f6ec2d11f63db13934047e7cd5fe588/TileStache/Goodies/VecTiles/server.py#L509,L527
        simplify_before_intersect = layer_datum['simplify_before_intersect']

        for shape, props, feature_id in features:
            # perform any simplification as necessary
            tolerance = tolerance_for_zoom(coord.zoom)
            simplify_until = layer_datum['simplify_until']
            suppress_simplification = layer_datum['suppress_simplification']
            should_simplify = coord.zoom not in suppress_simplification and \
                coord.zoom < simplify_until

            if should_simplify and simplify_before_intersect:
                # To reduce the performance hit of simplifying potentially huge
                # geometries to extract only a small portion of them when
                # cutting out the actual tile, we cut out a slightly larger
                # bounding box first. See here for an explanation:
                # https://github.com/mapzen/TileStache/blob/d52e54975f6ec2d11f63db13934047e7cd5fe588/TileStache/Goodies/VecTiles/server.py#L509,L527

                min_x, min_y, max_x, max_y = format_padded_bounds.bounds
                gutter_bbox_size = (max_x - min_x) * 0.1
                gutter_bbox = geometry.box(
                    min_x - gutter_bbox_size,
                    min_y - gutter_bbox_size,
                    max_x + gutter_bbox_size,
                    max_y + gutter_bbox_size)
                clipped_shape = shape.intersection(gutter_bbox)
                simplified_shape = clipped_shape.simplify(
                    tolerance, preserve_topology=True)
                shape = make_valid_if_necessary(simplified_shape)

            if is_vtm_format:
                if is_clipped:
                    shape = shape.intersection(format_padded_bounds)
            else:
                # for non vtm formats, we need to explicitly check if
                # the geometry intersects with the unpadded bounds
                if not format_padded_bounds.intersects(shape):
                    continue
                # now we know that we should include the geometry, but
                # if the geometry should be clipped, we'll clip to the
                # unpadded bounds
                if is_clipped:
                    shape = shape.intersection(format_padded_bounds)

            if should_simplify and not simplify_before_intersect:
                simplified_shape = shape.simplify(tolerance,
                                                  preserve_topology=True)
                shape = make_valid_if_necessary(simplified_shape)

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
