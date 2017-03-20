from numbers import Number
from shapely import geometry
from shapely.ops import transform
from shapely.wkb import dumps
from tilequeue.format import json_format
from tilequeue.format import topojson_format
from tilequeue.format import vtm_format
from tilequeue.tile import bounds_buffer
from tilequeue.tile import normalize_geometry_type
import math


half_circumference_meters = 20037508.342789244


def mercator_point_to_lnglat(x, y, z=None):
    x /= half_circumference_meters
    y /= half_circumference_meters

    y = (2 * math.atan(math.exp(y * math.pi)) - (math.pi / 2)) / math.pi

    x *= 180
    y *= 180

    return x, y


def rescale_point(bounds, scale):
    minx, miny, maxx, maxy = bounds

    def fn(x, y, z=None):
        xfac = scale / (maxx - minx)
        yfac = scale / (maxy - miny)
        x = xfac * (x - minx)
        y = yfac * (y - miny)

        return round(x), round(y)

    return fn


def apply_to_all_coords(fn):
    return lambda shape: transform(fn, shape)


# returns a geometry which is the given bounds expanded by `factor`. that is,
# if the original shape was a 1x1 box, the new one will be `factor`x`factor`
# box, with the same centroid as the original box.
def calculate_padded_bounds(factor, bounds):
    min_x, min_y, max_x, max_y = bounds
    dx = 0.5 * (max_x - min_x) * (factor - 1.0)
    dy = 0.5 * (max_y - min_y) * (factor - 1.0)
    return geometry.box(min_x - dx, min_y - dy, max_x + dx, max_y + dy)


# function which returns its argument, used to assign to a function variable
# to use as a null transform. flake8 insists that it be named and not a
# lambda.
def _noop(shape):
    return shape


def calc_buffered_bounds(
        format, bounds, meters_per_pixel_dim, layer_name, geometry_type,
        buffer_cfg):
    """
    Calculate the buffered bounds per format per layer based on config.
    """

    if not buffer_cfg:
        return bounds

    format_buffer_cfg = buffer_cfg.get(format.extension)
    if format_buffer_cfg is None:
        return bounds

    geometry_type = normalize_geometry_type(geometry_type)

    per_layer_cfg = format_buffer_cfg.get('layer', {}).get(layer_name)
    if per_layer_cfg is not None:
        layer_geom_pixels = per_layer_cfg.get(geometry_type)
        if layer_geom_pixels is not None:
            assert isinstance(layer_geom_pixels, Number)
            result = bounds_buffer(
                bounds, meters_per_pixel_dim * layer_geom_pixels)
            return result

    by_geometry_pixels = format_buffer_cfg.get('geometry', {}).get(
        geometry_type)
    if by_geometry_pixels is not None:
        assert isinstance(by_geometry_pixels, Number)
        result = bounds_buffer(
            bounds, meters_per_pixel_dim * by_geometry_pixels)
        return result

    return bounds


def _intersect_multipolygon(shape, tile_bounds, clip_bounds):
    """
    Return the parts of the MultiPolygon shape which overlap the tile_bounds,
    each clipped to the clip_bounds. This can be used to extract only the
    parts of a multipolygon which are actually visible in the tile, while
    keeping those parts which extend beyond the tile clipped to avoid huge
    polygons.
    """

    polys = []
    for poly in shape.geoms:
        if tile_bounds.intersects(poly):
            if not clip_bounds.contains(poly):
                poly = clip_bounds.intersection(poly)

            # the intersection operation can make the resulting polygon
            # invalid. including it in a MultiPolygon would make that
            # invalid too. instead, we skip it, and hope it wasn't too
            # important.
            if not poly.is_valid:
                continue

            if poly.type == 'Polygon':
                polys.append(poly)
            elif poly.type == 'MultiPolygon':
                polys.extend(poly.geoms)

    return geometry.MultiPolygon(polys)


def _clip_shape(shape, buffer_padded_bounds, is_clipped, clip_factor):
    """
    Return the shape clipped to a clip_factor expansion of buffer_padded_bounds
    if is_clipped is True. Otherwise return the original shape, or None if the
    shape does not intersect buffer_padded_bounds at all.

    This is used to reduce the size of the geometries which are encoded in the
    tiles by removing things which aren't in the tile, and clipping those which
    are to the clip_factor expanded bounding box.
    """

    shape_buf_bounds = geometry.box(*buffer_padded_bounds)

    if not shape_buf_bounds.intersects(shape):
        return None

    if is_clipped:
        # now we know that we should include the geometry, but
        # if the geometry should be clipped, we'll clip to the
        # layer-specific padded bounds
        layer_padded_bounds = calculate_padded_bounds(
            clip_factor, buffer_padded_bounds)

        if shape.type == 'MultiPolygon':
            shape = _intersect_multipolygon(
                shape, shape_buf_bounds, layer_padded_bounds)
        else:
            shape = shape.intersection(layer_padded_bounds)

    return shape


def transform_feature_layers_shape(
        feature_layers, format, scale, unpadded_bounds,
        meters_per_pixel_dim, buffer_cfg):
    if format in (json_format, topojson_format):
        transform_fn = apply_to_all_coords(mercator_point_to_lnglat)
    elif format == vtm_format:
        transform_fn = apply_to_all_coords(
            rescale_point(unpadded_bounds, scale))
    else:
        # mvt and unknown formats get no geometry transformation
        transform_fn = _noop

    # shape_unpadded_bounds = geometry.box(*unpadded_bounds)

    transformed_feature_layers = []
    for feature_layer in feature_layers:
        layer_name = feature_layer['name']
        transformed_features = []
        layer_datum = feature_layer['layer_datum']
        is_clipped = layer_datum['is_clipped']
        clip_factor = layer_datum.get('clip_factor', 1.0)

        for shape, props, feature_id in feature_layer['features']:

            if shape.is_empty or shape.type == 'GeometryCollection':
                continue

            buffer_padded_bounds = calc_buffered_bounds(
                format, unpadded_bounds, meters_per_pixel_dim, layer_name,
                shape.type, buffer_cfg)

            shape = _clip_shape(
                shape, buffer_padded_bounds, is_clipped, clip_factor)
            if shape is None or shape.is_empty:
                continue

            # perform the format specific geometry transformations
            shape = transform_fn(shape)

            if format.supports_shapely_geometry:
                geom = shape
            else:
                geom = dumps(shape)

            transformed_features.append((geom, props, feature_id))

        transformed_feature_layer = dict(
            name=feature_layer['name'],
            features=transformed_features,
            layer_datum=layer_datum,
        )
        transformed_feature_layers.append(transformed_feature_layer)

    return transformed_feature_layers
