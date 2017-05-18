from collections import namedtuple
from cStringIO import StringIO
from shapely.geometry import MultiPolygon
from shapely import geometry
from shapely.wkb import loads
from tilequeue.config import create_query_bounds_pad_fn
from tilequeue.tile import calc_meters_per_pixel_dim
from tilequeue.tile import coord_to_mercator_bounds
from tilequeue.tile import normalize_geometry_type
from tilequeue.transform import mercator_point_to_lnglat
from tilequeue.transform import transform_feature_layers_shape
from zope.dottedname.resolve import resolve
from sys import getsizeof


def make_transform_fn(transform_fns):
    if not transform_fns:
        return None

    def transform_fn(shape, properties, fid, zoom):
        for fn in transform_fns:
            shape, properties, fid = fn(shape, properties, fid, zoom)
        return shape, properties, fid
    return transform_fn


def resolve_transform_fns(fn_dotted_names):
    if not fn_dotted_names:
        return None
    return map(resolve, fn_dotted_names)


def _sizeof(val):
    size = 0

    if isinstance(val, dict):
        for k, v in val.items():
            size += len(k) + _sizeof(v)
    elif isinstance(val, list):
        for v in val:
            size += _sizeof(v)
    elif isinstance(val, (str, bytes, unicode)):
        size += len(val)
    else:
        size += getsizeof(val)

    return size


# shared context for all the post-processor functions. this single object can
# be passed around rather than needing all the parameters to be explicit.
Context = namedtuple('Context', [
    'feature_layers',    # the feature layers list
    'nominal_zoom',      # the zoom level to use for styling (display scale)
    'unpadded_bounds',   # the latlon bounds of the tile
    'params',            # user configuration parameters
    'resources',         # resources declared in config
])


# post-process all the layers simultaneously, which allows new
# layers to be created from processing existing ones (e.g: for
# computed centroids) or modifying layers based on the contents
# of other layers (e.g: projecting attributes, deleting hidden
# features, etc...)
def _postprocess_data(
        feature_layers, post_process_data, nominal_zoom, unpadded_bounds):

    for step in post_process_data:
        fn = resolve(step['fn_name'])

        ctx = Context(
            feature_layers=feature_layers,
            nominal_zoom=nominal_zoom,
            unpadded_bounds=unpadded_bounds,
            params=step['params'],
            resources=step['resources'],
        )

        layer = fn(ctx)
        feature_layers = ctx.feature_layers
        if layer is not None:
            for index, feature_layer in enumerate(feature_layers):
                layer_datum = feature_layer['layer_datum']
                layer_name = layer_datum['name']
                if layer_name == layer['layer_datum']['name']:
                    feature_layers[index] = layer
                    layer = None
                    break
            # if this layer isn't replacing an old layer, then
            # append it.
            if layer is not None:
                feature_layers.append(layer)

    return feature_layers


def _cut_coord(
        feature_layers, unpadded_bounds, meters_per_pixel_dim, buffer_cfg):
    cut_feature_layers = []
    for feature_layer in feature_layers:
        features = feature_layer['features']
        padded_bounds_fn = create_query_bounds_pad_fn(
            buffer_cfg, feature_layer['name'])
        padded_bounds = padded_bounds_fn(unpadded_bounds, meters_per_pixel_dim)

        cut_features = []
        for feature in features:
            shape, props, feature_id = feature

            geom_type_bounds = padded_bounds[
                normalize_geometry_type(shape.type)]
            shape_padded_bounds = geometry.box(*geom_type_bounds)
            if not shape_padded_bounds.intersects(shape):
                continue
            props_copy = props.copy()
            cut_feature = shape, props_copy, feature_id

            cut_features.append(cut_feature)

        cut_feature_layer = dict(
            name=feature_layer['name'],
            layer_datum=feature_layer['layer_datum'],
            features=cut_features,
            padded_bounds=padded_bounds,
        )
        cut_feature_layers.append(cut_feature_layer)

    return cut_feature_layers


def _make_valid_if_necessary(shape):
    """
    attempt to correct invalid shapes if necessary

    After simplification, even when preserving topology, invalid
    shapes can be returned. This appears to only occur with polygon
    types. As an optimization, we only check if the polygon types are
    valid.
    """
    if shape.type in ('Polygon', 'MultiPolygon') and not shape.is_valid:
        shape = shape.buffer(0)

        # return value from buffer is usually valid, but it's
        # not clear from the docs whether this is guaranteed,
        # so return None if not.
        if not shape.is_valid:
            return None

    return shape


# returns none if the shape is not visible given the number of meters
# per pixel. for multipolygons, will filter out any subpolygons that
# should not be visible, which means that the shape could have been
# altered
def _visible_shape(shape, area_threshold_meters):
    if shape is None:
        return None
    elif shape.type == 'MultiPolygon':
        visible_shapes = []
        for subshape in shape.geoms:
            subshape = _visible_shape(subshape, area_threshold_meters)
            if subshape:
                visible_shapes.append(subshape)
        if visible_shapes:
            return MultiPolygon(visible_shapes)
        else:
            return None
    elif shape.type == 'Polygon':
        shape_meters = shape.area
        return shape if shape_meters >= area_threshold_meters else None
    else:
        return shape


def _create_formatted_tile(
        feature_layers, format, scale, unpadded_bounds, unpadded_bounds_lnglat,
        coord, nominal_zoom, layer, meters_per_pixel_dim, buffer_cfg):

    # perform format specific transformations
    transformed_feature_layers = transform_feature_layers_shape(
        feature_layers, format, scale, unpadded_bounds,
        meters_per_pixel_dim, buffer_cfg)

    # use the formatter to generate the tile
    tile_data_file = StringIO()
    format.format_tile(
        tile_data_file, transformed_feature_layers, nominal_zoom,
        unpadded_bounds, unpadded_bounds_lnglat)
    tile = tile_data_file.getvalue()

    formatted_tile = dict(format=format, tile=tile, coord=coord, layer=layer)
    return formatted_tile


def process_coord_no_format(
        feature_layers, nominal_zoom, unpadded_bounds, post_process_data):

    extra_data = dict(size={})
    processed_feature_layers = []
    # filter, and then transform each layer as necessary
    for feature_layer in feature_layers:
        layer_datum = feature_layer['layer_datum']
        layer_name = layer_datum['name']
        geometry_types = layer_datum['geometry_types']
        padded_bounds = feature_layer['padded_bounds']

        transform_fn_names = layer_datum['transform_fn_names']
        if transform_fn_names:
            transform_fns = resolve_transform_fns(transform_fn_names)
            layer_transform_fn = make_transform_fn(transform_fns)
        else:
            layer_transform_fn = None

        features = []
        features_size = 0
        for row in feature_layer['features']:
            wkb = row.pop('__geometry__')
            shape = loads(wkb)

            if shape.is_empty:
                continue

            if not shape.is_valid:
                continue

            if geometry_types is not None:
                if shape.type not in geometry_types:
                    continue

            # since a bounding box intersection is used, we
            # perform a more accurate check here to filter out
            # any extra features
            # the formatter specific transformations will take
            # care of any additional filtering
            geom_type_bounds = padded_bounds[
                normalize_geometry_type(shape.type)]
            shape_padded_bounds = geometry.box(*geom_type_bounds)
            if not shape_padded_bounds.intersects(shape):
                continue

            feature_id = row.pop('__id__')
            props = dict()
            feature_size = getsizeof(feature_id) + len(wkb)
            for k, v in row.iteritems():
                if k == 'mz_properties':
                    for output_key, output_val in v.items():
                        if output_val is not None:
                            # all other tags are utf8 encoded, encode
                            # these the same way to be consistent
                            if isinstance(output_key, unicode):
                                output_key = output_key.encode('utf-8')
                            if isinstance(output_val, unicode):
                                output_val = output_val.encode('utf-8')
                            props[output_key] = output_val
                            feature_size += len(output_key) + \
                                _sizeof(output_val)
                else:
                    props[k] = v
                    feature_size += len(k) + _sizeof(v)
                features_size += feature_size

            extra_data['size'][layer_datum['name']] = features_size

            if layer_transform_fn:
                shape, props, feature_id = layer_transform_fn(
                    shape, props, feature_id, nominal_zoom)

            feature = shape, props, feature_id
            features.append(feature)

        sort_fn_name = layer_datum['sort_fn_name']
        if sort_fn_name:
            sort_fn = resolve(sort_fn_name)
            features = sort_fn(features, nominal_zoom)

        feature_layer = dict(
            name=layer_name,
            features=features,
            layer_datum=layer_datum,
            padded_bounds=padded_bounds,
        )
        processed_feature_layers.append(feature_layer)

    # post-process data here, before it gets formatted
    processed_feature_layers = _postprocess_data(
        processed_feature_layers, post_process_data, nominal_zoom,
        unpadded_bounds)

    return processed_feature_layers, extra_data


def _format_feature_layers(
        processed_feature_layers, coord, nominal_zoom, formats,
        unpadded_bounds, scale, buffer_cfg):

    meters_per_pixel_dim = calc_meters_per_pixel_dim(nominal_zoom)

    # topojson formatter expects bounds to be in lnglat
    unpadded_bounds_lnglat = (
        mercator_point_to_lnglat(unpadded_bounds[0], unpadded_bounds[1]) +
        mercator_point_to_lnglat(unpadded_bounds[2], unpadded_bounds[3]))

    # now, perform the format specific transformations
    # and format the tile itself
    formatted_tiles = []
    layer = 'all'
    for format in formats:
        formatted_tile = _create_formatted_tile(
            processed_feature_layers, format, scale, unpadded_bounds,
            unpadded_bounds_lnglat, coord, nominal_zoom, layer,
            meters_per_pixel_dim, buffer_cfg)
        formatted_tiles.append(formatted_tile)

    return formatted_tiles


def _cut_child_tiles(
        feature_layers, cut_coord, nominal_zoom, formats, scale, buffer_cfg):

    unpadded_cut_bounds = coord_to_mercator_bounds(cut_coord)
    meters_per_pixel_dim = calc_meters_per_pixel_dim(nominal_zoom)

    cut_feature_layers = _cut_coord(
        feature_layers, unpadded_cut_bounds, meters_per_pixel_dim, buffer_cfg)

    return _format_feature_layers(
        cut_feature_layers, cut_coord, nominal_zoom, formats,
        unpadded_cut_bounds, scale, buffer_cfg)


def format_coord(
        coord, nominal_zoom, processed_feature_layers, formats,
        unpadded_bounds, cut_coords, buffer_cfg, extra_data, scale=4096):

    coord_formatted_tiles = _format_feature_layers(
        processed_feature_layers, coord, nominal_zoom, formats,
        unpadded_bounds, scale, buffer_cfg)

    children_formatted_tiles = []
    if cut_coords:
        for cut_coord in cut_coords:
            child_tiles = _cut_child_tiles(
                processed_feature_layers, cut_coord, nominal_zoom, formats,
                scale, buffer_cfg)
            children_formatted_tiles.extend(child_tiles)

    all_formatted_tiles = coord_formatted_tiles + children_formatted_tiles
    return all_formatted_tiles, extra_data


# given a coord and the raw feature layers results from the database,
# filter, transform, sort, post-process and then format according to
# each formatter. this is the entry point from the worker process
#
# the nominal zoom is the "display scale" zoom, which may not correspond
# to actual tile coordinates in future versions of the code. it just
# becomes a measure of the scale between tile features and intended
# display size.
def process_coord(coord, nominal_zoom, feature_layers, post_process_data,
                  formats, unpadded_bounds, cut_coords, buffer_cfg,
                  scale=4096):
    processed_feature_layers, extra_data = process_coord_no_format(
        feature_layers, nominal_zoom, unpadded_bounds, post_process_data)

    all_formatted_tiles, extra_data = format_coord(
        coord, nominal_zoom, processed_feature_layers, formats,
        unpadded_bounds, cut_coords, buffer_cfg, extra_data, scale)

    return all_formatted_tiles, extra_data
