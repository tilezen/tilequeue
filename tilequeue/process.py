from collections import namedtuple
from cStringIO import StringIO
from shapely.geometry import MultiPolygon
from shapely import geometry
from shapely.wkb import loads
from tilequeue.tile import calc_meters_per_pixel_dim
from tilequeue.tile import coord_to_mercator_bounds
from tilequeue.transform import mercator_point_to_lnglat
from tilequeue.transform import transform_feature_layers_shape
from zope.dottedname.resolve import resolve


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


def _preprocess_data(feature_layers):
    preproc_feature_layers = []

    for feature_layer in feature_layers:
        layer_datum = feature_layer['layer_datum']
        geometry_types = layer_datum['geometry_types']
        padded_bounds = feature_layer['padded_bounds']
        shape_padded_bounds = geometry.box(*padded_bounds)

        features = []
        for row in feature_layer['features']:
            wkb = bytes(row.pop('__geometry__'))
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
            if not shape_padded_bounds.intersects(shape):
                continue

            feature_id = row.pop('__id__')
            props = dict()
            for k, v in row.iteritems():
                if v is None:
                    continue
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
                else:
                    props[k] = v

            feature = shape, props, feature_id
            features.append(feature)

        preproc_feature_layer = dict(
            name=layer_datum['name'],
            layer_datum=layer_datum,
            features=features,
            padded_bounds=padded_bounds,
        )
        preproc_feature_layers.append(preproc_feature_layer)

    return preproc_feature_layers


# shared context for all the post-processor functions. this single object can
# be passed around rather than needing all the parameters to be explicit.
Context = namedtuple('Context', [
    'feature_layers',    # the feature layers list
    'tile_coord',        # the original tile coordinate obj
    'unpadded_bounds',   # the latlon bounds of the tile
    'params',            # user configuration parameters
    'resources',         # resources declared in config
    'buffer_cfg',        # format buffer config
])


# post-process all the layers simultaneously, which allows new
# layers to be created from processing existing ones (e.g: for
# computed centroids) or modifying layers based on the contents
# of other layers (e.g: projecting attributes, deleting hidden
# features, etc...)
def _postprocess_data(
        feature_layers, post_process_data, tile_coord, unpadded_bounds,
        buffer_cfg):

    for step in post_process_data:
        fn = resolve(step['fn_name'])

        ctx = Context(
            feature_layers=feature_layers,
            tile_coord=tile_coord,
            unpadded_bounds=unpadded_bounds,
            params=step['params'],
            resources=step['resources'],
            buffer_cfg=buffer_cfg,
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
    from tilequeue.command import _create_query_bounds_pad_fn
    cut_feature_layers = []
    for feature_layer in feature_layers:
        features = feature_layer['features']
        padded_bounds_fn = _create_query_bounds_pad_fn(
            buffer_cfg, feature_layer['name'])
        padded_bounds = padded_bounds_fn(unpadded_bounds, meters_per_pixel_dim)
        shape_padded_bounds = geometry.box(*padded_bounds)
        cut_features = []
        for feature in features:
            shape, props, feature_id = feature

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
        coord, layer, meters_per_pixel_dim, buffer_cfg):

    # perform format specific transformations
    transformed_feature_layers = transform_feature_layers_shape(
        feature_layers, format, scale, unpadded_bounds, coord,
        meters_per_pixel_dim, buffer_cfg)

    # use the formatter to generate the tile
    tile_data_file = StringIO()
    format.format_tile(tile_data_file, transformed_feature_layers, coord,
                       unpadded_bounds, unpadded_bounds_lnglat)
    tile = tile_data_file.getvalue()

    formatted_tile = dict(format=format, tile=tile, coord=coord, layer=layer)
    return formatted_tile


def _process_feature_layers(
        feature_layers, coord, post_process_data, formats, unpadded_bounds,
        scale, layers_to_format, buffer_cfg):

    processed_feature_layers = []
    # filter, and then transform each layer as necessary
    for feature_layer in feature_layers:
        layer_datum = feature_layer['layer_datum']
        layer_name = layer_datum['name']
        features = feature_layer['features']

        transform_fn_names = layer_datum['transform_fn_names']
        if transform_fn_names:
            transform_fns = resolve_transform_fns(transform_fn_names)
            layer_transform_fn = make_transform_fn(transform_fns)
        else:
            layer_transform_fn = None

        # perform any specific layer transformations
        if layer_transform_fn is None:
            processed_features = features
        else:
            processed_features = []
            for feature in features:
                shape, props, feature_id = feature
                shape, props, feature_id = layer_transform_fn(
                    shape, props, feature_id, coord.zoom)
                transformed_feature = shape, props, feature_id
                processed_features.append(transformed_feature)

        sort_fn_name = layer_datum['sort_fn_name']
        if sort_fn_name:
            sort_fn = resolve(sort_fn_name)
            processed_features = sort_fn(processed_features, coord.zoom)

        feature_layer = dict(
            name=layer_name,
            features=processed_features,
            layer_datum=layer_datum,
            padded_bounds=feature_layer['padded_bounds'],
        )
        processed_feature_layers.append(feature_layer)

    # post-process data here, before it gets formatted
    processed_feature_layers = _postprocess_data(
        processed_feature_layers, post_process_data, coord, unpadded_bounds,
        buffer_cfg)

    meters_per_pixel_dim = calc_meters_per_pixel_dim(coord.zoom)

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
            unpadded_bounds_lnglat, coord, layer, meters_per_pixel_dim,
            buffer_cfg)
        formatted_tiles.append(formatted_tile)

    # this assumes that we only store single layers, and no combinations
    for layer, formats, zoom_start, zoom_until in layers_to_format:
        if not (zoom_start <= coord.zoom <= zoom_until):
            continue
        for feature_layer in processed_feature_layers:
            if feature_layer['name'] == layer:
                pruned_feature_layers = [feature_layer]
                for format in formats:
                    formatted_tile = _create_formatted_tile(
                        pruned_feature_layers, format, scale, unpadded_bounds,
                        unpadded_bounds_lnglat, coord, layer,
                        meters_per_pixel_dim, buffer_cfg)
                    formatted_tiles.append(formatted_tile)
                    break

    return formatted_tiles


# given a coord and the raw feature layers results from the database,
# filter, transform, sort, post-process and then format according to
# each formatter. this is the entry point from the worker process
def process_coord(coord, feature_layers, post_process_data, formats,
                  unpadded_bounds, cut_coords, layers_to_format,
                  buffer_cfg, scale=4096):
    feature_layers = _preprocess_data(feature_layers)

    children_formatted_tiles = []
    if cut_coords:
        for cut_coord in cut_coords:
            unpadded_cut_bounds = coord_to_mercator_bounds(cut_coord)

            meters_per_pixel_dim = calc_meters_per_pixel_dim(cut_coord.zoom)
            child_feature_layers = _cut_coord(
                feature_layers, unpadded_cut_bounds, meters_per_pixel_dim,
                buffer_cfg)
            child_formatted_tiles = _process_feature_layers(
                child_feature_layers, cut_coord, post_process_data, formats,
                unpadded_cut_bounds, scale, layers_to_format, buffer_cfg)
            children_formatted_tiles.extend(child_formatted_tiles)

    coord_formatted_tiles = _process_feature_layers(
        feature_layers, coord, post_process_data, formats, unpadded_bounds,
        scale, layers_to_format, buffer_cfg)
    all_formatted_tiles = coord_formatted_tiles + children_formatted_tiles
    return all_formatted_tiles
