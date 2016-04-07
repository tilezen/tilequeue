from cStringIO import StringIO
from shapely.geometry import MultiPolygon
from shapely import geometry
from shapely.wkb import loads
from tilequeue.tile import coord_to_mercator_bounds
from tilequeue.tile import pad_bounds_for_zoom
from tilequeue.tile import tolerance_for_zoom
from tilequeue.transform import mercator_point_to_wgs84
from tilequeue.transform import transform_feature_layers_shape
from tilequeue.transform import calculate_padded_bounds
from TileStache.Config import loadClassPath
from TileStache.Goodies.VecTiles.server import make_transform_fn
from TileStache.Goodies.VecTiles.server import resolve_transform_fns
from collections import namedtuple


def _preprocess_data(feature_layers, shape_padded_bounds):
    preproc_feature_layers = []

    for feature_layer in feature_layers:
        layer_datum = feature_layer['layer_datum']
        geometry_types = layer_datum['geometry_types']

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
                    props.update(v)
                else:
                    props[k] = v

            feature = shape, props, feature_id
            features.append(feature)

        preproc_feature_layer = dict(
            name=layer_datum['name'],
            layer_datum=layer_datum,
            features=features)
        preproc_feature_layers.append(preproc_feature_layer)

    return preproc_feature_layers


# shared context for all the post-processor functions. this single object can
# be passed around rather than needing all the parameters to be explicit.
Context = namedtuple('Context',
                     ['feature_layers',    # the feature layers list
                      'tile_coord',        # the original tile coordinate obj
                      'unpadded_bounds',   # the latlon bounds of the tile
                      'padded_bounds',     # the padded bounds of the tile
                      'params',            # user configuration parameters
                      'resources'])        # resources declared in config


# post-process all the layers simultaneously, which allows new
# layers to be created from processing existing ones (e.g: for
# computed centroids) or modifying layers based on the contents
# of other layers (e.g: projecting attributes, deleting hidden
# features, etc...)
def _postprocess_data(feature_layers, post_process_data,
                      tile_coord, unpadded_bounds, padded_bounds):

    for step in post_process_data:
        fn = loadClassPath(step['fn_name'])

        ctx = Context(
            feature_layers=feature_layers,
            tile_coord=tile_coord,
            unpadded_bounds=unpadded_bounds,
            padded_bounds=padded_bounds,
            params=step['params'],
            resources=step['resources'])

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


def _cut_coord(feature_layers, shape_padded_bounds):
    cut_feature_layers = []
    for feature_layer in feature_layers:
        features = feature_layer['features']
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
            features=cut_features)
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


# radius from http://wiki.openstreetmap.org/wiki/Zoom_levels
earth_equatorial_radius_meters = 6372798.2
earth_equatorial_circumference_meters = 40041472.01586051


def _find_meters_per_pixel(zoom):
    meters_in_dimension = (earth_equatorial_circumference_meters /
                           (2 ** (zoom + 8)))
    meters_per_pixel = meters_in_dimension * meters_in_dimension
    return meters_per_pixel


# returns none if the shape is not visible given the number of meters
# per pixel. for multipolygons, will filter out any subpolygons that
# should not be visible, which means that the shape could have been
# altered
def _visible_shape(shape, meters_per_pixel):
    if shape is None:
        return None
    elif shape.type == 'MultiPolygon':
        visible_shapes = []
        for subshape in shape.geoms:
            subshape = _visible_shape(subshape, meters_per_pixel)
            if subshape:
                visible_shapes.append(subshape)
        if visible_shapes:
            return MultiPolygon(visible_shapes)
        else:
            return None
    elif shape.type == 'Polygon':
        shape_meters = shape.area
        return shape if shape_meters >= meters_per_pixel else None
    else:
        return shape


def _simplify_data(feature_layers, bounds, zoom):
    tolerance = tolerance_for_zoom(zoom)

    meters_per_pixel = _find_meters_per_pixel(zoom)

    simplified_feature_layers = []
    for feature_layer in feature_layers:
        simplified_features = []

        layer_datum = feature_layer['layer_datum']
        is_clipped = layer_datum['is_clipped']
        clip_factor = layer_datum.get('clip_factor', 1.0)
        layer_padded_bounds = \
            calculate_padded_bounds(clip_factor, bounds)

        # The logic behind simplifying before intersecting rather than the
        # other way around is extensively explained here:
        # https://github.com/mapzen/TileStache/blob/d52e54975f6ec2d11f63db13934047e7cd5fe588/TileStache/Goodies/VecTiles/server.py#L509,L527
        simplify_before_intersect = layer_datum['simplify_before_intersect']

        # perform any simplification as necessary
        simplify_start = layer_datum['simplify_start']
        simplify_until = 16
        should_simplify = simplify_start <= zoom < simplify_until

        for shape, props, feature_id in feature_layer['features']:

            if should_simplify and simplify_before_intersect:
                # To reduce the performance hit of simplifying potentially huge
                # geometries to extract only a small portion of them when
                # cutting out the actual tile, we cut out a slightly larger
                # bounding box first. See here for an explanation:
                # https://github.com/mapzen/TileStache/blob/d52e54975f6ec2d11f63db13934047e7cd5fe588/TileStache/Goodies/VecTiles/server.py#L509,L527

                min_x, min_y, max_x, max_y = layer_padded_bounds.bounds
                gutter_bbox_size = (max_x - min_x) * 0.1
                gutter_bbox = geometry.box(
                    min_x - gutter_bbox_size,
                    min_y - gutter_bbox_size,
                    max_x + gutter_bbox_size,
                    max_y + gutter_bbox_size)
                clipped_shape = shape.intersection(gutter_bbox)
                simplified_shape = clipped_shape.simplify(
                    tolerance, preserve_topology=True)
                shape = _make_valid_if_necessary(simplified_shape)

            if is_clipped:
                shape = shape.intersection(layer_padded_bounds)

            if should_simplify and not simplify_before_intersect:
                simplified_shape = shape.simplify(tolerance,
                                                  preserve_topology=True)
                shape = _make_valid_if_necessary(simplified_shape)

            # this could alter multipolygon geometries
            if zoom < simplify_until:
                shape = _visible_shape(shape, meters_per_pixel)

            # don't keep features which have been simplified to empty or
            # None.
            if shape is None or shape.is_empty:
                continue

            simplified_feature = shape, props, feature_id
            simplified_features.append(simplified_feature)

        simplified_feature_layer = dict(
            name=feature_layer['name'],
            features=simplified_features,
            layer_datum=layer_datum,
        )
        simplified_feature_layers.append(simplified_feature_layer)

    return simplified_feature_layers


def _create_formatted_tile(feature_layers, format, scale, unpadded_bounds,
                           padded_bounds, unpadded_bounds_wgs84, coord, layer):
    # perform format specific transformations
    transformed_feature_layers = transform_feature_layers_shape(
        feature_layers, format, scale, unpadded_bounds,
        padded_bounds, coord)

    # use the formatter to generate the tile
    tile_data_file = StringIO()
    format.format_tile(tile_data_file, transformed_feature_layers, coord,
                       unpadded_bounds, unpadded_bounds_wgs84)
    tile = tile_data_file.getvalue()

    formatted_tile = dict(format=format, tile=tile, coord=coord, layer=layer)
    return formatted_tile


def _process_feature_layers(feature_layers, coord, post_process_data,
                            formats, unpadded_bounds, padded_bounds,
                            scale, layers_to_format):
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
            sort_fn = loadClassPath(sort_fn_name)
            processed_features = sort_fn(processed_features, coord.zoom)

        feature_layer = dict(name=layer_name, features=processed_features,
                             layer_datum=layer_datum)
        processed_feature_layers.append(feature_layer)

    # post-process data here, before it gets formatted
    processed_feature_layers = _postprocess_data(
        processed_feature_layers, post_process_data, coord, unpadded_bounds,
        padded_bounds)

    # after post processing, perform simplification and clipping
    processed_feature_layers = _simplify_data(
        processed_feature_layers, padded_bounds, coord.zoom)

    # topojson formatter expects bounds to be in wgs84
    unpadded_bounds_wgs84 = (
        mercator_point_to_wgs84(unpadded_bounds[:2]) +
        mercator_point_to_wgs84(unpadded_bounds[2:4]))

    # now, perform the format specific transformations
    # and format the tile itself
    formatted_tiles = []
    layer = 'all'
    for format in formats:
        formatted_tile = _create_formatted_tile(
            processed_feature_layers, format, scale, unpadded_bounds,
            padded_bounds, unpadded_bounds_wgs84, coord, layer)
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
                        padded_bounds, unpadded_bounds_wgs84, coord, layer)
                    formatted_tiles.append(formatted_tile)
                    break

    return formatted_tiles


# given a coord and the raw feature layers results from the database,
# filter, transform, sort, post-process and then format according to
# each formatter. this is the entry point from the worker process
def process_coord(coord, feature_layers, post_process_data, formats,
                  unpadded_bounds, padded_bounds, cut_coords, layers_to_format,
                  scale=4096):
    shape_padded_bounds = geometry.box(*padded_bounds)
    feature_layers = _preprocess_data(feature_layers, shape_padded_bounds)

    children_formatted_tiles = []
    if cut_coords:
        for cut_coord in cut_coords:
            unpadded_cut_bounds = coord_to_mercator_bounds(cut_coord)
            padded_cut_bounds = pad_bounds_for_zoom(unpadded_cut_bounds,
                                                    cut_coord.zoom)

            shape_cut_padded_bounds = geometry.box(*padded_cut_bounds)
            child_feature_layers = _cut_coord(feature_layers,
                                              shape_cut_padded_bounds)
            child_formatted_tiles = _process_feature_layers(
                child_feature_layers, cut_coord, post_process_data, formats,
                unpadded_cut_bounds, padded_cut_bounds, scale,
                layers_to_format)
            children_formatted_tiles.extend(child_formatted_tiles)

    coord_formatted_tiles = _process_feature_layers(
        feature_layers, coord, post_process_data, formats, unpadded_bounds,
        padded_bounds, scale, layers_to_format)
    all_formatted_tiles = coord_formatted_tiles + children_formatted_tiles
    return all_formatted_tiles
