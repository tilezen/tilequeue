from cStringIO import StringIO
from shapely import geometry
from shapely.wkb import loads
from tilequeue.tile import coord_to_mercator_bounds
from tilequeue.tile import pad_bounds_for_zoom
from tilequeue.transform import mercator_point_to_wgs84
from tilequeue.transform import transform_feature_layers_shape
from TileStache.Config import loadClassPath
from TileStache.Goodies.VecTiles.server import make_transform_fn
from TileStache.Goodies.VecTiles.server import resolve_transform_fns


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
            props = dict((k, v) for k, v in row.items()
                         if v is not None)

            feature = shape, props, feature_id
            features.append(feature)

        preproc_feature_layer = dict(
            name=layer_datum['name'],
            layer_datum=layer_datum,
            features=features)
        preproc_feature_layers.append(preproc_feature_layer)

    return preproc_feature_layers

# post-process all the layers simulataneously, which allows new
# layers to be created from processing existing ones (e.g: for
# computed centroids) or modifying layers based on the contents
# of other layers (e.g: projecting attributes, deleting hidden
# features, etc...)
def _postprocess_data(feature_layers, post_process_data):

    for step in post_process_data:
        fn = loadClassPath(step['fn_name'])
        params = step['params']

        layer = fn(feature_layers, **params)
        if layer is not None:
            for index, feature_layer in enumerate(feature_layers):
                layer_datum = feature_layer['layer_datum']
                layer_name = layer_datum['name']
                if layer_name == layer['name']:
                    feature_layers[index] = layer
                    break

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


def _process_feature_layers(feature_layers, coord, post_process_data,
                            formats, unpadded_bounds, padded_bounds,
                            scale):
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
    processed_feature_layers = _postprocess_data(processed_feature_layers, post_process_data)

    # topojson formatter expects bounds to be in wgs84
    unpadded_bounds_merc = unpadded_bounds
    unpadded_bounds_wgs84 = (
        mercator_point_to_wgs84(unpadded_bounds[:2]) +
        mercator_point_to_wgs84(unpadded_bounds[2:4]))

    # now, perform the format specific transformations
    # and format the tile itself
    formatted_tiles = []
    for format in formats:
        # perform format specific transformations
        transformed_feature_layers = transform_feature_layers_shape(
            processed_feature_layers, format, scale, unpadded_bounds,
            padded_bounds, coord)

        # use the formatter to generate the tile
        tile_data_file = StringIO()
        format.format_tile(tile_data_file, transformed_feature_layers, coord,
                           unpadded_bounds_merc, unpadded_bounds_wgs84)
        tile = tile_data_file.getvalue()

        formatted_tile = dict(format=format, tile=tile, coord=coord)
        formatted_tiles.append(formatted_tile)

    return formatted_tiles


# given a coord and the raw feature layers results from the database,
# filter, transform, sort, post-process and then format according to
# each formatter. this is the entry point from the worker process
def process_coord(coord, feature_layers, post_process_data, formats,
                  unpadded_bounds, padded_bounds, cut_coords, scale=4096):
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
                unpadded_cut_bounds, padded_cut_bounds, scale)
            children_formatted_tiles.extend(child_formatted_tiles)

    coord_formatted_tiles = _process_feature_layers(
        feature_layers, coord, post_process_data, formats, unpadded_bounds,
        padded_bounds, scale)
    all_formatted_tiles = coord_formatted_tiles + children_formatted_tiles
    return all_formatted_tiles
