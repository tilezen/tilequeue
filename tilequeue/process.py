from cStringIO import StringIO
from shapely import geometry
from shapely.wkb import loads
from tilequeue.transform import transform_feature_layers_shape
from TileStache.Config import loadClassPath
from TileStache.Goodies.VecTiles.server import make_transform_fn
from TileStache.Goodies.VecTiles.server import resolve_transform_fns


# given a coord and the raw feature layers results from the database,
# filter, transform, sort, and then format according to each formatter
def process_coord(coord, feature_layers, formats, unpadded_bounds,
                  padded_bounds, scale=4096):
    shape_padded_bounds = geometry.box(*padded_bounds)

    processed_feature_layers = []
    # filter, and then transform each layer as necessary
    for feature_layer in feature_layers:
        layer_datum = feature_layer['layer_datum']
        layer_name = layer_datum['name']
        geometry_types = layer_datum['geometry_types']

        transform_fn_names = layer_datum['transform_fn_names']
        if transform_fn_names:
            transform_fns = resolve_transform_fns(transform_fn_names)
            layer_transform_fn = make_transform_fn(transform_fns)
        else:
            layer_transform_fn = None

        features = []

        for row in feature_layer['features']:
            wkb = bytes(row.pop('__geometry__'))
            shape = loads(wkb)

            if shape.is_empty:
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

            # perform any specific layer transformations
            if layer_transform_fn is not None:
                shape, props, feature_id = layer_transform_fn(
                    shape, props, feature_id)

            feature = shape, props, feature_id
            features.append(feature)

        sort_fn_name = layer_datum['sort_fn_name']
        if sort_fn_name:
            sort_fn = loadClassPath(sort_fn_name)
            features = sort_fn(features)

        feature_layer = dict(name=layer_name, features=features,
                             layer_datum=layer_datum)
        processed_feature_layers.append(feature_layer)

    # now, perform the format specific transformations
    # and format the tile itself
    formatted_tiles = []
    for format in formats:
        # perform format specific transformations
        transformed_feature_layers = transform_feature_layers_shape(
            processed_feature_layers, format, scale, unpadded_bounds,
            padded_bounds, coord)

        # use the formatted to generate the tile
        tile_data_file = StringIO()
        format.format_tile(tile_data_file, transformed_feature_layers, coord,
                           unpadded_bounds)
        tile = tile_data_file.getvalue()

        formatted_tile = dict(format=format, tile=tile)
        formatted_tiles.append(formatted_tile)

    return formatted_tiles
