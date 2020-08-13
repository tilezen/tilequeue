from __future__ import division

from collections import defaultdict
from collections import namedtuple
from io import BytesIO
from shapely.geometry import MultiPolygon
from shapely import geometry
from shapely.wkb import loads
from sys import getsizeof
from tilequeue.config import create_query_bounds_pad_fn
from tilequeue.log import make_coord_dict
from tilequeue.tile import calc_meters_per_pixel_dim
from tilequeue.tile import coord_to_mercator_bounds
from tilequeue.tile import normalize_geometry_type
from tilequeue.transform import mercator_point_to_lnglat
from tilequeue.transform import transform_feature_layers_shape
from tilequeue import utils
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

    # callable to log out a JSON object. the single parameter should be plain
    # data structures (list, dict, etc...)
    'log',
])


# post-process all the layers simultaneously, which allows new
# layers to be created from processing existing ones (e.g: for
# computed centroids) or modifying layers based on the contents
# of other layers (e.g: projecting attributes, deleting hidden
# features, etc...)
def _postprocess_data(
        feature_layers, post_process_data, nominal_zoom, unpadded_bounds,
        log_fn=None):

    for step in post_process_data:
        fn = resolve(step['fn_name'])

        # if no logger is configured, just drop the output. but we don't want
        # to pass the complexity on to the inner functions - more readable and
        # less prone to bugs if we just have a single check here.
        def _log_fn(data):
            if log_fn:
                log_fn(dict(fn_name=step['fn_name'], msg=data))

        ctx = Context(
            feature_layers=feature_layers,
            nominal_zoom=nominal_zoom,
            unpadded_bounds=unpadded_bounds,
            params=step['params'],
            resources=step['resources'],
            log=_log_fn,
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
    tile_data_file = BytesIO()
    format.format_tile(
        tile_data_file, transformed_feature_layers, nominal_zoom,
        unpadded_bounds, unpadded_bounds_lnglat, scale)
    tile = tile_data_file.getvalue()

    formatted_tile = dict(format=format, tile=tile, coord=coord, layer=layer)
    return formatted_tile


def _accumulate_props(dest_props, src_props):
    """
    helper to accumulate a dict of properties

    Mutates dest_props by adding the non None src_props and returns
    the new size
    """
    props_size = 0
    if src_props:
        for k, v in src_props.items():
            if v is not None:
                props_size += len(k) + _sizeof(v)
                dest_props[k] = v
    return props_size


Metadata = namedtuple('Metadata', 'source')


# source ties together a short name for a source of data and a longer tag
# value to use on features from that source.
#
# note that this isn't an Enum, since we want sources to be extended for
# testing and 3rd party re-use with additional data sources.
#
# for example:
#   osm = Source('osm', 'openstreetmap.org'),
#   wof = Source('wof', 'whosonfirst.org'),
Source = namedtuple('Source', 'name value')


def make_metadata(source):
    assert source is None or isinstance(source, Source)
    return Metadata(source and source.name)


def lookup_source(source):
    result = None
    if source == 'openstreetmap.org':
        result = Source('osm', source)
    elif source == 'naturalearthdata.com':
        result = Source('ne', source)
    elif source == 'osmdata.openstreetmap.de':
        result = Source('shp', 'osmdata.openstreetmap.de')
    elif source == 'whosonfirst.org':
        result = Source('wof', source)
    elif source == 'tilezen.org':
        result = Source('shp', source)

    return result


def meta_for_properties(query_props):
    meta = None
    query_props_source = query_props.get('source')
    if query_props_source:
        source = lookup_source(query_props_source)
        assert source, 'Unknown source: %s' % query_props_source
        meta = make_metadata(source)
    return meta


def process_coord_no_format(
        feature_layers, nominal_zoom, unpadded_bounds, post_process_data,
        output_calc_mapping, log_fn=None):

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

        layer_output_calc = output_calc_mapping.get(layer_name)
        assert layer_output_calc, 'output_calc_mapping missing layer: %s' % \
            layer_name

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
            props = {}
            feature_size = getsizeof(feature_id) + len(wkb)

            label = row.pop('__label__', None)
            if label:
                # TODO probably formalize as part of the feature
                props['mz_label_placement'] = label
            feature_size += len('__label__') + _sizeof(label)

            # first ensure that all strings are utf-8 encoded
            # it would be better for it all to be unicode instead, but
            # some downstream transforms / formatters might be
            # expecting utf-8
            row = utils.encode_utf8(row)

            query_props = row.pop('__properties__')
            feature_size += len('__properties__') + _sizeof(query_props)

            # TODO:
            # Right now this is hacked to map the particular source,
            # which all relevant queries include, back to another
            # metadata property
            # The reason for this is to support the same yaml syntax
            # for python output calculation and sql min zoom function
            # generation.
            # This is done in python here to avoid having to update
            # all the queries in the jinja file with redundant
            # information.
            meta = meta_for_properties(query_props)

            # set the "tags" key
            # some transforms expect to be able to read it from this location
            # longer term, we might want to separate the notion of
            # "input" and "output" properties as a part of the feature
            props['tags'] = query_props
            output_props = layer_output_calc(
                shape, query_props, feature_id, meta)

            assert output_props, 'No output calc rule matched'

            # a feature can belong to more than one layer
            # this check ensures that it only appears in the
            # layers it should
            # NOTE: the min zoom can be calculated by the yaml, so
            # this check must happen after that
            min_zoom = output_props.get('min_zoom')
            assert min_zoom is not None, \
                'Missing min_zoom in layer %s' % layer_name

            # TODO would be better if 16 wasn't hard coded here
            if nominal_zoom < 16 and min_zoom >= nominal_zoom + 1:
                continue

            for k, v in output_props.items():
                if v is not None:
                    props[k] = v

            if layer_transform_fn:
                shape, props, feature_id = layer_transform_fn(
                    shape, props, feature_id, nominal_zoom)

            feature = shape, props, feature_id
            features.append(feature)
            features_size += feature_size

        extra_data['size'][layer_datum['name']] = features_size

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
        unpadded_bounds, log_fn)

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


def _calculate_scale(scale, coord, nominal_zoom):
    # it doesn't happen very often that the coordinate zoom is greater than
    # the nominal zoom, but when it is, we don't want a loss of precision
    # compared with the previous behaviour, so we don't scale down.
    if coord.zoom <= nominal_zoom:
        return scale * 2**(nominal_zoom - coord.zoom)
    else:
        return scale


def format_coord(
        coord, nominal_zoom, processed_feature_layers, formats,
        unpadded_bounds, cut_coords, buffer_cfg, extra_data, scale):

    formatted_tiles = []
    for cut_coord in cut_coords:
        cut_scale = _calculate_scale(scale, coord, nominal_zoom)

        if cut_coord == coord:
            # no need for cutting if this is the original tile.
            tiles = _format_feature_layers(
                processed_feature_layers, coord, nominal_zoom, formats,
                unpadded_bounds, cut_scale, buffer_cfg)

        else:
            tiles = _cut_child_tiles(
                processed_feature_layers, cut_coord, nominal_zoom, formats,
                _calculate_scale(scale, cut_coord, nominal_zoom), buffer_cfg)

        formatted_tiles.extend(tiles)

    return formatted_tiles, extra_data


# given a coord and the raw feature layers results from the database,
# filter, transform, sort, post-process and then format according to
# each formatter. this is the entry point from the worker process
#
# the nominal zoom is the "display scale" zoom, which may not correspond
# to actual tile coordinates in future versions of the code. it just
# becomes a measure of the scale between tile features and intended
# display size.
#
# the scale parameter is the number of integer coordinates across the
# extent of the tile (where applicable - some formats don't care) for the
# nominal zoom. this means that there will be more pixels for tiles at
# other zooms!
#
# note that the coordinate `coord` is not implicitly rendered and formatted,
# it must be included in `cut_coords` if a formatted version is wanted in
# the output.
def process_coord(coord, nominal_zoom, feature_layers, post_process_data,
                  formats, unpadded_bounds, cut_coords, buffer_cfg,
                  output_calc_spec, scale=4096, log_fn=None):
    processed_feature_layers, extra_data = process_coord_no_format(
        feature_layers, nominal_zoom, unpadded_bounds, post_process_data,
        output_calc_spec, log_fn=log_fn)

    all_formatted_tiles, extra_data = format_coord(
        coord, nominal_zoom, processed_feature_layers, formats,
        unpadded_bounds, cut_coords, buffer_cfg, extra_data, scale)

    return all_formatted_tiles, extra_data


def convert_source_data_to_feature_layers(rows, layer_data, bounds, zoom):
    # TODO we might want to fold in the other processing into this
    # step at some point. This will prevent us from having to iterate
    # through all the features again.

    features_by_layer = defaultdict(list)

    for row in rows:

        fid = row.pop('__id__')

        geometry = row.pop('__geometry__', None)
        label_geometry = row.pop('__label__', None)
        boundaries_geometry = row.pop('__boundaries_geometry__', None)
        assert geometry or boundaries_geometry

        common_props = row.pop('__properties__', None)
        if common_props is None:
            # if __properties__ exists but is null in the query, we
            # want to normalize that to an empty dict too
            common_props = {}

        row_props_by_layer = dict(
            boundaries=row.pop('__boundaries_properties__', None),
            buildings=row.pop('__buildings_properties__', None),
            earth=row.pop('__earth_properties__', None),
            landuse=row.pop('__landuse_properties__', None),
            places=row.pop('__places_properties__', None),
            pois=row.pop('__pois_properties__', None),
            roads=row.pop('__roads_properties__', None),
            transit=row.pop('__transit_properties__', None),
            water=row.pop('__water_properties__', None),
            admin_areas=row.pop('__admin_areas_properties__', None),
        )

        # TODO at first pass, simulate the structure that we're
        # expecting downstream in the process_coord function
        for layer_datum in layer_data:
            layer_name = layer_datum['name']
            layer_props = row_props_by_layer[layer_name]
            if layer_props is not None:
                props = common_props.copy()
                props.update(layer_props)

                query_props = dict(
                    __properties__=props,
                    __id__=fid,
                )

                if boundaries_geometry and layer_name == 'boundaries':
                    geom = boundaries_geometry
                else:
                    geom = geometry
                query_props['__geometry__'] = geom
                if label_geometry:
                    query_props['__label__'] = label_geometry

                features_by_layer[layer_name].append(query_props)

    feature_layers = []
    for layer_datum in layer_data:
        layer_name = layer_datum['name']
        features = features_by_layer[layer_name]
        # TODO padded bounds
        padded_bounds = dict(
            polygon=bounds,
            line=bounds,
            point=bounds,
        )
        feature_layer = dict(
            name=layer_name,
            features=features,
            layer_datum=layer_datum,
            padded_bounds=padded_bounds,
        )
        feature_layers.append(feature_layer)

    return feature_layers


def _is_power_of_2(x):
    """
    Returns True if `x` is a power of 2.
    """

    # see:
    # https://graphics.stanford.edu/~seander/bithacks.html#DetermineIfPowerOf2
    return x != 0 and (x & (x - 1)) == 0


def metatile_children_with_size(coord, metatile_zoom, nominal_zoom, tile_size):
    """
    Return a list of all the coords which are children of the input metatile
    at `coord` with zoom `metatile_zoom` (i.e: 0 for a single tile metatile,
    1 for 2x2, 2 for 4x4, etc...) with size `tile_size` corrected for the
    `nominal_zoom`.

    For example, in a single tile metatile, the `tile_size` must be 256 and the
    returned list contains only `coord`.

    For an 8x8 metatile (`metatile_zoom = 3`), requesting the 512px children
    would give a list of the 4x4 512px children at `coord.zoom + 2` with
    nominal zoom `nominal_zoom`.

    Correcting for nominal zoom means that some tiles may have coordinate zooms
    lower than they would otherwise be. For example, the 0/0/0 tile with
    metatile zoom 3 (8x8 256px tiles) would have 4x4 512px tiles at coordinate
    zoom 2 and nominal zoom 3. At nominal zoom 2, there would be 2x2 512px
    tiles at coordinate zoom 1.
    """

    from tilequeue.tile import coord_children_subrange
    from tilequeue.tile import metatile_zoom_from_size

    assert tile_size >= 256
    assert tile_size <= 256 * (1 << metatile_zoom)
    assert _is_power_of_2(tile_size)

    # delta is how many zoom levels _lower_ we want the child tiles, based on
    # their tile size. 256px tiles are defined as being at nominal zoom, so
    # delta = 0 for them.
    delta = metatile_zoom_from_size(tile_size // 256)

    zoom = nominal_zoom - delta

    return list(coord_children_subrange(coord, zoom, zoom))


def calculate_sizes_by_zoom(coord, metatile_zoom, cfg_tile_sizes, max_zoom):
    """
    Returns a map of nominal zoom to the list of tile sizes to generate at that
    zoom.

    This is because we want to generate different metatile contents at
    different zoom levels. At the most detailed zoom level, we want to generate
    the smallest tiles possible, as this allows "overzooming" by simply
    extracting the smaller tiles. At the minimum zoom, we want to get as close
    as we can to zero nominal zoom by using any "unused" space in the metatile
    for larger tile sizes that we're not generating.

    For example, with 1x1 metatiles, the tile size is always 256px, and the
    function will return {coord.zoom: [256]}

    Note that max_zoom should be the maximum *coordinate* zoom, not nominal
    zoom.
    """

    from tilequeue.tile import metatile_zoom_from_size

    tile_size_by_zoom = {}
    nominal_zoom = coord.zoom + metatile_zoom

    # check that the tile sizes are correct and within range.
    for tile_size in cfg_tile_sizes:
        assert tile_size >= 256
        assert tile_size <= 256 * (1 << metatile_zoom)
        assert _is_power_of_2(tile_size)

    if coord.zoom >= max_zoom:
        # all the tile_sizes down to 256 at the nominal zoom.
        tile_sizes = []
        tile_sizes.extend(cfg_tile_sizes)

        lowest_tile_size = min(tile_sizes)
        while lowest_tile_size > 256:
            lowest_tile_size //= 2
            tile_sizes.append(lowest_tile_size)

        tile_size_by_zoom[nominal_zoom] = tile_sizes

    elif coord.zoom <= 0:
        # the tile_sizes, plus max(tile_sizes) size at nominal zooms decreasing
        # down to 0 (or as close as we can get)
        tile_size_by_zoom[nominal_zoom] = cfg_tile_sizes

        max_tile_size = max(cfg_tile_sizes)
        max_tile_zoom = metatile_zoom_from_size(max_tile_size // 256)
        assert max_tile_zoom <= metatile_zoom
        for delta in range(0, metatile_zoom - max_tile_zoom):
            z = nominal_zoom - (delta + 1)
            tile_size_by_zoom[z] = [max_tile_size]

    else:
        # the tile_sizes at nominal zoom only.
        tile_size_by_zoom[nominal_zoom] = cfg_tile_sizes

    return tile_size_by_zoom


def calculate_cut_coords_by_zoom(
        coord, metatile_zoom, cfg_tile_sizes, max_zoom):
    """
    Returns a map of nominal zoom to the list of cut coordinates at that
    nominal zoom.

    Note that max_zoom should be the maximum coordinate zoom, not nominal
    zoom.
    """

    tile_sizes_by_zoom = calculate_sizes_by_zoom(
        coord, metatile_zoom, cfg_tile_sizes, max_zoom)

    cut_coords_by_zoom = {}
    for nominal_zoom, tile_sizes in tile_sizes_by_zoom.items():
        cut_coords = []
        for tile_size in tile_sizes:
            cut_coords.extend(metatile_children_with_size(
                coord, metatile_zoom, nominal_zoom, tile_size))

        cut_coords_by_zoom[nominal_zoom] = cut_coords

    return cut_coords_by_zoom


class Processor(object):
    def __init__(self, coord, metatile_zoom, fetch_fn, layer_data,
                 post_process_data, formats, buffer_cfg, output_calc_mapping,
                 max_zoom, cfg_tile_sizes, log_fn=None):
        self.coord = coord
        self.metatile_zoom = metatile_zoom
        self.fetch_fn = fetch_fn
        self.layer_data = layer_data
        self.post_process_data = post_process_data
        self.formats = formats
        self.buffer_cfg = buffer_cfg
        self.output_calc_mapping = output_calc_mapping
        self.max_zoom = max_zoom
        self.cfg_tile_sizes = cfg_tile_sizes
        self.log_fn = None

    def fetch(self):
        unpadded_bounds = coord_to_mercator_bounds(self.coord)

        cut_coords_by_zoom = calculate_cut_coords_by_zoom(
            self.coord, self.metatile_zoom, self.cfg_tile_sizes, self.max_zoom)
        feature_layers_by_zoom = {}

        for nominal_zoom, _ in cut_coords_by_zoom.items():
            source_rows = self.fetch_fn(nominal_zoom, unpadded_bounds)
            feature_layers = convert_source_data_to_feature_layers(
                source_rows, self.layer_data, unpadded_bounds, self.coord.zoom)
            feature_layers_by_zoom[nominal_zoom] = feature_layers

        self.cut_coords_by_zoom = cut_coords_by_zoom
        self.feature_layers_by_zoom = feature_layers_by_zoom

    def process_tiles(self):
        unpadded_bounds = coord_to_mercator_bounds(self.coord)

        all_formatted_tiles = []
        all_extra_data = {}

        for nominal_zoom, cut_coords in self.cut_coords_by_zoom.items():
            def log_fn(data):
                if self.log_fn:
                    self.log_fn(dict(
                        coord=make_coord_dict(self.coord),
                        nominal_zoom=nominal_zoom,
                        msg=data,
                    ))

            feature_layers = self.feature_layers_by_zoom[nominal_zoom]
            formatted_tiles, extra_data = process_coord(
                self.coord, nominal_zoom, feature_layers,
                self.post_process_data, self.formats, unpadded_bounds,
                cut_coords, self.buffer_cfg, self.output_calc_mapping,
                log_fn=log_fn,
            )
            all_formatted_tiles.extend(formatted_tiles)
            all_extra_data.update(extra_data)

        return all_formatted_tiles, all_extra_data


def process(coord, metatile_zoom, fetch_fn, layer_data, post_process_data,
            formats, buffer_cfg, output_calc_mapping, max_zoom,
            cfg_tile_sizes):
    p = Processor(coord, metatile_zoom, fetch_fn, layer_data,
                  post_process_data, formats, buffer_cfg, output_calc_mapping,
                  max_zoom, cfg_tile_sizes)
    p.fetch()
    return p.process_tiles()
