from collections import namedtuple, defaultdict
from shapely.geometry import box
from shapely.wkb import loads as wkb_loads
from tilequeue.query.common import layer_properties
from tilequeue.query.common import is_station_or_stop
from tilequeue.query.common import is_station_or_line
from tilequeue.query.common import deassoc
from tilequeue.query.common import mz_is_interesting_transit_relation
from tilequeue.query.common import shape_type_lookup
from tilequeue.query.common import name_keys
from tilequeue.query.common import wkb_shape_type
from tilequeue.query.common import ShapeType
from tilequeue.transform import calculate_padded_bounds
from tilequeue.utils import CoordsByParent
from raw_tiles.tile import shape_tile_coverage
from math import floor


class Relation(object):
    """
    Relation object holds data about a relation and provides a nicer interface
    than the raw tuple by turning the tags array into a dict, and separating
    out the "parts" array of IDs into separate lists for nodes, ways and other
    relations.
    """

    def __init__(self, rel_id, way_off, rel_off, parts, members, tags):
        self.id = rel_id
        self.tags = deassoc(tags)
        self.node_ids = parts[0:way_off]
        self.way_ids = parts[way_off:rel_off]
        self.rel_ids = parts[rel_off:]


class TilePyramid(namedtuple('TilePyramid', 'z x y max_z')):
    """
    Represents a "tile pyramid" of all tiles which are geographically
    contained within the tile `z/x/y` up to a maximum zoom of `max_z`. This is
    the set of tiles corresponding to one RAWR tile.
    """

    def tile(self):
        from raw_tiles.tile import Tile
        return Tile(self.z, self.x, self.y)

    def bounds(self):
        from ModestMaps.Core import Coordinate
        from tilequeue.tile import coord_to_mercator_bounds

        coord = Coordinate(zoom=self.z, column=self.x, row=self.y)
        bounds = coord_to_mercator_bounds(coord)

        return bounds

    def bbox(self):
        return box(*self.bounds())


# return true if the tuple of values corresponds to, and each is an instance
# of, the tuple of types. this is used to make sure that argument lists are
# the right "shape" before destructuring (splatting?) them in a function call.
def _match_type(values, types):
    if len(values) != len(types):
        return False
    for val, typ in zip(values, types):
        if not isinstance(val, typ):
            return False
    return True


# return true if the tags indicate that this is a gate
def _is_gate(props):
    return props.get('barrier') == 'gate'


# return true if the tags indicate that this is a highway, cycleway or footway
# which might be part of a route relation. note that this is pretty loose, and
# might return true for things we don't eventually render as roads, but is just
# aimed at cutting down the number of items we need in our index.
def _is_routeable(props):
    return props.get('whitewater') == 'portage_way' or 'highway' in props


class OsmRawrLookup(object):
    """
    Implements the interface needed by the common code (e.g: layer_properties)
    to look up information about node, way and relation IDs. For database
    lookups, we previously did this with a JOIN, and the fixture data source
    just iterates over the (small) number of items.

    For RAWR tiles, we index the data to provide faster lookup, and are more
    selective about what goes into the index.
    """

    def __init__(self):
        self.nodes = {}
        self.ways = {}
        self.relations = {}

        self._ways_using_node = defaultdict(list)
        self._relations_using_node = defaultdict(list)
        self._relations_using_way = defaultdict(list)
        self._relations_using_rel = defaultdict(list)

    def add_row(self, *args):
        # there's only a single dispatch from the indexing function, which
        # passes row data from the table. we have to figure out here what
        # kind of row it was, and send the data on to the right function.

        # IDs can be either ints or longs, and generally we don't care which,
        # so we accept either as the type for that position in the function.
        num = (int, long)

        if _match_type(args, (num, (str, bytes), dict)):
            self.add_feature(*args)

        elif _match_type(args, (num, list, list)):
            self.add_way(*args)

        elif _match_type(args, (num, num, num, list, list, list)):
            self.add_relation(*args)

        else:
            raise Exception("Unknown row shape for OsmRawrLookup.add_row: %s" %
                            (repr(map(type, args)),))

    def add_feature(self, fid, shape_wkb, props):
        if fid < 0:
            return

        shape_type = wkb_shape_type(shape_wkb)
        if is_station_or_stop(fid, None, props) and \
           shape_type == ShapeType.point:
            # must be a station or stop node
            self.nodes[fid] = (fid, shape_wkb, props)

        elif _is_gate(props) and shape_type == ShapeType.point:
            # index the highways that use gates to influence min zoom
            self.nodes[fid] = (fid, shape_wkb, props)

        elif (is_station_or_line(fid, None, props) and
              shape_type != ShapeType.point):
            # must be a station polygon or stop line
            self.ways[fid] = (fid, shape_wkb, props)

        elif _is_routeable(props) and shape_type == ShapeType.line:
            # index routable items (highways, cycleways, footpaths) to
            # get the relations using them.
            self.ways[fid] = (fid, shape_wkb, props)

    def add_way(self, way_id, nodes, tags):
        for node_id in nodes:
            if node_id in self.nodes:
                # a way might be missing here if we filtered it out because it
                # was not interesting, e.g: not a road, station, etc... that
                # we might need to look up later.
                if way_id in self.ways:
                    self._ways_using_node[node_id].append(way_id)

    def add_relation(self, rel_id, way_off, rel_off, parts, members, tags):
        r = Relation(rel_id, way_off, rel_off, parts, members, tags)
        is_transit_relation = mz_is_interesting_transit_relation(r.tags)
        is_route = 'route' in r.tags and \
                   ('network' in r.tags or 'ref' in r.tags)
        if is_route or is_transit_relation:
            self.relations[r.id] = r
            for node_id in r.node_ids:
                if node_id in self.nodes:
                    self._relations_using_node[node_id].append(rel_id)
            for way_id in r.way_ids:
                if way_id in self.ways:
                    self._relations_using_way[way_id].append(rel_id)
            for member_rel_id in r.rel_ids:
                self._relations_using_rel[member_rel_id].append(rel_id)

    def relations_using_node(self, node_id):
        "Returns a list of relation IDs which contain the node with that ID."

        return self._relations_using_node.get(node_id, [])

    def relations_using_way(self, way_id):
        "Returns a list of relation IDs which contain the way with that ID."

        return self._relations_using_way.get(way_id, [])

    def relations_using_rel(self, rel_id):
        """
        Returns a list of relation IDs which contain the relation with that
        ID.
        """

        return self._relations_using_rel.get(rel_id, [])

    def ways_using_node(self, node_id):
        "Returns a list of way IDs which contain the node with that ID."

        return self._ways_using_node.get(node_id, [])

    def relation(self, rel_id):
        "Returns the Relation object with the given ID."

        return self.relations.get(rel_id)

    def way(self, way_id):
        """
        Returns the feature (fid, shape, props) which was generated from the
        given way.
        """

        return self.ways.get(way_id)

    def node(self, node_id):
        """
        Returns the feature (fid, shape, props) which was generated from the
        given node.
        """

        return self.nodes.get(node_id)

    def transit_relations(self, rel_id):
        "Return transit relations containing the relation with the given ID."

        return set(self.relations_using_rel(rel_id))


def _snapping_round(num, eps, resolution):
    """
    Return num snapped to within eps of an integer, or int(resolution(num)).
    """

    rounded = round(num)
    delta = abs(num - rounded)
    if delta < eps:
        return int(rounded)
    else:
        return int(resolution(num))


# yield all the tiles at the given zoom level which intersect the given bounds.
def _tiles(zoom, unpadded_bounds):
    from tilequeue.tile import mercator_point_to_coord_fractional
    from raw_tiles.tile import Tile
    import math

    minx, miny, maxx, maxy = unpadded_bounds
    topleft = mercator_point_to_coord_fractional(zoom, minx, maxy)
    bottomright = mercator_point_to_coord_fractional(zoom, maxx, miny)

    # make sure that the bottom right coordinate is below and to the right
    # of the top left coordinate. it can happen that the coordinates are
    # mixed up due to small numerical precision artefacts being enlarged
    # by the conversion to integer and y-coordinate flip.
    assert topleft.zoom == bottomright.zoom
    minx = min(topleft.column, bottomright.column)
    maxx = max(topleft.column, bottomright.column)
    miny = min(topleft.row, bottomright.row)
    maxy = max(topleft.row, bottomright.row)

    eps = 1.0e-5
    minx = _snapping_round(minx, eps, math.floor)
    maxx = _snapping_round(maxx, eps, math.ceil)
    miny = _snapping_round(miny, eps, math.floor)
    maxy = _snapping_round(maxy, eps, math.ceil)

    for x in range(minx, maxx):
        for y in range(miny, maxy):
            tile = Tile(zoom, x, y)
            yield tile


# the object which gets indexed. this is a normal (fid, shape, props) tuple
# expanded to include a dict of layer name to min zoom in `layer_min_zooms`.
# this means that the properties don't have to be copied and altered to
# include the min zoom for each layer, reducing the memory footprint.
_Feature = namedtuple('_Feature', 'fid shape properties layer_min_zooms')


class _LazyShape(object):
    """
    This proxy exists so that we can avoid parsing the WKB for a shape unless
    it is actually needed. Parsing WKB is pretty fast, but multiplied over
    many thousands of objects, it can become the slowest part of the indexing
    process. Given that we reject many features on the basis of their
    properties alone, lazily parsing the WKB can provide a significant saving.
    """

    def __init__(self, wkb):
        self.wkb = wkb
        self.obj = None
        self._bounds = None

    def __getattr__(self, name):
        if self.obj is None:
            self.obj = wkb_loads(self.wkb)
        return getattr(self.obj, name)

    @property
    def bounds(self):
        if self.obj is None:
            self.obj = wkb_loads(self.wkb)
        if self._bounds is None:
            self._bounds = self.obj.bounds
        return self._bounds


_Metadata = namedtuple('_Metadata', 'source ways relations')


def _make_meta(source, fid, shape_type, osm):
    ways = []
    rels = []

    # fetch ways and relations for any node
    if fid >= 0 and shape_type == ShapeType.point:
        for way_id in osm.ways_using_node(fid):
            way = osm.way(way_id)
            if way:
                ways.append(way)
        for rel_id in osm.relations_using_node(fid):
            rel = osm.relation(rel_id)
            if rel:
                rels.append(rel)

    # and relations for any way
    if fid >= 0 and shape_type == ShapeType.line:
        for rel_id in osm.relations_using_way(fid):
            rel = osm.relation(rel_id)
            if rel:
                rels.append(rel)

    # have to transform the Relation object into a dict, which is
    # what the functions called on this data expect.
    # TODO: reusing the Relation object would be better.
    rel_dicts = []
    for r in rels:
        tags = []
        for k, v in r.tags.items():
            tags.append(k)
            tags.append(v)
        rel_dicts.append(dict(tags=tags))

    return _Metadata(source.name, ways, rel_dicts)


class _LayersIndex(object):
    """
    Index features by the tile(s) that they appear in.

    This is done by calculating a min-min-zoom, the lowest min_zoom for that
    feature across all layers, and then adding that feature to a list for each
    tile it appears in from the min-min-zoom up to the max zoom for the tile
    pyramid.
    """

    def __init__(self, layers, tile_pyramid):
        self.layers = layers
        self.tile_pyramid = tile_pyramid
        self.tile_index = defaultdict(list)
        self.delayed_features = []

    def add_row(self, fid, shape_wkb, props):
        shape = _LazyShape(shape_wkb)
        # single object (hence single id()) will be shared amongst all layers.
        # this allows us to easily and quickly de-duplicate at later layers in
        # the stack.
        feature = _Feature(fid, shape, props, {})

        # delay min zoom calculation in order to collect more information about
        # the ways and relations using a particular feature.
        self.delayed_features.append(feature)

    def index(self, osm, source):
        for feature in self.delayed_features:
            self._index_feature(feature, osm, source)
        del self.delayed_features

    def _index_feature(self, feature, osm, source):
        # stash this for later, so that it's accessible when the index is read.
        self.source = source

        fid = feature.fid
        shape = feature.shape
        props = feature.properties
        layer_min_zooms = feature.layer_min_zooms

        # grab the shape type without decoding the WKB to save time.
        shape_type = wkb_shape_type(shape.wkb)

        meta = _make_meta(source, fid, shape_type, osm)
        for layer_name, info in self.layers.items():
            if info.shape_types and shape_type not in info.shape_types:
                continue
            min_zoom = info.min_zoom_fn(shape, props, fid, meta)
            if min_zoom is not None:
                layer_min_zooms[layer_name] = min_zoom

        # quick exit if the feature didn't have a min zoom in any layer.
        if not layer_min_zooms:
            return

        # lowest zoom that this feature appears in any layer. note that this
        # is clamped to the max zoom, so that all features that appear at some
        # zoom level appear at the max zoom. this is different from the min
        # zoom in layer_min_zooms, which is a property that will be injected
        # for each layer and is used by the _client_ to determine feature
        # visibility.
        min_zoom = min(self.tile_pyramid.max_z, min(layer_min_zooms.values()))

        # take the minimum integer zoom - this is the min zoom tile that the
        # feature should appear in, and a feature with min_zoom = 1.9 should
        # appear in a tile at z=1, not 2, since the tile at z=N is used for
        # the zoom range N to N+1.
        #
        # we cut this off at this index's min zoom, as we aren't interested
        # in any tiles outside of that.
        floor_zoom = max(self.tile_pyramid.z, int(floor(min_zoom)))

        # seed initial set of tiles at maximum zoom. all features appear at
        # least at the max zoom, even if the min_zoom function returns a
        # value larger than the max zoom.
        zoom = self.tile_pyramid.max_z
        tiles = shape_tile_coverage(shape, zoom, self.tile_pyramid.tile())

        while zoom >= floor_zoom:
            parent_tiles = set()
            for tile in tiles:
                self.tile_index[tile].append(feature)
                parent_tiles.add(tile.parent())

            zoom -= 1
            tiles = parent_tiles

    def __call__(self, tile):
        return self.tile_index.get(tile, [])


class RawrTile(object):

    def __init__(self, layers, tables, tile_pyramid, label_placement_layers):
        """
        Expect layers to be a dict of layer name to LayerInfo (see fixture.py).
        Tables should be a callable which returns a Table object (namedtuple
        of a source and iterator over the rows in the table) when called with
        that table's name.
        """

        from raw_tiles.index.index import index_table

        self.layers = layers
        self.tile_pyramid = tile_pyramid
        self.label_placement_layers = label_placement_layers
        self.layer_indexes = {}

        table_indexes = defaultdict(list)

        self.layers_index = _LayersIndex(self.layers, self.tile_pyramid)
        for shape_type in ('point', 'line', 'polygon'):
            table_name = 'planet_osm_' + shape_type
            table_indexes[table_name].append(self.layers_index)

        # source for all these layers has to be the same
        source = None

        self.osm = OsmRawrLookup()
        # NOTE: order here is different from that in raw_tiles index()
        # function. this is because here we want to gather up some
        # "interesting" feature IDs before we look at the ways/rels tables.
        for typ in ('point', 'line', 'polygon', 'ways', 'rels'):
            table_name = 'planet_osm_' + typ
            table = tables(table_name)
            extra_indexes = table_indexes[table_name]
            index_table(table.rows, self.osm, *extra_indexes)

            if source is None:
                source = table.source
            else:
                assert source == table.source, 'Mismatched sources'

        assert source
        self.layers_index_source = source

        # there's a chicken and egg problem with the indexes: we want to know
        # which features to index, but also calculate the feature's min zoom,
        # which might depend on ways and relations not seen yet. one solution
        # would be to do this in two passes, but that might mean paying a cost
        # to decompress or deserialize the data twice. instead, the index
        # buffers the features and indexes them in the following step. this
        # might mean we buffer more information in memory than we technically
        # need if many of the features are not visible, but means we get one
        # single set of _Feature objects.
        self.layers_index.index(self.osm, self.layers_index_source)

    def _named_layer(self, layer_min_zooms):
        # we want only one layer from ('pois', 'landuse', 'buildings') for
        # each feature to be assigned a name. therefore, we use the presence
        # or absence of a min zoom to check whether these features as in these
        # layers, and therefore which should be assigned the name. handily,
        # the min zooms are already pre-calculated as layer_min_zooms from the
        # index.
        for layer_name in ('pois', 'landuse', 'buildings'):
            if layer_name in layer_min_zooms:
                return layer_name
        return None

    def _lookup(self, zoom, unpadded_bounds):
        features = []
        seen_ids = set()

        for tile in _tiles(zoom, unpadded_bounds):
            tile_features = self.layers_index(tile)
            for feature in tile_features:
                feature_id = id(feature)
                if feature_id not in seen_ids:
                    seen_ids.add(feature_id)
                    features.append(feature)

        return {self.layers_index_source: features}.iteritems()

    def __call__(self, zoom, unpadded_bounds):
        read_rows = []
        bbox = box(*unpadded_bounds)

        # check that the call is fetching data which is actually within the
        # bounds of the tile pyramid. we don't have data outside of that, so
        # can't fulfil requests. if these assertions are tripping, it probably
        # indicates a programming error - has the wrong DataFetcher been
        # loaded?
        assert zoom <= self.tile_pyramid.max_z
        assert zoom >= self.tile_pyramid.z
        assert bbox.within(self.tile_pyramid.bbox())

        for source, features in self._lookup(zoom, unpadded_bounds):
            for (fid, shape, props, layer_min_zooms) in features:
                read_row = self._parse_row(
                    zoom, unpadded_bounds, bbox, source, fid, shape, props,
                    layer_min_zooms)
                if read_row:
                    read_rows.append(read_row)

        return read_rows

    def _parse_row(self, zoom, unpadded_bounds, bbox, source, fid, shape,
                   props, layer_min_zooms):
        # reject any feature which doesn't intersect the given bounds
        if bbox.disjoint(shape):
            return None

        # place for assembing the read row as if from postgres
        read_row = {}
        generate_label_placement = False

        # add names into whichever of the pois, landuse or buildings
        # layers has claimed this feature.
        names = {}
        for k in name_keys(props):
            names[k] = props[k]
        named_layer = self._named_layer(layer_min_zooms)

        for layer_name, min_zoom in layer_min_zooms.items():
            # we need to keep fractional zooms, e.g: 4.999 should appear
            # in tiles at zoom level 4, but not 3. also, tiles at zooms
            # past the max zoom should be clamped to the max zoom.
            tile_zoom = min(self.tile_pyramid.max_z, floor(min_zoom))
            if tile_zoom > zoom:
                continue

            layer_props = layer_properties(
                fid, shape, props, layer_name, zoom, self.osm)
            layer_props['min_zoom'] = min_zoom

            if names and named_layer == layer_name:
                layer_props.update(names)

            read_row['__' + layer_name + '_properties__'] = layer_props

            # if the feature exists in any label placement layer, then we
            # should consider generating a centroid
            label_layers = self.label_placement_layers.get(
                shape_type_lookup(shape), {})
            if layer_name in label_layers:
                generate_label_placement = True

        if read_row:
            read_row['__id__'] = fid

            # if this is a water layer feature, then clip to an expanded
            # bounding box to avoid tile-edge artefacts.
            clip_box = bbox
            if layer_name == 'water':
                pad_factor = 1.1
                clip_box = calculate_padded_bounds(
                    pad_factor, unpadded_bounds)
            # don't need to clip if geom is fully within the clipping box
            if box(*shape.bounds).within(clip_box):
                clip_shape = shape
            else:
                clip_shape = clip_box.intersection(shape)
            read_row['__geometry__'] = bytes(clip_shape.wkb)

            if generate_label_placement:
                read_row['__label__'] = bytes(
                    shape.representative_point().wkb)

            if source:
                read_row['__properties__'] = {'source': source.value}

        return read_row


class DataFetcher(object):

    def __init__(self, min_z, max_z, storage, layers, label_placement_layers):
        self.min_z = min_z
        self.max_z = max_z
        self.storage = storage
        self.layers = layers
        self.label_placement_layers = label_placement_layers

    def fetch_tiles(self, all_data):
        # group all coords by the "unit of work" zoom, i.e: z10 for
        # RAWR tiles.
        coords_by_parent = CoordsByParent(self.min_z)
        for data in all_data:
            coord = data['coord']
            coords_by_parent.add(coord, data)

        # this means we can dispatch groups of jobs by their common parent
        # tile, which allows DataFetcher to take advantage of any common
        # locality.
        for top_coord, coord_group in coords_by_parent:
            tile_pyramid = TilePyramid(
                self.min_z, int(top_coord.column), int(top_coord.row),
                self.max_z)

            tables = self.storage(tile_pyramid.tile())

            fetcher = RawrTile(self.layers, tables, tile_pyramid,
                               self.label_placement_layers)

            for coord, data in coord_group:
                yield fetcher, data


# Make a RAWR tile data fetcher given:
#
#  - min_z:   Lowest *nominal* zoom level, e.g: z10 for a RAWR tile.
#  - max_z:   Highest *nominal* zoom level, e.g: z16 for a RAWR tile.
#  - storage: Callable which takes a TilePyramid as the only argument,
#             returning a "tables" callable. The "tables" callable returns a
#             list of rows given the table name as its only argument.
#  - layers:  A dict of layer name to LayerInfo (see fixture.py).
#  - label_placement_layers:
#             A dict of geometry type ('point', 'linestring', 'polygon') to
#             set (or other in-supporting collection) of layer names.
#             Geometries of that type in that layer will have a label
#             placement generated for them.
def make_rawr_data_fetcher(min_z, max_z, storage, layers,
                           label_placement_layers={}):
    return DataFetcher(min_z, max_z, storage, layers, label_placement_layers)
