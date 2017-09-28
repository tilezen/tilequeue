from collections import namedtuple, defaultdict
from shapely.geometry import box
from tilequeue.query.common import layer_properties
from tilequeue.query.common import is_station_or_stop
from tilequeue.query.common import is_station_or_line
from tilequeue.query.common import deassoc
from tilequeue.query.common import mz_is_interesting_transit_relation
from tilequeue.query.common import shape_type_lookup
from tilequeue.transform import calculate_padded_bounds


class Relation(object):
    def __init__(self, rel_id, way_off, rel_off, parts, members, tags):
        self.id = rel_id
        self.tags = deassoc(tags)
        self.node_ids = parts[0:way_off]
        self.way_ids = parts[way_off:rel_off]
        self.rel_ids = parts[rel_off:]


class TilePyramid(namedtuple('TilePyramid', 'z x y max_z')):

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


# weak type of enum type
class ShapeType(object):
    point = 1
    line = 2
    polygon = 3


def _wkb_shape(wkb):
    reverse = ord(wkb[0]) == 1
    type_bytes = map(ord, wkb[1:5])
    if reverse:
        type_bytes.reverse()
    typ = type_bytes[3]
    if typ == 1 or typ == 4:
        return ShapeType.point
    elif typ == 2 or typ == 5:
        return ShapeType.line
    elif typ == 3 or typ == 6:
        return ShapeType.polygon
    else:
        assert False, "WKB shape type %d not understood." % (typ,)


def _match_type(values, types):
    if len(values) != len(types):
        return False
    for val, typ in zip(values, types):
        if not isinstance(val, typ):
            return False
    return True


class OsmRawrLookup(object):

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

        elif _match_type(args, (num, list, dict)):
            self.add_way(*args)

        elif _match_type(args, (num, num, num, list, list, list)):
            self.add_relation(*args)

        else:
            raise Exception("Unknown row shape for OsmRawrLookup.add_row: %s" %
                            (repr(map(type, args)),))

    def add_feature(self, fid, shape_wkb, props):
        if fid < 0:
            return

        shape_type = _wkb_shape(shape_wkb)
        if is_station_or_stop(fid, None, props) and \
           shape_type == ShapeType.point:
            # must be a station or stop node
            self.nodes[fid] = (fid, shape_wkb, props)

        elif (is_station_or_line(fid, None, props) and
              shape_type != ShapeType.point):
            # must be a station polygon or stop line
            self.ways[fid] = (fid, shape_wkb, props)

    def add_way(self, way_id, nodes, tags):
        for node_id in nodes:
            if node_id in self.nodes:
                self._ways_using_node[node_id] = way_id
                assert way_id in self.ways

    def add_relation(self, rel_id, way_off, rel_off, parts, members, tags):
        r = Relation(rel_id, way_off, rel_off, parts, members, tags)
        if mz_is_interesting_transit_relation(r.tags):
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

        return self.relations[rel_id]

    def way(self, way_id):
        """
        Returns the feature (fid, shape, props) which was generated from the
        given way.
        """

        return self.ways[way_id]

    def node(self, node_id):
        """
        Returns the feature (fid, shape, props) which was generated from the
        given node.
        """

        return self.nodes[node_id]

    def transit_relations(self, rel_id):
        "Return transit relations containing the relation with the given ID."

        return set(self.relations_using_rel(rel_id))


class DataFetcher(object):

    def __init__(self, layers, tables, tile_pyramid, label_placement_layers):
        """
        Expect layers to be a dict of layer name to LayerInfo (see fixture.py).
        Tables should be a callable which returns a generator over the rows in
        the table when called with that table's name.
        """

        from raw_tiles.index.features import FeatureTileIndex
        from raw_tiles.index.index import index_table

        self.layers = layers
        self.tile_pyramid = tile_pyramid
        self.label_placement_layers = label_placement_layers
        self.layer_indexes = {}

        tile = self.tile_pyramid.tile()
        max_zoom = self.tile_pyramid.max_z

        table_indexes = defaultdict(list)

        for layer_name, info in self.layers.items():
            # TODO! this shouldn't be none!
            meta = None

            def min_zoom(fid, shape, props):
                return info.min_zoom_fn(fid, shape, props, meta)

            layer_index = FeatureTileIndex(tile, max_zoom, min_zoom)

            for shape_type in ('point', 'line', 'polygon'):
                if info.allows_shape_type(shape_type):
                    table_name = 'planet_osm_' + shape_type
                    table_indexes[table_name].append(layer_index)

            self.layer_indexes[layer_name] = layer_index

        self.osm = OsmRawrLookup()
        # NOTE: order here is different from that in raw_tiles index()
        # function. this is because here we want to gather up some
        # "interesting" feature IDs before we look at the ways/rels tables.
        for typ in ('point', 'line', 'polygon', 'ways', 'rels'):
            table_name = 'planet_osm_' + typ
            source = tables(table_name)
            extra_indexes = table_indexes[table_name]
            index_table(source, self.osm, *extra_indexes)

    def _named_layer(self, fid, shape, props):
        # we want only one layer from ('pois', 'landuse', 'buildings') for
        # each feature to be assigned a name. therefore, we use the presence
        # or absence of a min zoom to check whether these features as in these
        # layers, and therefore which should be assigned the name.
        for layer_name in ('pois', 'landuse', 'buildings'):
            info = self.layers.get(layer_name)
            if info and info.min_zoom_fn:
                # TODO! meta should not be None!
                min_zoom = info.min_zoom_fn(shape, props, fid, None)
                if min_zoom is not None:
                    return layer_name
        return None

    def _lookup(self, zoom, unpadded_bounds, layer_name):
        from tilequeue.tile import mercator_point_to_coord
        from raw_tiles.tile import Tile

        minx, miny, maxx, maxy = unpadded_bounds
        topleft = mercator_point_to_coord(zoom, minx, miny)
        bottomright = mercator_point_to_coord(zoom, maxx, maxy)
        index = self.layer_indexes[layer_name]

        # make sure that the bottom right coordinate is below and to the right
        # of the top left coordinate. it can happen that the coordinates are
        # mixed up due to small numerical precision artefacts being enlarged
        # by the conversion to integer and y-coordinate flip.
        assert topleft.zoom == bottomright.zoom
        bottomright.column = max(bottomright.column, topleft.column)
        bottomright.row = max(bottomright.row, topleft.row)

        features = []
        seen_ids = set()
        for x in range(int(topleft.column), int(bottomright.column) + 1):
            for y in range(int(topleft.row), int(bottomright.row) + 1):
                tile = Tile(zoom, x, y)
                tile_features = index(tile)
                for feature in tile_features:
                    feature_id = id(feature)
                    if feature_id not in seen_ids:
                        seen_ids.add(feature_id)
                        features.append(feature)
        return features

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

        for layer_name, info in self.layers.items():

            for (fid, shape, props) in self._lookup(
                    zoom, unpadded_bounds, layer_name):
                # reject any feature which doesn't intersect the given bounds
                if bbox.disjoint(shape):
                    continue

                # place for assembing the read row as if from postgres
                read_row = {}

                layer_props = layer_properties(
                    fid, shape, props, layer_name, zoom, self.osm)

                # add name into whichever of the pois, landuse or buildings
                # layers has claimed this feature.
                name = props.get('name', None)
                if name and self._named_layer(fid, shape, props) == layer_name:
                    layer_props['name'] = name

                read_row['__' + layer_name + '_properties__'] = layer_props
                read_row['__id__'] = fid

                # if this is a water layer feature, then clip to an expanded
                # bounding box to avoid tile-edge artefacts.
                clip_box = bbox
                if layer_name == 'water':
                    pad_factor = 1.1
                    clip_box = calculate_padded_bounds(
                        pad_factor, unpadded_bounds)
                clip_shape = clip_box.intersection(shape)
                read_row['__geometry__'] = bytes(clip_shape.wkb)

                # if the feature exists in any label placement layer, then we
                # should consider generating a centroid
                label_layers = self.label_placement_layers.get(
                    shape_type_lookup(shape), {})
                if layer_name in label_layers:
                    read_row['__label__'] = bytes(
                        shape.representative_point().wkb)

                read_rows.append(read_row)

        return read_rows


# tables is a callable which should return a generator over the rows of the
# table when called with the table name.
def make_rawr_data_fetcher(layers, tables, tile_pyramid,
                           label_placement_layers={}):
    return DataFetcher(layers, tables, tile_pyramid, label_placement_layers)
