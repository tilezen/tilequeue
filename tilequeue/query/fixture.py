from shapely.geometry import box
from tilequeue.process import lookup_source
from tilequeue.transform import calculate_padded_bounds
from tilequeue.query.common import Metadata
from tilequeue.query.common import Relation
from tilequeue.query.common import layer_properties
from tilequeue.query.common import shape_type_lookup
from tilequeue.query.common import mz_is_interesting_transit_relation
from collections import defaultdict


class OsmFixtureLookup(object):

    def __init__(self, rows, rels):
        # extract out all relations and index by ID. this is helpful when
        # looking them up later.
        relations = {}
        nodes = {}
        ways = {}
        ways_using_node = {}

        for (fid, shape, props) in rows:
            if fid >= 0:
                if shape.geom_type in ('Point', 'MultiPoint'):
                    nodes[fid] = (fid, shape, props)
                    features = props.get('__ways__', [])
                    ways_using_node[fid] = [f[0] for f in features]
                else:
                    ways[fid] = (fid, shape, props)

        for r in rels:
            r = Relation(r)
            assert r.id not in relations
            relations[r.id] = r

        relations_using_node = defaultdict(list)
        relations_using_way = defaultdict(list)
        relations_using_rel = defaultdict(list)

        for rel_id, rel in relations.items():
            for (ids, index) in ((rel.node_ids, relations_using_node),
                                 (rel.way_ids, relations_using_way),
                                 (rel.rel_ids, relations_using_rel)):
                for osm_id in ids:
                    index[osm_id].append(rel_id)

        transit_relations = defaultdict(set)
        for rel_id, rel in relations.items():
            if mz_is_interesting_transit_relation(rel.tags):
                for member in rel.rel_ids:
                    transit_relations[member].add(rel_id)

        # looks up relation IDs
        self._relations_using_node = relations_using_node
        self._relations_using_way = relations_using_way
        self._relations_using_rel = relations_using_rel
        # looks up way IDs
        self._ways_using_node = ways_using_node
        # looks up Relation objects
        self._relations = relations
        # looks up (fid, shape, props) feature objects
        self._ways = ways
        self._nodes = nodes
        # returns the set of transit relation IDs that contain the given
        # relation IDs
        self._transit_relations = transit_relations

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

        return self._relations[rel_id]

    def way(self, way_id):
        """
        Returns the feature (fid, shape, props) which was generated from the
        given way.
        """

        return self._ways[way_id]

    def node(self, node_id):
        """
        Returns the feature (fid, shape, props) which was generated from the
        given node.
        """

        return self._nodes[node_id]

    def transit_relations(self, rel_id):
        "Return transit relations containing the relation with the given ID."

        return self._transit_relations.get(rel_id, set())


class DataFetcher(object):

    def __init__(self, layers, rows, rels, label_placement_layers):
        """
        Expect layers to be a dict of layer name to LayerInfo. Expect rows to
        be a list of (fid, shape, properties). Label placement layers should
        be a dict of geometry type ('point', 'linestring', 'polygon') to set
        of layer names, meaning that each feature of the given type in any of
        the named layers should additionally get a generated label point.
        """

        self.layers = layers
        self.rows = rows
        self.rels = rels
        self.label_placement_layers = label_placement_layers
        self.osm = OsmFixtureLookup(self.rows, self.rels)

    def fetch_tiles(self, all_data):
        # fixture data fetcher doesn't need this kind of session management,
        # so we can just return the same object for all uses.
        for data in all_data:
            yield self, data

    def __call__(self, zoom, unpadded_bounds):
        read_rows = []
        bbox = box(*unpadded_bounds)

        for (fid, shape, props) in self.rows:
            # reject any feature which doesn't intersect the given bounds
            if bbox.disjoint(shape):
                continue

            # copy props so that any updates to it don't affect the original
            # data.
            props = props.copy()

            # TODO: there must be some better way of doing this?
            rels = props.pop('__relations__', [])
            ways = props.pop('__ways__', [])

            # place for assembing the read row as if from postgres
            read_row = {}

            # whether to generate a label placement centroid
            generate_label_placement = False

            # whether to clip to a padded box
            has_water_layer = False

            for layer_name, info in self.layers.items():
                if not info.allows_shape_type(shape):
                    continue

                source = lookup_source(props.get('source'))
                meta = Metadata(source, ways, rels)
                min_zoom = info.min_zoom_fn(shape, props, fid, meta)

                # reject features which don't match in this layer
                if min_zoom is None:
                    continue

                # reject anything which isn't in the current zoom range
                # note that this is (zoom+1) because things with a min_zoom of
                # (e.g) 14.999 should still be in the zoom 14 tile.
                #
                # also, if zoom >= 16, we should include all features, even
                # those with min_zoom > zoom.
                if zoom < 16 and (zoom + 1) <= min_zoom:
                    continue

                # UGLY HACK: match the query for "max zoom" for NE places.
                # this removes larger cities at low zooms, and smaller cities
                # as the zoom increases and as the OSM cities start to "fade
                # in".
                if props.get('source') == 'naturalearthdata.com':
                    pop_max = int(props.get('pop_max', '0'))
                    remove = ((zoom >= 8 and zoom < 10 and pop_max > 50000) or
                              (zoom >= 10 and zoom < 11 and pop_max > 20000) or
                              (zoom >= 11 and pop_max > 5000))
                    if remove:
                        continue

                # if the feature exists in any label placement layer, then we
                # should consider generating a centroid
                label_layers = self.label_placement_layers.get(
                    shape_type_lookup(shape), {})
                if layer_name in label_layers:
                    generate_label_placement = True

                layer_props = layer_properties(
                    fid, shape, props, layer_name, zoom, self.osm)

                layer_props['min_zoom'] = min_zoom
                props_name = '__%s_properties__' % layer_name
                read_row[props_name] = layer_props
                if layer_name == 'water':
                    has_water_layer = True

            # if at least one min_zoom / properties match
            if read_row:
                clip_box = bbox
                if has_water_layer:
                    pad_factor = 1.1
                    clip_box = calculate_padded_bounds(
                        pad_factor, unpadded_bounds)
                clip_shape = clip_box.intersection(shape)

                # add back name into whichever of the pois, landuse or
                # buildings layers has claimed this feature.
                name = props.get('name', None)
                if name:
                    for layer_name in ('pois', 'landuse', 'buildings'):
                        props_name = '__%s_properties__' % layer_name
                        if props_name in read_row:
                            read_row[props_name]['name'] = name
                            break

                read_row['__id__'] = fid
                read_row['__geometry__'] = bytes(clip_shape.wkb)
                if generate_label_placement:
                    read_row['__label__'] = bytes(
                        shape.representative_point().wkb)
                read_rows.append(read_row)

        return read_rows


def make_fixture_data_fetcher(
        layers, rows, label_placement_layers={}, relations=[]):
    return DataFetcher(layers, rows, relations, label_placement_layers)
