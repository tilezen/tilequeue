from collections import namedtuple
from collections import defaultdict
from shapely.geometry import box
from tilequeue.process import lookup_source
from itertools import izip
from tilequeue.transform import calculate_padded_bounds


def namedtuple_with_defaults(name, props, defaults):
    t = namedtuple(name, props)
    t.__new__.__defaults__ = defaults
    return t


class LayerInfo(namedtuple_with_defaults(
        'LayerInfo', 'min_zoom_fn props_fn shape_types', (None,))):

    def allows_shape_type(self, shape):
        if self.shape_types is None:
            return True
        typ = _shape_type_lookup(shape)
        return typ in self.shape_types


def deassoc(x):
    """
    Turns an array consisting of alternating key-value pairs into a
    dictionary.

    Osm2pgsql stores the tags for ways and relations in the planet_osm_ways and
    planet_osm_rels tables in this format. Hstore would make more sense now,
    but this encoding pre-dates the common availability of hstore.

    Example:
    >>> from raw_tiles.index.util import deassoc
    >>> deassoc(['a', 1, 'b', 'B', 'c', 3.14])
    {'a': 1, 'c': 3.14, 'b': 'B'}
    """

    pairs = [iter(x)] * 2
    return dict(izip(*pairs))


# fixtures extend metadata to include ways and relations for the feature.
# this is unnecessary for SQL, as the ways and relations tables are
# "ambiently available" and do not need to be passed in arguments.
Metadata = namedtuple('Metadata', 'source ways relations')


def _shape_type_lookup(shape):
    typ = shape.geom_type
    if typ.startswith('Multi'):
        typ = typ[len('Multi'):]
    return typ.lower()


# list of road types which are likely to have buses on them. used to cut
# down the number of queries the SQL used to do for relations. although this
# isn't necessary for fixtures, we replicate the logic to keep the behaviour
# the same.
BUS_ROADS = set([
    'motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary',
    'primary_link', 'secondary', 'secondary_link', 'tertiary',
    'tertiary_link', 'residential', 'unclassified', 'road', 'living_street',
])


class Relation(object):
    def __init__(self, obj):
        self.id = obj['id']
        self.tags = deassoc(obj['tags'])
        way_off = obj['way_off']
        rel_off = obj['rel_off']
        self.node_ids = obj['parts'][0:way_off]
        self.way_ids = obj['parts'][way_off:rel_off]
        self.rel_ids = obj['parts'][rel_off:]


def mz_is_interesting_transit_relation(tags):
    public_transport = tags.get('public_transport')
    typ = tags.get('type')
    return public_transport in ('stop_area', 'stop_area_group') or \
        typ in ('stop_area', 'stop_area_group', 'site')


# starting with the IDs in seed_relations, recurse up the transit relations
# of which they are members. returns the set of all the relation IDs seen
# and the "root" relation ID, which was the "furthest" relation from any
# leaf relation.
def mz_recurse_up_transit_relations(seed_relations, relations):
    transit_relations = defaultdict(set)
    for rel_id, rel in relations.items():
        if mz_is_interesting_transit_relation(rel.tags):
            for member in rel.rel_ids:
                transit_relations[member].add(rel_id)

    root_relation_ids = set()
    root_relation_level = 0
    all_relations = set()

    for rel_id in seed_relations:
        front = set([rel_id])
        seen = set([rel_id])
        level = 0

        if root_relation_level == 0:
            root_relation_ids.add(rel_id)

        while front:
            new_rels = set()
            for r in front:
                new_rels |= transit_relations[r]
            new_rels -= seen
            level += 1
            if new_rels and level > root_relation_level:
                root_relation_ids = new_rels
                root_relation_level = level
            elif new_rels and level == root_relation_level:
                root_relation_ids |= new_rels
            front = new_rels
            seen |= front

        all_relations |= seen

    root_relation_id = min(root_relation_ids) if root_relation_ids else None
    return all_relations, root_relation_id


# extract a name for a transit route relation. this can expand comma
# separated lists and prefers to use the ref rather than the name.
def mz_transit_route_name(tags):
    # prefer ref as it's less likely to contain the destination name
    name = tags.get('ref')
    if not name:
        name = tags.get('name')
    if name:
        name = name.strip()
    return name


Transit = namedtuple(
    'Transit', 'score root_relation_id '
    'trains subways light_rails trams railways')


def mz_calculate_transit_routes_and_score(rows, rels, node_id, way_id):
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

    candidate_relations = set()
    if node_id:
        candidate_relations.update(relations_using_node.get(node_id, []))
    if way_id:
        candidate_relations.update(relations_using_way.get(way_id, []))

    seed_relations = set()
    for rel_id in candidate_relations:
        rel = relations[rel_id]
        if mz_is_interesting_transit_relation(rel.tags):
            seed_relations.add(rel_id)
    del candidate_relations

    # TODO: if the station is also a multipolygon relation?

    # this complex query does two recursive sweeps of the relations
    # table starting from a seed set of relations which are or contain
    # the original station.
    #
    # the first sweep goes "upwards" from relations to "parent" relations. if
    # a relation R1 is a member of relation R2, then R2 will be included in
    # this sweep as long as it has "interesting" tags, as defined by the
    # function mz_is_interesting_transit_relation.
    #
    # the second sweep goes "downwards" from relations to "child" relations.
    # if a relation R1 has a member R2 which is also a relation, then R2 will
    # be included in this sweep as long as it also has "interesting" tags.
    all_relations, root_relation_id = mz_recurse_up_transit_relations(
        seed_relations, relations)
    del seed_relations

    # collect all the interesting nodes - this includes the station node (if
    # any) and any nodes which are members of found relations which have
    # public transport tags indicating that they're stations or stops.
    stations_and_stops = set()
    for rel_id in all_relations:
        rel = relations[rel_id]
        for node_id in rel.node_ids:
            fid, shape, props = nodes[node_id]
            railway = props.get('railway') in ('station', 'stop', 'tram_stop')
            public_transport = props.get('public_transport') in \
                ('stop', 'stop_position', 'tram_stop')
            if railway or public_transport:
                stations_and_stops.add(fid)

    if node_id:
        stations_and_stops.add(node_id)

    # collect any physical railway which includes any of the above
    # nodes.
    stations_and_lines = set()
    for node_id in stations_and_stops:
        for way_id in ways_using_node[node_id]:
            fid, shape, props = ways[way_id]
            railway = props.get('railway')
            if railway in ('subway', 'light_rail', 'tram', 'rail'):
                stations_and_lines.add(way_id)

    if way_id:
        stations_and_lines.add(way_id)

    # collect all IDs together in one array to intersect with the parts arrays
    # of route relations which may include them.
    all_routes = set()
    for lookup, ids in ((relations_using_node, stations_and_stops),
                        (relations_using_way, stations_and_lines),
                        (relations_using_rel, all_relations)):
        for i in ids:
            for rel_id in lookup.get(i, []):
                rel = relations[rel_id]
                if rel.tags.get('type') == 'route' and \
                   rel.tags.get('route') in ('subway', 'light_rail', 'tram',
                                             'train', 'railway'):
                    all_routes.add(rel_id)

    routes_lookup = defaultdict(set)
    for rel_id in all_routes:
        rel = relations[rel_id]
        route = rel.tags.get('route')
        if route:
            route_name = mz_transit_route_name(rel.tags)
            routes_lookup[route].add(route_name)
    trains = routes_lookup['train']
    subways = routes_lookup['subway']
    light_rails = routes_lookup['light_rail']
    trams = routes_lookup['tram']
    railways = routes_lookup['railway']
    del routes_lookup

    # if a station is an interchange between mainline rail and subway or
    # light rail, then give it a "bonus" boost of importance.
    bonus = 2 if trains and (subways or light_rails) else 1

    score = (100 * min(9, bonus * len(trains)) +
             10 * min(9, bonus * (len(subways) + len(light_rails))) +
             min(9, len(trams) + len(railways)))

    return Transit(score=score, root_relation_id=root_relation_id,
                   trains=trains, subways=subways, light_rails=light_rails,
                   railways=railways, trams=trams)


# properties for a feature (fid, shape, props) in layer `layer_name` at zoom
# level `zoom` where that feature is used in `rels` relations directly. also
# needs `all_rows`, a list of all the features, and `all_rels` a list of all
# the relations in the tile, even those which do not use this feature
# directly.
def layer_properties(fid, shape, props, layer_name, zoom, rels,
                     all_rows, all_rels):
    layer_props = props.copy()

    # need to make sure that the name is only applied to one of
    # the pois, landuse or buildings layers - in that order of
    # priority.
    #
    # TODO: do this for all name variants & translations
    if layer_name in ('pois', 'landuse', 'buildings'):
        layer_props.pop('name', None)

    # urgh, hack!
    if layer_name == 'water' and shape.geom_type == 'Point':
        layer_props['label_placement'] = True

    if shape.geom_type in ('Polygon', 'MultiPolygon'):
        layer_props['area'] = shape.area

    if layer_name == 'roads' and \
       shape.geom_type in ('LineString', 'MultiLineString'):
        mz_networks = []
        mz_cycling_networks = set()
        mz_is_bus_route = False
        for rel in rels:
            rel_tags = deassoc(rel['tags'])
            typ, route, network, ref = [rel_tags.get(k) for k in (
                'type', 'route', 'network', 'ref')]
            if route and (network or ref):
                mz_networks.extend([route, network, ref])
            if typ == 'route' and \
               route in ('hiking', 'foot', 'bicycle') and \
               network in ('icn', 'ncn', 'rcn', 'lcn'):
                mz_cycling_networks.add(network)
            if typ == 'route' and route in ('bus', 'trolleybus'):
                mz_is_bus_route = True

        mz_cycling_network = None
        for cn in ('icn', 'ncn', 'rcn', 'lcn'):
            if layer_props.get(cn) == 'yes' or \
               ('%s_ref' % cn) in layer_props or \
               cn in mz_cycling_networks:
                mz_cycling_network = cn
                break

        if mz_is_bus_route and \
           zoom >= 12 and \
           layer_props.get('highway') in BUS_ROADS:
            layer_props['is_bus_route'] = True

        layer_props['mz_networks'] = mz_networks
        if mz_cycling_network:
            layer_props['mz_cycling_network'] = mz_cycling_network

    is_poi = layer_name == 'pois'
    is_railway_station = props.get('railway') == 'station'
    is_point_or_poly = shape.geom_type in (
        'Point', 'MultiPoint', 'Polygon', 'MultiPolygon')

    if is_poi and is_railway_station and \
       is_point_or_poly and fid >= 0:
        node_id = None
        way_id = None
        if shape.geom_type in ('Point', 'MultiPoint'):
            node_id = fid
        else:
            way_id = fid

        transit = mz_calculate_transit_routes_and_score(
            all_rows, all_rels, node_id, way_id)
        layer_props['mz_transit_score'] = transit.score
        layer_props['mz_transit_root_relation_id'] = (
            transit.root_relation_id)
        layer_props['train_routes'] = transit.trains
        layer_props['subway_routes'] = transit.subways
        layer_props['light_rail_routes'] = transit.light_rails
        layer_props['tram_routes'] = transit.trams

    return layer_props


class DataFetcher(object):

    def __init__(self, layers, rows, rels, label_placement_layers):
        """
        Expect layers to be a dict of layer name to LayerInfo. Expect rows to
        be a list of (fid, shape, properties). Label placement layers should
        be a set of layer names for which to generate label placement points.
        """

        self.layers = layers
        self.rows = rows
        self.rels = rels
        self.label_placement_layers = label_placement_layers

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
                    _shape_type_lookup(shape), {})
                if layer_name in label_layers:
                    generate_label_placement = True

                layer_props = layer_properties(
                    fid, shape, props, layer_name, zoom, rels,
                    self.rows, self.rels)

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
