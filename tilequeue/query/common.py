from collections import namedtuple
from collections import defaultdict
from enum import Enum
from tilequeue.process import Source
from typing import NamedTuple


def namedtuple_with_defaults(name, props, defaults):
    t = namedtuple(name, props)
    t.__new__.__defaults__ = defaults
    return t


class LayerInfo(namedtuple_with_defaults(
        'LayerInfo', 'min_zoom_fn props_fn shape_types', (None,))):

    def allows_shape_type(self, shape):
        if self.shape_types is None:
            return True
        typ = shape_type_lookup(shape)
        return typ in self.shape_types


class ShapeType(Enum):
    point = 1
    line = 2
    polygon = 3

    # aliases, don't use these directly!
    multipoint = 1
    linestring = 2
    multilinestring = 2
    multipolygon = 3

    @classmethod
    def parse_set(cls, inputs):
        outputs = set()
        for value in inputs:
            t = cls[value.lower()]
            outputs.add(t)

        return outputs or None


# determine the shape type from the raw WKB bytes. this means we don't have to
# parse the WKB, which can be an expensive operation for large polygons.
def wkb_shape_type(wkb):
    reverse = (wkb[0] == 1)
    type_bytes = list(wkb[1:5])
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
    return dict(zip(*pairs))


# fixtures extend metadata to include ways and relations for the feature.
# this is unnecessary for SQL, as the ways and relations tables are
# "ambiently available" and do not need to be passed in arguments.
class Metadata(object):
    def __init__(self, source, ways, relations):
        assert source is None or isinstance(source, Source)
        self.source = source and source.name
        self.ways = ways
        self.relations = relations


class Table(NamedTuple):
    source: Source
    rows: list


def shape_type_lookup(shape):
    typ = shape.geom_type
    if typ.startswith('Multi'):
        typ = typ[len('Multi'):]
    return typ.lower()


# set of road types which are likely to have buses on them. used to cut
# down the number of queries the SQL used to do for relations. although this
# isn't necessary for fixtures, we replicate the logic to keep the behaviour
# the same.
BUS_ROADS = {
    'motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary',
    'primary_link', 'secondary', 'secondary_link', 'tertiary',
    'tertiary_link', 'residential', 'unclassified', 'road', 'living_street',
}


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
def mz_recurse_up_transit_relations(seed_relations, osm):
    root_relation_ids = set()
    root_relation_level = 0
    all_relations = set()

    for rel_id in seed_relations:
        front = {rel_id}
        seen = {rel_id}
        level = 0

        if root_relation_level == 0:
            root_relation_ids.add(rel_id)

        while front:
            new_rels = set()
            for r in front:
                new_rels |= osm.transit_relations(r)
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


def is_station_or_stop(fid, shape, props):
    "Returns true if the given (point) feature is a station or stop."
    return (
        props.get('railway') in ('station', 'stop', 'tram_stop') or
        props.get('public_transport') in ('stop', 'stop_position', 'tram_stop')
    )


def is_station_or_line(fid, shape, props):
    """
    Returns true if the given (line or polygon from way) feature is a station
    or transit line.
    """

    railway = props.get('railway')
    return railway in ('subway', 'light_rail', 'tram', 'rail')


Transit = namedtuple(
    'Transit', 'score root_relation_id '
    'trains subways light_rails trams railways')


def mz_calculate_transit_routes_and_score(osm, node_id, way_id, rel_id):
    candidate_relations = set()
    if node_id:
        candidate_relations.update(osm.relations_using_node(node_id))
    if way_id:
        candidate_relations.update(osm.relations_using_way(way_id))
    if rel_id:
        candidate_relations.add(rel_id)

    seed_relations = set()
    for rel_id in candidate_relations:
        rel = osm.relation(rel_id)
        if rel and mz_is_interesting_transit_relation(rel.tags):
            seed_relations.add(rel_id)
    del candidate_relations

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
        seed_relations, osm)
    del seed_relations

    # collect all the interesting nodes - this includes the station node (if
    # any) and any nodes which are members of found relations which have
    # public transport tags indicating that they're stations or stops.
    stations_and_stops = set()
    for rel_id in all_relations:
        rel = osm.relation(rel_id)
        if not rel:
            continue
        for node_id in rel.node_ids:
            node = osm.node(node_id)
            if node and is_station_or_stop(*node):
                stations_and_stops.add(node_id)

    if node_id:
        stations_and_stops.add(node_id)

    # collect any physical railway which includes any of the above
    # nodes.
    stations_and_lines = set()
    for node_id in stations_and_stops:
        for way_id in osm.ways_using_node(node_id):
            way = osm.way(way_id)
            if way and is_station_or_line(*way):
                stations_and_lines.add(way_id)

    if way_id:
        stations_and_lines.add(way_id)

    # collect all IDs together in one array to intersect with the parts arrays
    # of route relations which may include them.
    all_routes = set()
    for lookup, ids in ((osm.relations_using_node, stations_and_stops),
                        (osm.relations_using_way, stations_and_lines),
                        (osm.relations_using_rel, all_relations)):
        for i in ids:
            for rel_id in lookup(i):
                rel = osm.relation(rel_id)
                if rel and \
                   rel.tags.get('type') == 'route' and \
                   rel.tags.get('route') in ('subway', 'light_rail', 'tram',
                                             'train', 'railway'):
                    all_routes.add(rel_id)

    routes_lookup = defaultdict(set)
    for rel_id in all_routes:
        rel = osm.relation(rel_id)
        if not rel:
            continue
        route = rel.tags.get('route')
        if route:
            route_name = mz_transit_route_name(rel.tags)
            routes_lookup[route].add(route_name)
    trains = list(sorted(routes_lookup['train']))
    subways = list(sorted(routes_lookup['subway']))
    light_rails = list(sorted(routes_lookup['light_rail']))
    trams = list(sorted(routes_lookup['tram']))
    railways = list(sorted(routes_lookup['railway']))
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


_TAG_NAME_ALTERNATES = (
    'name',
    'int_name',
    'loc_name',
    'nat_name',
    'official_name',
    'old_name',
    'reg_name',
    'short_name',
    'name_left',
    'name_right',
    'name:short',
)


_ALT_NAME_PREFIX_CANDIDATES = (
    'name:left:', 'name:right:', 'name:', 'alt_name:', 'old_name:'
)


# given a dictionary of key-value properties, returns a list of all the keys
# which represent names. this is used to assign all the names to a single
# layer. this makes sure that when we generate multiple features from a single
# database record, only one feature gets named and labelled.
def name_keys(props):
    name_keys = []
    for k in props.keys():
        is_name_key = k in _TAG_NAME_ALTERNATES

        if not is_name_key:
            for prefix in _ALT_NAME_PREFIX_CANDIDATES:
                if k.startswith(prefix):
                    is_name_key = True
                    break

        if is_name_key:
            name_keys.append(k)

    return name_keys


_US_ROUTE_MODIFIERS = {
    'Business',
    'Spur',
    'Truck',
    'Alternate',
    'Bypass',
    'Connector',
    'Historic',
    'Toll',
    'Scenic',
}


# properties for a feature (fid, shape, props) in layer `layer_name` at zoom
# level `zoom`. also takes an `osm` parameter, which is an object which can
# be used to look up nodes, ways and relations and the relationships between
# them.
def layer_properties(fid, shape, props, layer_name, zoom, osm):
    layer_props = props.copy()

    # drop the 'source' tag, if it exists. we override it anyway, and it just
    # gets confusing having multiple source tags. in the future, we may
    # replace the whole thing with a separate 'meta' for source.
    layer_props.pop('source', None)

    # need to make sure that the name is only applied to one of
    # the pois, landuse or buildings layers - in that order of
    # priority.
    if layer_name in ('pois', 'landuse', 'buildings'):
        for key in name_keys(layer_props):
            layer_props.pop(key, None)

    # urgh, hack!
    if layer_name == 'water' and shape.geom_type == 'Point':
        layer_props['label_placement'] = True

    if shape.geom_type in ('Polygon', 'MultiPolygon'):
        layer_props['area'] = shape.area

    if layer_name == 'roads' and \
       shape.geom_type in ('LineString', 'MultiLineString') and \
       fid >= 0:
        mz_networks = []
        mz_cycling_networks = set()
        mz_is_bus_route = False
        for rel_id in osm.relations_using_way(fid):
            rel = osm.relation(rel_id)
            if not rel:
                continue
            typ, route, network, ref, modifier = [rel.tags.get(k) for k in (
                'type', 'route', 'network', 'ref', 'modifier')]

            # the `modifier` tag gives extra information about the route, but
            # we want that information to be part of the `network` property.
            if network and modifier:
                modifier = modifier.capitalize()
                us_network = network.startswith('US:')
                us_route_modifier = modifier in _US_ROUTE_MODIFIERS
                # don't want to add the suffix if it's already there.
                suffix = ':' + modifier
                needs_suffix = suffix not in network
                if us_network and us_route_modifier and needs_suffix:
                    network += suffix

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
       is_point_or_poly:
        node_id = None
        way_id = None
        rel_id = None
        if shape.geom_type in ('Point', 'MultiPoint'):
            node_id = fid
        elif fid >= 0:
            way_id = fid
        else:
            rel_id = -fid

        transit = mz_calculate_transit_routes_and_score(
            osm, node_id, way_id, rel_id)
        layer_props['mz_transit_score'] = transit.score
        layer_props['mz_transit_root_relation_id'] = (
            transit.root_relation_id)
        layer_props['train_routes'] = transit.trains
        layer_props['subway_routes'] = transit.subways
        layer_props['light_rail_routes'] = transit.light_rails
        layer_props['tram_routes'] = transit.trams

    return layer_props
