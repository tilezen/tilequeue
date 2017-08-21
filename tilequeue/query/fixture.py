from collections import namedtuple
from shapely.geometry import box
from tilequeue.process import lookup_source
from itertools import izip
from tilequeue.transform import calculate_padded_bounds


LayerInfo = namedtuple('LayerInfo', 'min_zoom_fn props_fn')


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


class DataFetcher(object):

    def __init__(self, layers, rows, label_placement_layers):
        """
        Expect layers to be a dict of layer name to LayerInfo. Expect rows to
        be a list of (fid, shape, properties). Label placement layers should
        be a set of layer names for which to generate label placement points.
        """

        self.layers = layers
        self.rows = rows
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

                # if the feature exists in any label placement layer, then we
                # should consider generating a centroid (if it's a polygon)
                if layer_name in self.label_placement_layers:
                    generate_label_placement = True

                layer_props = props.copy()
                layer_props['min_zoom'] = min_zoom

                # urgh, hack!
                if layer_name == 'water' and shape.geom_type == 'Point':
                    layer_props['label_placement'] = True

                if shape.geom_type in ('Polygon', 'MultiPolygon'):
                    layer_props['area'] = shape.area

                if layer_name == 'roads' and \
                   shape.geom_type in ('LineString', 'MultiLineString'):
                    mz_networks = []
                    mz_cycling_networks = set()
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

                    mz_cycling_network = None
                    for cn in ('icn', 'ncn', 'rcn', 'lcn'):
                        if layer_props.get(cn) == 'yes' or \
                           ('%s_ref' % cn) in layer_props or \
                           cn in mz_cycling_networks:
                            mz_cycling_network = cn
                            break

                    layer_props['mz_networks'] = mz_networks
                    if mz_cycling_network:
                        layer_props['mz_cycling_network'] = mz_cycling_network

                if layer_props:
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

                read_row['__id__'] = fid
                read_row['__geometry__'] = bytes(clip_shape.wkb)
                if shape.geom_type in ('Polygon', 'MultiPolygon') and \
                   generate_label_placement:
                    read_row['__label__'] = bytes(
                        shape.representative_point().wkb)
                read_rows.append(read_row)

        return read_rows


def make_fixture_data_fetcher(layers, rows, label_placement_layers=set()):
    return DataFetcher(layers, rows, label_placement_layers)
