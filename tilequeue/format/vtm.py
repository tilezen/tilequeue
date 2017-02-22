# extracted from mapzen tilestache fork

from OSciMap4 import TileData_v4_pb2
from OSciMap4.GeomEncoder import GeomEncoder
from OSciMap4.StaticVals import getValues
from OSciMap4.StaticKeys import getKeys
from OSciMap4.TagRewrite import fixTag
import logging
import struct

statickeys = getKeys()
staticvals = getValues()

# custom keys/values start at attrib_offset
attrib_offset = 256

# coordindates are scaled to this range within tile
extents = 4096

# tiles are padded by this number of pixels for the current zoom level
# (OSciMap uses this to cover up seams between tiles)
padding = 5


def encode(file, features, layer_name=''):
        layer_name = layer_name or ''
        tile = VectorTile(extents)

        for feature in features:
            tile.addFeature(feature, layer_name)

        tile.complete()

        data = tile.out.SerializeToString()
        file.write(struct.pack(">I", len(data)))
        file.write(data)


def merge(file, feature_layers):
    ''' Retrieve a list of OSciMap4 tile responses and merge them into one.

        get_tiles() retrieves data and performs basic integrity checks.
    '''
    tile = VectorTile(extents)

    for layer in feature_layers:
        tile.addFeatures(layer['features'], layer['name'])

    tile.complete()

    data = tile.out.SerializeToString()
    file.write(struct.pack(">I", len(data)))
    file.write(data)


class VectorTile:
    """
    """
    def __init__(self, extents):
        self.geomencoder = GeomEncoder(extents)

        # TODO count to sort by number of occurrences
        self.keydict = {}
        self.cur_key = attrib_offset

        self.valdict = {}
        self.cur_val = attrib_offset

        self.tagdict = {}
        self.num_tags = 0

        self.out = TileData_v4_pb2.Data()
        self.out.version = 4

    def complete(self):
        if self.num_tags == 0:
            logging.info("empty tags")

        self.out.num_tags = self.num_tags

        if self.cur_key - attrib_offset > 0:
            self.out.num_keys = self.cur_key - attrib_offset

        if self.cur_val - attrib_offset > 0:
            self.out.num_vals = self.cur_val - attrib_offset

    def addFeatures(self, features, this_layer):
        for feature in features:
            self.addFeature(feature, this_layer)

    def addFeature(self, row, this_layer):
        geom = self.geomencoder
        tags = []

        # height = None
        layer = None
        # add layer tag
        tags.append(self.getTagId(('layer_name', this_layer)))
        for k, v in row[1].iteritems():
            if v is None:
                continue

            # the vtm stylesheet expects the heights to be an integer,
            # multiplied by 100
            if this_layer == 'buildings' and k in ('height', 'min_height'):
                try:
                    v = int(v * 100)
                except ValueError:
                    logging.warning('vtm: Invalid %s value: %s' % (k, v))

            tag = str(k), str(v)

            # use unsigned int for layer. i.e. map to 0..10
            if "layer" == tag[0]:
                layer = self.getLayer(tag[1])
                continue

            tag = fixTag(tag)

            if tag is None:
                continue

            tags.append(self.getTagId(tag))

        if len(tags) == 0:
            logging.debug('missing tags')
            return

        geom.parseGeometry(row[0])
        feature = None

        geometry_type = None
        if geom.isPoint:
            geometry_type = 'Point'
            feature = self.out.points.add()
            # add number of points (for multi-point)
            if len(geom.coordinates) > 2:
                logging.info('points %s' % len(geom.coordinates))
                feature.indices.append(len(geom.coordinates)/2)
        else:
            # empty geometry
            if len(geom.index) == 0:
                logging.debug('empty geom: %s %s' % row[1])
                return

            if geom.isPoly:
                geometry_type = 'Polygon'
                feature = self.out.polygons.add()
            else:
                geometry_type = 'LineString'
                feature = self.out.lines.add()

            # add coordinate index list (coordinates per geometry)
            feature.indices.extend(geom.index)

            # add indice count (number of geometries)
            if len(feature.indices) > 1:
                feature.num_indices = len(feature.indices)

        # add coordinates
        feature.coordinates.extend(geom.coordinates)

        # add geometry type to tags
        geometry_type_tag = 'geometry_type', geometry_type
        tags.append(self.getTagId(geometry_type_tag))

        # add tags
        feature.tags.extend(tags)
        if len(tags) > 1:
            feature.num_tags = len(tags)

        # add osm layer
        if layer is not None and layer != 5:
            feature.layer = layer

        # logging.debug('tags %d, indices %d' %(len(tags),len(feature.indices)))  # noqa

    def getLayer(self, val):
        try:
            l = max(min(10, int(val)) + 5, 0)
            if l != 0:
                return l
        except ValueError:
            logging.debug("layer invalid %s" % val)

        return None

    def getKeyId(self, key):
        if key in statickeys:
            return statickeys[key]

        if key in self.keydict:
            return self.keydict[key]

        self.out.keys.append(key)

        r = self.cur_key
        self.keydict[key] = r
        self.cur_key += 1
        return r

    def getAttribId(self, var):
        if var in staticvals:
            return staticvals[var]

        if var in self.valdict:
            return self.valdict[var]

        self.out.values.append(var)

        r = self.cur_val
        self.valdict[var] = r
        self.cur_val += 1
        return r

    def getTagId(self, tag):
        # logging.debug(tag)

        if tag in self.tagdict:
                return self.tagdict[tag]

        key = self.getKeyId(tag[0].decode('utf-8'))
        val = self.getAttribId(tag[1].decode('utf-8'))

        self.out.tags.append(key)
        self.out.tags.append(val)
        # logging.info("add tag %s - %d/%d" %(tag, key, val))
        r = self.num_tags
        self.tagdict[tag] = r
        self.num_tags += 1
        return r
