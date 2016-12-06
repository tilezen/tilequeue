import zipfile
import StringIO
from collections import defaultdict
from tilequeue.format import zip_format


def make_single_metatile(size, tiles):
    """
    Make a single metatile from a list of tiles all having the same
    coordinate and layer.
    """

    assert size == 1, \
        "Tilequeue only supports metatiles of size one at the moment."

    if len(tiles) == 0:
        return []

    coord = tiles[0]['coord']
    layer = tiles[0]['layer']

    buf = StringIO.StringIO()
    with zipfile.ZipFile(buf, mode='w') as z:
        for tile in tiles:
            assert tile['coord'] == coord
            assert tile['layer'] == layer

            tile_name = '0/0/0.%s' % tile['format'].extension
            tile_data = tile['tile']
            z.writestr(tile_name, tile_data)

    return [dict(tile=buf.getvalue(), format=zip_format, coord=coord,
                 layer=layer)]


def make_metatiles(size, tiles):
    """
    Group by coordinates and layers, and make metatiles out of all the tiles
    which share those properties.
    """

    groups = defaultdict(list)
    for tile in tiles:
        key = (tile['layer'], tile['coord'])
        groups[key].append(tile)

    metatiles = []
    for group in groups.itervalues():
        metatiles.extend(make_single_metatile(size, group))

    return metatiles


def extract_metatile(size, io, tile):
    """
    Extract the tile from the metatile given in the file-like object io.
    """

    assert size == 1, \
        "Tilequeue only supports metatiles of size one at the moment."

    tile_name = '0/0/0.%s' % tile['format'].extension

    with zipfile.ZipFile(io, mode='r') as zf:
        return zf.open(tile_name).read()
