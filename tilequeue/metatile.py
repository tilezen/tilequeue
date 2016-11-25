import zipfile
import StringIO
from tilequeue.format import zip_format


def make_metatiles(size, tiles):
    """
    Make a list of metatiles from a list of tiles.
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
