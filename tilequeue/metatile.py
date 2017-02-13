import zipfile
import StringIO
from collections import defaultdict
from tilequeue.format import zip_format


def make_single_metatile(size, tiles, date_time=None):
    """
    Make a single metatile from a list of tiles all having the same
    coordinate and layer. Set date_time to a 6-tuple of (year, month,
    day, hour, minute, second) to set the timestamp for members.
    Otherwise the current wall clock time is used.
    """

    assert size == 1, \
        "Tilequeue only supports metatiles of size one at the moment."

    if len(tiles) == 0:
        return []

    if date_time is None:
        from time import gmtime
        date_time = gmtime()[0:6]

    coord = tiles[0]['coord']
    layer = tiles[0]['layer']

    buf = StringIO.StringIO()
    with zipfile.ZipFile(buf, mode='w') as z:
        for tile in tiles:
            assert tile['coord'] == coord
            assert tile['layer'] == layer

            tile_name = '0/0/0.%s' % tile['format'].extension
            tile_data = tile['tile']
            info = zipfile.ZipInfo(tile_name, date_time)
            z.writestr(info, tile_data)

    return [dict(tile=buf.getvalue(), format=zip_format, coord=coord,
                 layer=layer)]


def make_metatiles(size, tiles, date_time=None):
    """
    Group by coordinates and layers, and make metatiles out of all the tiles
    which share those properties. Provide a 6-tuple date_time to set the
    timestamp on each tile within the metatile, or leave it as None to use
    the current time.
    """

    groups = defaultdict(list)
    for tile in tiles:
        key = (tile['layer'], tile['coord'])
        groups[key].append(tile)

    metatiles = []
    for group in groups.itervalues():
        metatiles.extend(make_single_metatile(size, group, date_time))

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


def _metatile_contents_equal(zip_1, zip_2):
    """
    Given two open zip files as arguments, this returns True if the zips
    both contain the same set of files, having the same names, and each
    file within the zip is byte-wise identical to the one with the same
    name in the other zip.
    """

    names_1 = set(zip_1.namelist())
    names_2 = set(zip_2.namelist())

    if names_1 != names_2:
        return False

    for n in names_1:
        bytes_1 = zip_1.read(n)
        bytes_2 = zip_2.read(n)

        if bytes_1 != bytes_2:
            return False

    return True


def metatiles_are_equal(tile_data_1, tile_data_2):
    """
    Return True if the two tiles are both zipped metatiles and contain the
    same set of files with the same contents. This ignores the timestamp of
    the individual files in the zip files, as well as their order or any
    other metadata.
    """

    try:
        buf_1 = StringIO.StringIO(tile_data_1)
        buf_2 = StringIO.StringIO(tile_data_2)

        with zipfile.ZipFile(buf_1, mode='r') as zip_1:
            with zipfile.ZipFile(buf_2, mode='r') as zip_2:
                return _metatile_contents_equal(zip_1, zip_2)

    except (StandardError, zipfile.BadZipFile, zipfile.LargeZipFile):
        # errors, such as files not being proper zip files, or missing
        # some attributes or contents that we expect, are treated as not
        # equal.
        pass

    return False
