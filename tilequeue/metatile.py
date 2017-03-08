import zipfile
import cStringIO as StringIO
from collections import defaultdict
from tilequeue.format import zip_format
from time import gmtime


def make_multi_metatile(parent, tiles, date_time=None):
    """
    Make a metatile containing a list of tiles all having the same layer,
    with coordinates relative to the given parent. Set date_time to a 6-tuple
    of (year, month, day, hour, minute, second) to set the timestamp for
    members. Otherwise the current wall clock time is used.
    """

    assert parent is not None, \
        "Parent tile must be provided and not None to make a metatile."

    if len(tiles) == 0:
        return []

    if date_time is None:
        date_time = gmtime()[0:6]

    layer = tiles[0]['layer']

    buf = StringIO.StringIO()
    with zipfile.ZipFile(buf, mode='w') as z:
        for tile in tiles:
            assert tile['layer'] == layer

            coord = tile['coord']

            # change in zoom level from parent to coord. since parent should
            # be a parent, its zoom should always be equal or smaller to that
            # of coord.
            delta_z = coord.zoom - parent.zoom
            assert delta_z >= 0, "Coordinates must be descendents of parent"

            # change in row/col coordinates are relative to the upper left
            # coordinate at that zoom. both should be positive.
            delta_row = coord.row - (int(parent.row) << delta_z)
            delta_column = coord.column - (int(parent.column) << delta_z)
            assert delta_row >= 0, \
                "Coordinates must be contained by their parent, but " + \
                "row is not."
            assert delta_column >= 0, \
                "Coordinates must be contained by their parent, but " + \
                "column is not."

            tile_name = '%d/%d/%d.%s' % \
                (delta_z, delta_column, delta_row, tile['format'].extension)
            tile_data = tile['tile']
            info = zipfile.ZipInfo(tile_name, date_time)
            z.writestr(info, tile_data, zipfile.ZIP_DEFLATED)

    return [dict(tile=buf.getvalue(), format=zip_format, coord=parent,
                 layer=layer)]


def _common_parent(a, b):
    """
    Find the common parent tile of both a and b. The common parent is the tile
    at the highest zoom which both a and b can be transformed into by lowering
    their zoom levels.
    """

    if a.zoom < b.zoom:
        b = b.zoomTo(a.zoom).container()

    elif a.zoom > b.zoom:
        a = a.zoomTo(b.zoom).container()

    while a.row != b.row or a.column != b.column:
        a = a.zoomBy(-1).container()
        b = b.zoomBy(-1).container()

    # by this point a == b.
    return a


def _parent_tile(tiles):
    """
    Find the common parent tile for a sequence of tiles.
    """
    parent = None
    for t in tiles:
        if parent is None:
            parent = t

        else:
            parent = _common_parent(parent, t)

    return parent


def make_metatiles(size, tiles, date_time=None):
    """
    Group by layers, and make metatiles out of all the tiles which share those
    properties relative to the "top level" tile which is parent of them all.
    Provide a 6-tuple date_time to set the timestamp on each tile within the
    metatile, or leave it as None to use the current time.
    """

    groups = defaultdict(list)
    for tile in tiles:
        key = tile['layer']
        groups[key].append(tile)

    metatiles = []
    for group in groups.itervalues():
        parent = _parent_tile(t['coord'] for t in group)
        metatiles.extend(make_multi_metatile(parent, group, date_time))

    return metatiles


def extract_metatile(io, fmt, offset=None):
    """
    Extract the tile at the given offset (defaults to 0/0/0) and format from
    the metatile in the file-like object io.
    """

    ext = fmt.extension
    if offset is None:
        tile_name = '0/0/0.%s' % ext
    else:
        tile_name = '%d/%d/%d.%s' % (offset.zoom, offset.column, offset.row,
                                     ext)

    with zipfile.ZipFile(io, mode='r') as zf:
        if tile_name in zf.namelist():
            return zf.read(tile_name)
        else:
            return None


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
