# define locations to store the rendered data

from boto import connect_s3
from boto.s3.bucket import Bucket
import md5
import os


def calc_hash(s):
    m = md5.new()
    m.update(s)
    md5_hash = m.hexdigest()
    return md5_hash[:5]


def s3_tile_key(date, path, layer, coord, extension):
    prefix = '/%s' % path if path else ''
    path_to_hash = '%(prefix)s/%(layer)s/%(z)d/%(x)d/%(y)d.%(ext)s' % dict(
        prefix=prefix,
        layer=layer,
        z=coord.zoom,
        x=coord.column,
        y=coord.row,
        ext=extension,
    )
    md5_hash = calc_hash(path_to_hash)
    s3_path = '/%(date)s/%(md5)s%(path_to_hash)s' % dict(
        date=date,
        md5=md5_hash,
        path_to_hash=path_to_hash,
    )
    return s3_path


class S3(object):

    def __init__(
            self, bucket, date_prefix, path, reduced_redundancy):
        self.bucket = bucket
        self.date_prefix = date_prefix
        self.path = path
        self.reduced_redundancy = reduced_redundancy

    def write_tile(self, tile_data, coord, format, layer):
        key_name = s3_tile_key(
            self.date_prefix, self.path, layer, coord, format.extension)
        key = self.bucket.new_key(key_name)
        key.set_contents_from_string(
            tile_data,
            headers={'Content-Type': format.mimetype},
            policy='public-read',
            reduced_redundancy=self.reduced_redundancy,
        )

    def read_tile(self, coord, format, layer):
        key_name = s3_tile_key(
            self.date_prefix, self.path, layer, coord, format.extension)
        key = self.bucket.get_key(key_name)
        if key is None:
            return None
        tile_data = key.get_contents_as_string()
        return tile_data


def make_dir_path(base_path, coord, layer):
    path = os.path.join(
        base_path, layer, str(int(coord.zoom)), str(int(coord.column)))
    return path


def make_file_path(base_path, coord, layer, extension):
    basefile_path = os.path.join(
        base_path, layer,
        str(int(coord.zoom)), str(int(coord.column)), str(int(coord.row)))
    ext_str = '.%s' % extension
    full_path = basefile_path + ext_str
    return full_path


class TileDirectory(object):
    '''
    Writes tiles to individual files in a local directory.
    '''

    def __init__(self, base_path):
        if os.path.exists(base_path):
            if not os.path.isdir(base_path):
                raise IOError(
                    '`{}` exists and is not a directory!'.format(base_path))
        else:
            os.makedirs(base_path)

        self.base_path = base_path

    def write_tile(self, tile_data, coord, format, layer):
        dir_path = make_dir_path(self.base_path, coord, layer)
        try:
            os.makedirs(dir_path)
        except OSError:
            pass
        file_path = make_file_path(self.base_path, coord, layer,
                                   format.extension)
        with open(file_path, 'w') as tile_fp:
            tile_fp.write(tile_data)

    def read_tile(self, coord, format, layer):
        file_path = make_file_path(self.base_path, coord, layer,
                                   format.extension)
        try:
            with open(file_path, 'r') as tile_fp:
                tile_data = tile_fp.read()
            return tile_data
        except IOError:
            return None


def make_tile_file_store(base_path=None):
    if base_path is None:
        base_path = 'tiles'
    return TileDirectory(base_path)


class Memory(object):

    def __init__(self):
        self.data = None

    def write_tile(self, tile_data, coord, format, layer):
        self.data = tile_data, coord, format, layer

    def read_tile(self, coord, format, layer):
        if self.data is None:
            return None
        tile_data, coord, format, layer = self.data
        return tile_data


def make_s3_store(bucket_name,
                  aws_access_key_id=None, aws_secret_access_key=None,
                  path='osm', reduced_redundancy=False, date_prefix=''):
    conn = connect_s3(aws_access_key_id, aws_secret_access_key)
    bucket = Bucket(conn, bucket_name)
    s3_store = S3(bucket, date_prefix, path, reduced_redundancy)
    return s3_store


def tiles_are_equal(tile_data_1, tile_data_2, fmt):
    """
    Returns True if the tile data is equal in tile_data_1 and tile_data_2. For
    most formats, this is a simple byte-wise equality check. For zipped
    metatiles, we need to check the contents, as the zip format includes
    metadata such as timestamps and doesn't control file ordering.
    """

    from tilequeue.format import zip_format

    if fmt and fmt == zip_format:
        from tilequeue.metatile import metatiles_are_equal
        return metatiles_are_equal(tile_data_1, tile_data_2)

    else:
        return tile_data_1 == tile_data_2


def write_tile_if_changed(store, tile_data, coord, format, layer):
    """
    Only write tile data if different from existing.

    Try to read the tile data from the store first. If the existing
    data matches, don't write. Returns whether the tile was written.
    """

    existing_data = store.read_tile(coord, format, layer)
    if not existing_data or \
       not tiles_are_equal(existing_data, tile_data, format):
        store.write_tile(tile_data, coord, format, layer)
        return True
    else:
        return False
