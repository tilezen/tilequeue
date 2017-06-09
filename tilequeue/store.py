# define locations to store the rendered data

from boto import connect_s3
from boto.s3.bucket import Bucket
from builtins import range
from future.utils import raise_from
import md5
import os
from tilequeue.metatile import metatiles_are_equal
from tilequeue.format import zip_format
import random
import threading


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

    def delete_tiles(self, coords, format, layer):
        key_names = [
            s3_tile_key(self.date_prefix, self.path, layer, coord,
                        format.extension)
            for coord in coords
        ]
        del_result = self.bucket.delete_keys(key_names)
        return len(del_result.deleted)


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


def os_replace(src, dst):
    '''
    Simple emulation of function `os.replace(..)` from modern version
    of Python. Implementation is not fully atomic, but enough for us.
    '''

    orig_os_replace_func = getattr(os, 'replace', None)

    if orig_os_replace_func is not None:
        # not need for emulation: we using modern version of Python.
        # fully atomic for this case

        orig_os_replace_func(src, dst)
        return

    if os.name == 'posix':
        # POSIX requirement: `os.rename(..)` works as `os.replace(..)`
        # fully atomic for this case

        os.rename(src, dst)
        return

    # simple emulation for `os.name == 'nt'` and other marginal
    # operation systems.  not fully atomic implementation for this
    # case

    try:
        # trying atomic `os.rename(..)` without `os.remove(..)` or
        # other operations

        os.rename(src, dst)
        error = None
    except OSError as e:
        error = e

    if error is None:
        return

    for i in range(5):
        # some number of tries may be failed
        # because we may be in concurrent environment with other
        # processes/threads

        try:
            os.remove(dst)
        except OSError:
            # destination was not exist
            # or concurrent process/thread is removing it in parallel with us
            pass

        try:
            os.rename(src, dst)
            error = None
        except OSError as e:
            error = e
            continue

        break

    if error is not None:
        raise_from(OSError('failed to replace'), error)


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
        swap_file_path = '%s.swp-%s-%s-%s' % (
            file_path,
            os.getpid(),
            threading.currentThread().ident,
            random.randint(1, 1000000)
        )

        try:
            with open(swap_file_path, 'w') as tile_fp:
                tile_fp.write(tile_data)

            # write file as atomic operation
            os_replace(swap_file_path, file_path)
        except Exception as e:
            try:
                os.remove(swap_file_path)
            except OSError:
                pass
            raise e

    def read_tile(self, coord, format, layer):
        file_path = make_file_path(self.base_path, coord, layer,
                                   format.extension)
        try:
            with open(file_path, 'r') as tile_fp:
                tile_data = tile_fp.read()
            return tile_data
        except IOError:
            return None

    def delete_tiles(self, coords, format, layer):
        delete_count = 0
        for coord in coords:
            file_path = make_file_path(
                self.base_path, coord, layer, format.extension)
            if os.path.isfile(file_path):
                os.remove(file_path)
                delete_count += 1

        return delete_count


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

    if fmt and fmt == zip_format:
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
