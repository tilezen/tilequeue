# define locations to store the rendered data

from boto import connect_s3
from boto.s3.bucket import Bucket
from builtins import range
from future.utils import raise_from
import md5
from ModestMaps.Core import Coordinate
import os
from tilequeue.metatile import metatiles_are_equal
from tilequeue.format import zip_format
import random
import threading
import time


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


def parse_coordinate_from_path(path, extension, layer):
    if path.endswith(extension):
        fields = path.rsplit('/', 4)
        if len(fields) == 5:
            _, tile_layer, z_str, x_str, y_fmt = fields
            if tile_layer == layer:
                y_fields = y_fmt.split('.')
                if y_fields:
                    y_str = y_fields[0]
                    try:
                        z = int(z_str)
                        x = int(x_str)
                        y = int(y_str)
                        coord = Coordinate(zoom=z, column=x, row=y)
                        return coord
                    except ValueError:
                        pass


class S3(object):

    def __init__(
            self, bucket, date_prefix, path, reduced_redundancy,
            delete_retry_interval):
        self.bucket = bucket
        self.date_prefix = date_prefix
        self.path = path
        self.reduced_redundancy = reduced_redundancy
        self.delete_retry_interval = delete_retry_interval

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
                        format.extension).lstrip('/')
            for coord in coords
        ]

        num_deleted = 0
        while key_names:
            del_result = self.bucket.delete_keys(key_names)
            num_deleted += len(del_result.deleted)

            key_names = []
            for error in del_result.errors:
                # retry on internal error. documentation implies that the only
                # possible two errors are AccessDenied and InternalError.
                # retrying when access denied seems unlikely to work, but an
                # internal error might be transient.
                if error.code == 'InternalError':
                    key_names.append(error.key)

            # pause a bit to give transient errors a chance to clear.
            if key_names:
                time.sleep(self.delete_retry_interval)

        # make sure that we deleted all the tiles - this seems like the
        # expected behaviour from the calling code.
        assert num_deleted == len(coords), \
            "Failed to delete some coordinates from S3."

        return num_deleted

    def list_tiles(self, format, layer):
        ext = '.' + format.extension
        for key_obj in self.bucket.list(prefix=self.date_prefix):
            key = key_obj.key
            coord = parse_coordinate_from_path(key, ext, layer)
            if coord:
                yield coord


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

    def list_tiles(self, format, layer):
        ext = '.' + format.extension
        for root, dirs, files in os.walk(self.base_path):
            for name in files:
                tile_path = '%s/%s' % (root, name)
                coord = parse_coordinate_from_path(tile_path, ext, layer)
                if coord:
                    yield coord


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

    def delete_tiles(self, coords, format, layer):
        pass

    def list_tiles(self, format, layer):
        return [self.data] if self.data else []


def make_s3_store(bucket_name,
                  aws_access_key_id=None, aws_secret_access_key=None,
                  path='osm', reduced_redundancy=False, date_prefix='',
                  delete_retry_interval=60):
    conn = connect_s3(aws_access_key_id, aws_secret_access_key)
    bucket = Bucket(conn, bucket_name)
    s3_store = S3(bucket, date_prefix, path, reduced_redundancy,
                  delete_retry_interval)
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


def make_store(yml, credentials={}):
    store_type = yml.get('type')

    if store_type == 'directory':
        path = yml.get('path')
        name = yml.get('name')
        return make_tile_file_store(path or name)

    elif store_type == 's3':
        bucket = yml.get('name')
        path = yml.get('path')
        reduced_redundancy = yml.get('reduced-redundancy')
        date_prefix = yml.get('date-prefix')
        delete_retry_interval = yml.get('delete-retry-interval')

        assert credentials, 'S3 store configured, but no AWS credentials ' \
            'provided. AWS credentials are required to use S3.'
        aws_access_key_id = credentials.get('aws_access_key_id')
        aws_secret_access_key = credentials.get('aws_secret_access_key')

        return make_s3_store(
            bucket, aws_access_key_id, aws_secret_access_key, path=path,
            reduced_redundancy=reduced_redundancy, date_prefix=date_prefix,
            delete_retry_interval=delete_retry_interval)

    else:
        raise ValueError('Unrecognized store type: `{}`'.format(store_type))
