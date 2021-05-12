# define locations to store the rendered data

import boto3
from botocore.exceptions import ClientError
from builtins import range
from enum import Enum
from future.utils import raise_from
import md5
from ModestMaps.Core import Coordinate
import os
from tilequeue.metatile import metatiles_are_equal
from tilequeue.format import zip_format
import random
import threading
import time
from cStringIO import StringIO
from urllib import urlencode


def calc_hash(s):
    m = md5.new()
    m.update(s)
    md5_hash = m.hexdigest()
    return md5_hash[:5]


class KeyFormatType(Enum):
    """represents a type of s3 key path pattern"""

    # hash comes before prefix
    hash_prefix = 1

    # prefix comes before hash
    prefix_hash = 2


class S3TileKeyGenerator(object):
    """
    generates an s3 path

    The S3 store delegates here to generate the s3 key path for the tile.
    """

    def __init__(self, key_format_type=None, key_format=None):
        if key_format is not None and key_format_type is not None:
            raise ValueError('key_format and key_format_type both set')
        if key_format_type is not None:
            if key_format_type == KeyFormatType.hash_prefix:
                key_format = '%(hash)s/%(prefix)s/%(path)s'
            elif key_format_type == KeyFormatType.prefix_hash:
                key_format = '%(prefix)s/%(hash)s/%(path)s'
            else:
                raise ValueError('unknown key_format_type: %r' %
                                 key_format_type)
        self.key_format = key_format

    def __call__(self, prefix, coord, extension):
        path_to_hash = '%d/%d/%d.%s' % (
            coord.zoom, coord.column, coord.row, extension)
        md5_hash = calc_hash(path_to_hash)
        s3_key_path = self.key_format % dict(
            prefix=prefix,
            hash=md5_hash,
            path=path_to_hash,
        )
        return s3_key_path


def parse_coordinate_from_path(path, extension):
    if path.endswith(extension):
        fields = path.rsplit('/', 3)
        if len(fields) == 4:
            _, z_str, x_str, y_fmt = fields
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


# decorates a function to back off and retry
def _backoff_and_retry(ExceptionType, num_tries=5, retry_factor=2,
                       retry_interval=1, logger=None):
    from time import sleep
    from functools import wraps

    def decorator(f):
        @wraps(f)
        def func(*args, **kwargs):
            # do the first num_tries-1 attempts wrapped in something to catch
            # any exceptions, optionally log them, and try again.
            interval = retry_interval
            factor = retry_factor

            for _ in xrange(1, num_tries):
                try:
                    return f(*args, **kwargs)

                except ExceptionType as e:
                    if logger:
                        logger.warning("Failed. Backing off and retrying. "
                                       "Error: %s" % str(e))

                sleep(interval)
                interval *= factor

            # do final attempt without try-except, so we get the exception
            # in normal code.
            return f(*args, **kwargs)

        return func
    return decorator


class S3(object):

    def __init__(
            self, s3_client, bucket_name, date_prefix,
            reduced_redundancy, delete_retry_interval, logger,
            object_acl, tags, tile_key_gen, verbose_log=True):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.date_prefix = date_prefix
        self.reduced_redundancy = reduced_redundancy
        self.delete_retry_interval = delete_retry_interval
        self.logger = logger
        self.object_acl = object_acl
        self.tags = tags
        self.tile_key_gen = tile_key_gen
        self.verbose_log = verbose_log

    def write_tile(self, tile_data, coord, format):
        key_name = self.tile_key_gen(
            self.date_prefix, coord, format.extension)

        storage_class = 'STANDARD'
        if self.reduced_redundancy:
            storage_class = 'REDUCED_REDUNDANCY'

        @_backoff_and_retry(Exception, logger=self.logger)
        def write_to_s3():
            put_obj_props = dict(
                Bucket=self.bucket_name,
                Key=key_name,
                Body=tile_data,
                ContentType=format.mimetype,
                ACL=self.object_acl,
                StorageClass=storage_class,
            )
            if self.tags:
                put_obj_props['Tagging'] = urlencode(self.tags)
            try:
                if self.verbose_log:
                    if self.logger:
                        self.logger.info("[tilequeue]Write tile to key: {}".format(key_name))
                    else:
                        print("[tilequeue]Write tile to key: {}".format(key_name))
                self.s3_client.put_object(**put_obj_props)
            except ClientError as e:
                # it's really useful for debugging if we know exactly what
                # request is failing.
                raise RuntimeError(
                    "Error while trying to write %r to bucket %r: %s"
                    % (key_name, self.bucket_name, str(e)))

        write_to_s3()

    def read_tile(self, coord, format):
        key_name = self.tile_key_gen(
            self.date_prefix, coord, format.extension)

        try:
            io = StringIO()
            if self.verbose_log:
                if self.logger:
                    self.logger.info("[tilequeue]Read tile from key: {}".format(key_name))
                else:
                    print("[tilequeue]Read tile from key: {}".format(key_name))
            self.s3_client.download_fileobj(self.bucket_name, key_name, io)
            return io.getvalue()

        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise

        return None

    def delete_tiles(self, coords, format):
        key_names = [
            self.tile_key_gen(
                self.date_prefix, coord, format.extension).lstrip('/')
            for coord in coords
        ]

        num_deleted = 0
        chunk_size = 1000
        for idx in xrange(0, len(key_names), chunk_size):
            chunk = key_names[idx:idx+chunk_size]

            while chunk:
                if self.verbose_log:
                    if self.logger:
                        self.logger.info("[tilequeue]Delete tile of keys: {}".
                                         format(",".join(chunk)))
                    else:
                        print("[tilequeue]Delete tile of keys: {}".
                              format(",".join(chunk)))

                objects = [dict(Key=k) for k in chunk]
                del_result = self.s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete=dict(Objects=objects),
                )
                num_deleted += len(del_result['Deleted'])

                chunk = []
                for error in del_result['Errors']:
                    # retry on internal error. documentation implies that the
                    # only possible two errors are AccessDenied and
                    # InternalError. retrying when access denied seems
                    # unlikely to work, but an internal error might be
                    # transient.
                    if error['Code'] == 'InternalError':
                        chunk.append(error['Key'])

                # pause a bit to give transient errors a chance to clear.
                if chunk:
                    time.sleep(self.delete_retry_interval)

        # make sure that we deleted all the tiles - this seems like the
        # expected behaviour from the calling code.
        assert num_deleted == len(coords), \
            "Failed to delete some coordinates from S3."

        return num_deleted

    def list_tiles(self, format):
        ext = '.' + format.extension
        paginator = self.s3_client.get_paginator('list_objects_v2')
        page_iter = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=self.date_prefix
        )
        for page in page_iter:
            for key_obj in page['Contents']:
                key = key_obj['Key']
                coord = parse_coordinate_from_path(key, ext)
                if coord:
                    yield coord


def make_dir_path(base_path, coord):
    path = os.path.join(
        base_path, str(int(coord.zoom)), str(int(coord.column)))
    return path


def make_file_path(base_path, coord, extension):
    basefile_path = os.path.join(
        base_path,
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

    def write_tile(self, tile_data, coord, format):
        dir_path = make_dir_path(self.base_path, coord)
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

        file_path = make_file_path(self.base_path, coord, format.extension)
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

    def read_tile(self, coord, format):
        file_path = make_file_path(self.base_path, coord, format.extension)
        try:
            with open(file_path, 'r') as tile_fp:
                tile_data = tile_fp.read()
            return tile_data
        except IOError:
            return None

    def delete_tiles(self, coords, format):
        delete_count = 0
        for coord in coords:
            file_path = make_file_path(
                self.base_path, coord, format.extension)
            if os.path.isfile(file_path):
                os.remove(file_path)
                delete_count += 1

        return delete_count

    def list_tiles(self, format):
        ext = '.' + format.extension
        for root, dirs, files in os.walk(self.base_path):
            for name in files:
                tile_path = '%s/%s' % (root, name)
                coord = parse_coordinate_from_path(tile_path, ext)
                if coord:
                    yield coord


def make_tile_file_store(base_path=None):
    if base_path is None:
        base_path = 'tiles'
    return TileDirectory(base_path)


class Memory(object):

    def __init__(self):
        self.data = None

    def write_tile(self, tile_data, coord, format):
        self.data = tile_data, coord, format

    def read_tile(self, coord, format):
        if self.data is None:
            return None
        tile_data, coord, format = self.data
        return tile_data

    def delete_tiles(self, coords, format):
        pass

    def list_tiles(self, format):
        return [self.data] if self.data else []


class MultiStore(object):
    """
    MultiStore writes to multiple stores for redundancy.

    The stores are written in order from first to last, and checked in reverse
    order. Assuming that previously-written files don't disappear (which might
    not be true, but hopefully is at least very rare) then this should have the
    desired behaviour under crash conditions, either:

     1. The crash happened before the last tile was written, in which case some
        of the preceding stores may not have the tile and it should be
        re-rendered and stored again. Or,
     2. The crash happened after the last tile was written, in which case all
        preceding tiles should be present too.

    There's an optimisation we could make later, by checking the first tile if
    the last doesn't exist and copying it to the other stores if it does.
    """

    def __init__(self, stores):
        assert len(stores) > 0
        self.stores = stores

    def write_tile(self, tile_data, coord, format):
        for store in self.stores:
            store.write_tile(tile_data, coord, format)

    def read_tile(self, coord, format):
        return self.stores[-1].read_tile(coord, format)

    def delete_tiles(self, coords, format):
        num = 0
        for store in self.stores:
            num = store.delete_tiles(coords, format)

        # only returns the last-seen value, but this should normally be the
        # same as all the other values.
        return num

    def list_tiles(self, format):
        return self.stores[-1].list_tiles(self, format)


def _make_s3_store(cfg_name, constructor):
    # if buckets are given as a list, then write to each of them and read from
    # the last one. this behaviour is captured in MultiStore.
    if isinstance(cfg_name, list):
        s3_stores = []
        for bucket in cfg_name:
            s3_store = constructor(bucket)
            s3_stores.append(s3_store)

        s3_store = MultiStore(s3_stores)

    else:
        s3_store = constructor(cfg_name)

    return s3_store


def make_s3_store(cfg_name, tile_key_gen,
                  reduced_redundancy=False, date_prefix='',
                  delete_retry_interval=60, logger=None,
                  object_acl='public-read', tags=None, verbose_log=False):
    s3 = boto3.client('s3')

    # extract out the construction of the bucket, so that it can be abstracted
    # from the the logic of interpreting the configuration file.
    def _construct(bucket_name):
        return S3(
            s3, bucket_name, date_prefix, reduced_redundancy,
            delete_retry_interval, logger, object_acl, tags,
            tile_key_gen, verbose_log)

    return _make_s3_store(cfg_name, _construct)


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


def write_tile_if_changed(store, tile_data, coord, format):
    """
    Only write tile data if different from existing.

    Try to read the tile data from the store first. If the existing
    data matches, don't write. Returns whether the tile was written.
    """

    existing_data = store.read_tile(coord, format)
    if not existing_data or \
       not tiles_are_equal(existing_data, tile_data, format):
        store.write_tile(tile_data, coord, format)
        return True
    else:
        return False


def make_s3_tile_key_generator(yml_cfg):
    key_format_type_str = yml_cfg.get('key-format-type')
    key_format_type = None
    if key_format_type_str is None or key_format_type_str == 'hash-prefix':
        # if unspecified, prefer hash before prefix
        key_format_type = KeyFormatType.hash_prefix
    elif key_format_type_str == 'prefix-hash':
        key_format_type = KeyFormatType.prefix_hash
    else:
        raise ValueError('unknown s3 key-format: %r' % key_format_type_str)
    return S3TileKeyGenerator(key_format_type=key_format_type)


def make_store(yml, credentials={}, logger=None, verbose_log=False):
    store_type = yml.get('type')

    if store_type == 'directory':
        path = yml.get('path')
        name = yml.get('name')
        return make_tile_file_store(path or name)

    elif store_type == 's3':
        bucket = yml.get('name')
        reduced_redundancy = yml.get('reduced-redundancy')
        date_prefix = yml.get('date-prefix')
        delete_retry_interval = yml.get('delete-retry-interval')
        object_acl = yml.get('object-acl', 'public-read')
        tags = yml.get('tags')
        tile_key_gen = make_s3_tile_key_generator(yml)

        return make_s3_store(
            bucket, tile_key_gen,
            reduced_redundancy=reduced_redundancy,
            date_prefix=date_prefix,
            delete_retry_interval=delete_retry_interval, logger=logger,
            object_acl=object_acl, tags=tags, verbose_log=verbose_log)

    else:
        raise ValueError('Unrecognized store type: `{}`'.format(store_type))
