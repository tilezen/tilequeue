# define locations to store the rendered data

import boto3
from botocore.exceptions import ClientError
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
from cStringIO import StringIO
from urllib import urlencode


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
    s3_path = '%(md5)s/%(date)s%(path_to_hash)s' % dict(
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
            self, s3_client, bucket_name, date_prefix, path,
            reduced_redundancy, delete_retry_interval, logger,
            object_acl, tags):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.date_prefix = date_prefix
        self.path = path
        self.reduced_redundancy = reduced_redundancy
        self.delete_retry_interval = delete_retry_interval
        self.logger = logger
        self.object_acl = object_acl
        self.tags = tags

    def write_tile(self, tile_data, coord, format, layer):
        key_name = s3_tile_key(
            self.date_prefix, self.path, layer, coord, format.extension)

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
                self.s3_client.put_object(**put_obj_props)
            except ClientError as e:
                # it's really useful for debugging if we know exactly what
                # request is failing.
                raise RuntimeError(
                    "Error while trying to write %r to bucket %r: %s"
                    % (key_name, self.bucket_name, str(e)))

        write_to_s3()

    def read_tile(self, coord, format, layer):
        key_name = s3_tile_key(
            self.date_prefix, self.path, layer, coord, format.extension)

        try:
            io = StringIO()
            self.s3_client.download_fileobj(self.bucket_name, key_name, io)
            return io.getvalue()

        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise

        return None

    def delete_tiles(self, coords, format, layer):
        key_names = [
            s3_tile_key(self.date_prefix, self.path, layer, coord,
                        format.extension).lstrip('/')
            for coord in coords
        ]

        num_deleted = 0
        chunk_size = 1000
        for idx in xrange(0, len(key_names), chunk_size):
            chunk = key_names[idx:idx+chunk_size]

            while chunk:
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

    def list_tiles(self, format, layer):
        ext = '.' + format.extension
        paginator = self.s3_client.get_paginator('list_objects_v2')
        page_iter = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=self.date_prefix
        )
        for page in page_iter:
            for key_obj in page['Contents']:
                key = key_obj['Key']
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
                  path='osm', reduced_redundancy=False, date_prefix='',
                  delete_retry_interval=60, logger=None,
                  object_acl='public-read', tags=None):
    s3 = boto3.client('s3')
    s3_store = S3(s3, bucket_name, date_prefix, path, reduced_redundancy,
                  delete_retry_interval, logger, object_acl, tags)
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


def make_store(yml, credentials={}, logger=None):
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
        object_acl = yml.get('object-acl', 'public-read')
        tags = yml.get('tags')

        return make_s3_store(
            bucket, path=path,
            reduced_redundancy=reduced_redundancy, date_prefix=date_prefix,
            delete_retry_interval=delete_retry_interval, logger=logger,
            object_acl=object_acl, tags=tags)

    else:
        raise ValueError('Unrecognized store type: `{}`'.format(store_type))
