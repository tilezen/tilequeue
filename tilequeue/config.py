from tilequeue.tile import bounds_buffer
from tilequeue.tile import metatile_zoom_from_size
from yaml import load
import os


class Configuration(object):
    '''
    Flatten configuration from yaml
    '''

    def __init__(self, yml):
        self.yml = yml

        self.aws_access_key_id = \
            self._cfg('aws credentials aws_access_key_id') or \
            os.environ.get('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = \
            self._cfg('aws credentials aws_secret_access_key') or \
            os.environ.get('AWS_SECRET_ACCESS_KEY')

        self.queue_cfg = self.yml['queue']

        self.store_type = self._cfg('store type')
        self.s3_bucket = self._cfg('store name')
        self.s3_reduced_redundancy = self._cfg('store reduced-redundancy')
        self.s3_path = self._cfg('store path')
        self.s3_date_prefix = self._cfg('store date-prefix')
        self.s3_delete_retry_interval = \
            self._cfg('store delete-retry-interval')

        seed_cfg = self.yml['tiles']['seed']
        self.seed_all_zoom_start = seed_cfg['all']['zoom-start']
        self.seed_all_zoom_until = seed_cfg['all']['zoom-until']
        self.seed_n_threads = seed_cfg['n-threads']

        seed_metro_cfg = seed_cfg['metro-extract']
        self.seed_metro_extract_url = seed_metro_cfg['url']
        self.seed_metro_extract_zoom_start = seed_metro_cfg['zoom-start']
        self.seed_metro_extract_zoom_until = seed_metro_cfg['zoom-until']
        self.seed_metro_extract_cities = seed_metro_cfg['cities']

        seed_top_tiles_cfg = seed_cfg['top-tiles']
        self.seed_top_tiles_url = seed_top_tiles_cfg['url']
        self.seed_top_tiles_zoom_start = seed_top_tiles_cfg['zoom-start']
        self.seed_top_tiles_zoom_until = seed_top_tiles_cfg['zoom-until']

        toi_store_cfg = self.yml['toi-store']
        self.toi_store_type = toi_store_cfg['type']
        if self.toi_store_type == 's3':
            self.toi_store_s3_bucket = toi_store_cfg['s3']['bucket']
            self.toi_store_s3_key = toi_store_cfg['s3']['key']
        elif self.toi_store_type == 'file':
            self.toi_store_file_name = toi_store_cfg['file']['name']

        self.seed_should_add_to_tiles_of_interest = \
            seed_cfg['should-add-to-tiles-of-interest']

        seed_custom = seed_cfg['custom']
        self.seed_custom_zoom_start = seed_custom['zoom-start']
        self.seed_custom_zoom_until = seed_custom['zoom-until']
        self.seed_custom_bboxes = seed_custom['bboxes']
        if self.seed_custom_bboxes:
            for bbox in self.seed_custom_bboxes:
                assert len(bbox) == 4, (
                    'Seed config: custom bbox {} does not have exactly '
                    'four elements!').format(bbox)
                min_x, min_y, max_x, max_y = bbox
                assert min_x < max_x, \
                    'Invalid bbox. {} not less than {}'.format(min_x, max_x)
                assert min_y < max_y, \
                    'Invalid bbox. {} not less than {}'.format(min_y, max_y)

        self.seed_unique = seed_cfg['unique']

        intersect_cfg = self.yml['tiles']['intersect']
        self.intersect_expired_tiles_location = (
            intersect_cfg['expired-location'])
        self.intersect_zoom_until = intersect_cfg['parent-zoom-until']

        self.logconfig = self._cfg('logging config')
        self.redis_type = self._cfg('redis type')
        self.redis_host = self._cfg('redis host')
        self.redis_port = self._cfg('redis port')
        self.redis_db = self._cfg('redis db')
        self.redis_cache_set_key = self._cfg('redis cache-set-key')

        self.statsd_host = None
        if self.yml.get('statsd'):
            self.statsd_host = self._cfg('statsd host')
            self.statsd_port = self._cfg('statsd port')
            self.statsd_prefix = self._cfg('statsd prefix')

        process_cfg = self.yml['process']
        self.n_simultaneous_query_sets = \
            process_cfg['n-simultaneous-query-sets']
        self.n_simultaneous_s3_storage = \
            process_cfg['n-simultaneous-s3-storage']
        self.log_queue_sizes = process_cfg['log-queue-sizes']
        self.log_queue_sizes_interval_seconds = \
            process_cfg['log-queue-sizes-interval-seconds']
        self.query_cfg = process_cfg['query-config']
        self.template_path = process_cfg['template-path']
        self.reload_templates = process_cfg['reload-templates']
        self.output_formats = process_cfg['formats']
        self.buffer_cfg = process_cfg['buffer']
        self.process_yaml_cfg = process_cfg['yaml']

        self.postgresql_conn_info = self.yml['postgresql']
        dbnames = self.postgresql_conn_info.get('dbnames')
        assert dbnames is not None, 'Missing postgresql dbnames'
        assert isinstance(dbnames, (tuple, list)), \
            "Expecting postgresql 'dbnames' to be a list"
        assert len(dbnames) > 0, 'No postgresql dbnames configured'

        self.wof = self.yml.get('wof')

        self.metatile_size = self._cfg('metatile size')
        self.metatile_zoom = metatile_zoom_from_size(self.metatile_size)
        self.metatile_start_zoom = self._cfg('metatile start-zoom')

        self.max_zoom_with_changes = self._cfg('tiles max-zoom-with-changes')
        assert self.max_zoom_with_changes > self.metatile_zoom
        self.max_zoom = self.max_zoom_with_changes - self.metatile_zoom

        self.sql_queue_buffer_size = self._cfg('queue_buffer_size sql')
        self.proc_queue_buffer_size = self._cfg('queue_buffer_size proc')
        self.s3_queue_buffer_size = self._cfg('queue_buffer_size s3')

        self.tile_traffic_log_path = self._cfg(
            'toi-prune tile-traffic-log-path')

        self.group_by_zoom = self.subtree('rawr group-zoom')

        self.tile_sizes = self._cfg('metatile tile-sizes')
        if self.tile_sizes is None:
            self.tile_sizes = [256 * (1 << z) for z in
                               reversed(xrange(0, self.metatile_zoom + 1))]

    def __repr__(self):
        return 'yml: {yml},\n' \
               'aws_access_key_id: {aws_access_key_id},\n' \
               'aws_secret_access_key: {aws_secret_access_key},\n' \
               'queue_cfg: {queue_cfg},\n' \
               'store_type: {store_type},\n' \
               's3_bucket: {s3_bucket},\n' \
               's3_reduced_redundancy: {s3_reduced_redundancy},\n' \
               's3_path: {s3_path},\n' \
               's3_date_prefix: {s3_date_prefix},\n' \
               's3_delete_retry_interval: {s3_delete_retry_interval},\n' \
               'seed_all_zoom_start: {seed_all_zoom_start},\n' \
               'seed_all_zoom_until: {seed_all_zoom_until},\n' \
               'seed_n_threads: {seed_n_threads},\n' \
               'seed_metro_extract_url: {seed_metro_extract_url},\n' \
               'seed_metro_extract_zoom_start: {seed_metro_extract_zoom_start},\n' \
               'seed_metro_extract_zoom_until: {seed_metro_extract_zoom_until},\n' \
               'seed_metro_extract_cities: {seed_metro_extract_cities},\n' \
               'seed_top_tiles_url: {seed_top_tiles_url},\n' \
               'seed_top_tiles_zoom_start: {seed_top_tiles_zoom_start},\n' \
               'seed_top_tiles_zoom_until: {seed_top_tiles_zoom_until},\n' \
               'seed_top_tiles_url: {seed_top_tiles_url},\n' \
               'toi_store_type: {toi_store_type},\n' \
               'toi_store_s3_bucket: {toi_store_s3_bucket},\n' \
               'toi_store_s3_key: {toi_store_s3_key},\n' \
               'toi_store_file_name: {toi_store_file_name},\n' \
               'seed_custom_zoom_start: {seed_custom_zoom_start},\n' \
               'seed_should_add_to_tiles_of_interest: {seed_should_add_to_tiles_of_interest},\n' \
               'seed_custom_zoom_until: {seed_custom_zoom_until},\n' \
               'seed_unique: {seed_unique},\n' \
               'intersect_expired_tiles_location: {intersect_expired_tiles_location},\n' \
               'intersect_zoom_until: {intersect_zoom_until},\n' \
               'logconfig: {logconfig},\n' \
               'redis_type: {redis_type},\n' \
               'redis_host: {redis_host},\n' \
               'redis_port: {redis_port},\n' \
               'redis_db: {redis_db},\n' \
               'redis_cache_set_key: {redis_cache_set_key},\n' \
               'statsd_host: {statsd_host},\n' \
               'statsd_port: {statsd_port},\n' \
               'statsd_prefix: {statsd_prefix},\n' \
               'n_simultaneous_query_sets: {n_simultaneous_query_sets},\n' \
               'n_simultaneous_s3_storage: {n_simultaneous_s3_storage},\n' \
               'log_queue_sizes: {log_queue_sizes},\n' \
               'log_queue_sizes_interval_seconds: {log_queue_sizes_interval_seconds},\n' \
               'query_cfg: {query_cfg},\n' \
               'template_path: {template_path},\n' \
               'reload_templates: {reload_templates},\n' \
               'output_formats: {output_formats},\n' \
               'buffer_cfg: {buffer_cfg},\n' \
               'process_yaml_cfg: {process_yaml_cfg},\n' \
               'postgresql_conn_info: {postgresql_conn_info},\n' \
               'metatile_size: {metatile_size},\n' \
               'metatile_zoom: {metatile_zoom},\n' \
               'metatile_start_zoom: {metatile_start_zoom},\n' \
               'max_zoom_with_changes: {max_zoom_with_changes},\n' \
               'max_zoom: {max_zoom},\n' \
               'sql_queue_buffer_size: {sql_queue_buffer_size},\n' \
               'proc_queue_buffer_size: {proc_queue_buffer_size},\n' \
               's3_queue_buffer_size: {s3_queue_buffer_size},\n' \
               'tile_traffic_log_path: {tile_traffic_log_path},\n' \
               'group_by_zoom: {group_by_zoom},\n' \
               'tile_sizes: {tile_sizes}\n'.format(
            yml=self.yml,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            queue_cfg=self.queue_cfg,
            store_type=self.store_type,
            s3_bucket=self.s3_bucket,
            s3_reduced_redundancy=self.s3_reduced_redundancy,
            s3_path=self.s3_path,
            s3_date_prefix=self.s3_date_prefix,
            s3_delete_retry_interval=self.s3_delete_retry_interval,
            seed_all_zoom_start=self.seed_all_zoom_start,
            seed_all_zoom_until=self.seed_all_zoom_until,
            seed_n_threads=self.seed_n_threads,
            seed_metro_extract_url=self.seed_metro_extract_url,
            seed_metro_extract_zoom_start=self.seed_metro_extract_zoom_start,
            seed_metro_extract_zoom_until=self.seed_metro_extract_zoom_until,
            seed_metro_extract_cities=self.seed_metro_extract_cities,
            seed_top_tiles_url=self.seed_top_tiles_url if self.seed_top_tiles_url is not None else 'None',
            seed_top_tiles_zoom_start=self.seed_top_tiles_zoom_start if self.seed_top_tiles_zoom_start is not None else 'None',
            seed_top_tiles_zoom_until=self.seed_top_tiles_zoom_until if self.seed_top_tiles_zoom_until is not None else 'None',
            toi_store_type=self.toi_store_type if self.toi_store_type is not None else 'None',
            toi_store_s3_bucket=self.toi_store_s3_bucket if hasattr(self, 'property') and self.toi_store_s3_bucket is not None else 'None',
            toi_store_s3_key=self.toi_store_s3_key if hasattr(self, 'toi_store_s3_key') and self.toi_store_s3_key is not None else 'None',
            toi_store_file_name=self.toi_store_file_name if hasattr(self, 'toi_store_file_name') and self.toi_store_file_name is not None else 'None',
            seed_custom_zoom_start=self.seed_custom_zoom_start if hasattr(self, 'seed_custom_zoom_start') and self.seed_custom_zoom_start is not None else 'None',
            seed_should_add_to_tiles_of_interest=self.seed_should_add_to_tiles_of_interest if hasattr(self, 'seed_should_add_to_tiles_of_interest') and self.seed_should_add_to_tiles_of_interest is not None else 'None',
            seed_custom_zoom_until=self.seed_custom_zoom_until if hasattr(self, 'seed_custom_zoom_until') and self.seed_custom_zoom_until is not None else 'None',
            seed_unique=self.seed_unique if hasattr(self, 'seed_unique') and self.seed_unique is not None else 'None',
            intersect_expired_tiles_location=self.intersect_expired_tiles_location if hasattr(self, 'intersect_expired_tiles_location') and self.intersect_expired_tiles_location is not None else 'None',
            intersect_zoom_until=self.intersect_zoom_until if hasattr(self, 'intersect_zoom_until') and self.intersect_zoom_until is not None else 'None',
            logconfig=self.logconfig if hasattr(self, 'logconfig') and self.logconfig is not None else 'None',
            redis_type=self.redis_type if hasattr(self, 'redis_type') and self.redis_type is not None else 'None',
            redis_host=self.redis_host if hasattr(self, 'redis_host') and self.redis_host is not None else 'None',
            redis_port=self.redis_port if hasattr(self, 'redis_port') and self.redis_port is not None else 'None',
            redis_db=self.redis_db if hasattr(self, 'redis_db') and self.redis_db is not None else 'None',
            redis_cache_set_key=self.redis_cache_set_key if hasattr(self, 'redis_cache_set_key') and self.redis_cache_set_key is not None else 'None',
            statsd_host=self.statsd_host if hasattr(self, 'statsd_host') and self.statsd_host is not None else 'None',
            statsd_port=self.statsd_port if hasattr(self, 'statsd_port') and self.statsd_port is not None else 'None',
            statsd_prefix=self.statsd_prefix if hasattr(self, 'statsd_prefix') and self.statsd_prefix is not None else 'None',
            n_simultaneous_query_sets=self.n_simultaneous_query_sets if hasattr(self, 'n_simultaneous_query_sets') and self.n_simultaneous_query_sets is not None else 'None',
            n_simultaneous_s3_storage=self.n_simultaneous_s3_storage if hasattr(self, 'n_simultaneous_s3_storage') and self.n_simultaneous_s3_storage is not None else 'None',
            log_queue_sizes=self.log_queue_sizes if hasattr(self, 'log_queue_sizes') and self.log_queue_sizes is not None else 'None',
            log_queue_sizes_interval_seconds=self.log_queue_sizes_interval_seconds if hasattr(self, 'log_queue_sizes_interval_seconds') and self.log_queue_sizes_interval_seconds is not None else 'None',
            query_cfg=self.query_cfg if hasattr(self, 'query_cfg') and self.query_cfg is not None else 'None',
            template_path=self.template_path if hasattr(self, 'template_path') and self.template_path is not None else 'None',
            reload_templates=self.reload_templates if hasattr(self, 'reload_templates') and self.reload_templates is not None else 'None',
            output_formats=self.output_formats if hasattr(self, 'output_formats') and self.output_formats is not None else 'None',
            buffer_cfg=self.buffer_cfg if hasattr(self, 'buffer_cfg') and self.buffer_cfg is not None else 'None',
            process_yaml_cfg=self.process_yaml_cfg if hasattr(self, 'process_yaml_cfg') and self.process_yaml_cfg is not None else 'None',
            postgresql_conn_info=self.postgresql_conn_info if hasattr(self, 'postgresql_conn_info') and self.postgresql_conn_info is not None else 'None',
            metatile_size=self.metatile_size if hasattr(self, 'metatile_size') and self.metatile_size is not None else 'None',
            metatile_zoom=self.metatile_zoom if hasattr(self, 'metatile_zoom') and self.metatile_zoom is not None else 'None',
            metatile_start_zoom=self.metatile_start_zoom if hasattr(self, 'metatile_start_zoom') and self.metatile_start_zoom is not None else 'None',
            max_zoom_with_changes=self.max_zoom_with_changes if hasattr(self, 'max_zoom_with_changes') and self.max_zoom_with_changes is not None else 'None',
            max_zoom=self.max_zoom if hasattr(self, 'max_zoom') and self.max_zoom is not None else 'None',
            sql_queue_buffer_size=self.sql_queue_buffer_size if hasattr(self, 'sql_queue_buffer_size') and self.sql_queue_buffer_size is not None else 'None',
            proc_queue_buffer_size=self.proc_queue_buffer_size if hasattr(self, 'proc_queue_buffer_size') and self.proc_queue_buffer_size is not None else 'None',
            s3_queue_buffer_size=self.s3_queue_buffer_size if hasattr(self, 's3_queue_buffer_size') and self.s3_queue_buffer_size is not None else 'None',
            tile_traffic_log_path=self.tile_traffic_log_path if hasattr(self, 'tile_traffic_log_path') and self.tile_traffic_log_path is not None else 'None',
            group_by_zoom=self.group_by_zoom if hasattr(self, 'group_by_zoom') and self.group_by_zoom is not None else 'None',
            tile_sizes=self.tile_sizes if hasattr(self, 'tile_sizes') and 'self.tile_sizes' is not None else 'None')



    def _cfg(self, yamlkeys_str):
        yamlkeys = yamlkeys_str.split()
        yamlval = self.yml
        for subkey in yamlkeys:
            yamlval = yamlval[subkey]
        return yamlval

    def subtree(self, yamlkeys_str):
        yamlkeys = yamlkeys_str.split()
        yamlval = self.yml
        for subkey in yamlkeys:
            yamlval = yamlval.get(subkey)
            if yamlval is None:
                break
        return yamlval


def default_yml_config():
    return {
        'queue': {
            'name': None,
            'type': 'sqs',
            'timeout-seconds': 20
        },
        'store': {
            'type': 's3',
            'name': None,
            'path': 'osm',
            'reduced-redundancy': False,
            'date-prefix': '',
            'delete-retry-interval': 60,
        },
        'aws': {
            'credentials': {
                'aws_access_key_id': None,
                'aws_secret_access_key': None,
            }
        },
        'tiles': {
            'seed': {
                'all': {
                    'zoom-start': None,
                    'zoom-until': None,
                },
                'metro-extract': {
                    'url': None,
                    'zoom-start': None,
                    'zoom-until': None,
                    'cities': None
                },
                'top-tiles': {
                    'url': None,
                    'zoom-start': None,
                    'zoom-until': None,
                },
                'custom': {
                    'zoom-start': None,
                    'zoom-until': None,
                    'bboxes': []
                },
                'should-add-to-tiles-of-interest': True,
                'n-threads': 50,
                'unique': True,
            },
            'intersect': {
                'expired-location': None,
                'parent-zoom-until': None,
            },
            'max-zoom-with-changes': 16,
        },
        'toi-store': {
            'type': None,
        },
        'toi-prune': {
            'tile-traffic-log-path': '/tmp/tile-traffic.log',
        },
        'process': {
            'n-simultaneous-query-sets': 0,
            'n-simultaneous-s3-storage': 0,
            'log-queue-sizes': True,
            'log-queue-sizes-interval-seconds': 10,
            'query-config': None,
            'template-path': None,
            'reload-templates': False,
            'formats': ['json'],
            'buffer': {},
            'yaml': {
                'type': None,
                'parse': {
                    'path': '',
                },
                'callable': {
                    'dotted-name': '',
                },
            },
        },
        'logging': {
            'config': None
        },
        'redis': {
            'host': 'localhost',
            'port': 6379,
            'db': 0,
            'cache-set-key': 'tilequeue.tiles-of-interest',
            'type': 'redis_client',
        },
        'postgresql': {
            'host': 'localhost',
            'port': 5432,
            'dbnames': ('osm',),
            'user': 'osm',
            'password': None,
        },
        'metatile': {
            'size': None,
            'start-zoom': 0,
            'tile-sizes': None,
        },
        'queue_buffer_size': {
            'sql': None,
            'proc': None,
            's3': None,
        },
    }


def merge_cfg(dest, source):
    for k, v in source.items():
        if isinstance(v, dict):
            subdest = dest.setdefault(k, {})
            merge_cfg(subdest, v)
        else:
            dest[k] = v
    return dest


def _override_cfg(container, yamlkeys, value):
    """
    Override a hierarchical key in the config, setting it to the value.

    Note that yamlkeys should be a non-empty list of strings.
    """

    key = yamlkeys[0]
    rest = yamlkeys[1:]

    if len(rest) == 0:
        # no rest means we found the key to update.
        container[key] = value

    elif key in container:
        # still need to find the leaf in the tree, so recurse.
        _override_cfg(container[key], rest, value)

    else:
        # need to create a sub-tree down to the leaf to insert into.
        subtree = {}
        _override_cfg(subtree, rest, value)
        container[key] = subtree


def _make_yaml_key(s):
    """
    Turn an environment variable into a yaml key

    Keys in YAML files are generally lower case and use dashes instead of
    underscores. This isn't a universal rule, though, so we'll have to
    either change the keys to conform to this, or have some way of indicating
    this from the environment.
    """

    return s.lower().replace("_", "-")


def make_config_from_argparse(config_file_handle, default_yml=None,
                              postgresql_hosts=None,
                              postgresql_dbnames=None,
                              postgresql_user=None,
                              postgresql_password=None,
                              store_name=None,
                              store_date_prefix=None,
                              batch_check_metafile_exists=None,
                              ):
    """ Generate config from various sources. The configurations chain
        includes these in order:
        1. a hardcoded default_yml_config
        2. a passed-in config file
        3. environment variables with prefix `TILEQUEUE__`
        4. explicit override arguments such as postgresql_hosts

        the configuration values at the end of the chain override the values
        of those at the beginning of the chain
    """
    if default_yml is None:
        default_yml = default_yml_config()

    # override defaults from config file
    yml_data = load(config_file_handle)
    cfg = merge_cfg(default_yml, yml_data)

    # override config file with values from the environment
    for k in os.environ:
        # keys in the environment have the form TILEQUEUE__FOO__BAR (note the
        # _double_ underscores), which will decode the value as YAML and insert
        # it in cfg['foo']['bar'].
        #
        # TODO: should the prefix TILEQUEUE be configurable?
        if k.startswith('TILEQUEUE__'):
            keys = map(_make_yaml_key, k.split('__')[1:])
            value = load(os.environ[k])
            _override_cfg(cfg, keys, value)

    # override config values with explicit arguments if set
    if postgresql_hosts is not None:
        keys = ['postgresql', 'host']  # attention non-plural form `host`
        value = load(postgresql_hosts)
        _override_cfg(cfg, keys, value)

    if postgresql_dbnames is not None:
        keys = ['postgresql', 'dbnames']
        value = load(postgresql_dbnames)
        _override_cfg(cfg, keys, value)

    if postgresql_user is not None:
        keys = ['postgresql', 'user']
        value = load(postgresql_user)
        _override_cfg(cfg, keys, value)

    if postgresql_password is not None:
        keys = ['postgresql', 'password']
        value = load(postgresql_password)
        _override_cfg(cfg, keys, value)

    if store_name is not None:
        keys = ['store', 'name']
        value = load(store_name)
        _override_cfg(cfg, keys, value)

    if store_date_prefix is not None:
        keys = ['store', 'date-prefix']
        value = load(store_date_prefix)
        _override_cfg(cfg, keys, value)

    if batch_check_metafile_exists is not None:
        keys = ['batch', 'check-metatile-exists']
        value = load(batch_check_metafile_exists)
        _override_cfg(cfg, keys, value)

    return Configuration(cfg)


def _bounds_pad_no_buf(bounds, meters_per_pixel_dim):
    return dict(
        point=bounds,
        line=bounds,
        polygon=bounds,
    )


def create_query_bounds_pad_fn(buffer_cfg, layer_name):

    if not buffer_cfg:
        return _bounds_pad_no_buf

    buf_by_type = dict(
        point=0,
        line=0,
        polygon=0,
    )

    for format_ext, format_cfg in buffer_cfg.items():
        format_layer_cfg = format_cfg.get('layer', {}).get(layer_name)
        format_geometry_cfg = format_cfg.get('geometry', {})
        if format_layer_cfg:
            for geometry_type, buffer_size in format_layer_cfg.items():
                buf_by_type[geometry_type] = max(
                    buf_by_type[geometry_type], buffer_size)
        if format_geometry_cfg:
            for geometry_type, buffer_size in format_geometry_cfg.items():
                buf_by_type[geometry_type] = max(
                    buf_by_type[geometry_type], buffer_size)

    if (buf_by_type['point'] ==
            buf_by_type['line'] ==
            buf_by_type['polygon'] == 0):
        return _bounds_pad_no_buf

    def bounds_pad(bounds, meters_per_pixel_dim):
        buffered_by_type = {}
        for geometry_type in ('point', 'line', 'polygon'):
            offset = meters_per_pixel_dim * buf_by_type[geometry_type]
            buffered_by_type[geometry_type] = bounds_buffer(bounds, offset)
        return buffered_by_type

    return bounds_pad
