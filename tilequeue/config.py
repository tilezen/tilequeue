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
        _override_cfg(container, rest, value)

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


def make_config_from_argparse(config_file_handle, default_yml=None):
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
