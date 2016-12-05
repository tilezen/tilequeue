from tilequeue.tile import bounds_buffer
from yaml import load


class Configuration(object):
    '''
    Flatten configuration from yaml
    '''

    def __init__(self, yml):
        self.yml = yml

        self.aws_access_key_id = self._cfg('aws credentials aws_access_key_id')
        self.aws_secret_access_key = self._cfg('aws credentials '
                                               'aws_secret_access_key')

        self.queue_name = self._cfg('queue name')
        self.queue_type = self._cfg('queue type')
        self.queue_cfg = self.yml['queue']

        self.store_type = self._cfg('store type')
        self.s3_bucket = self._cfg('store name')
        self.s3_reduced_redundancy = self._cfg('store reduced-redundancy')
        self.s3_path = self._cfg('store path')
        self.s3_date_prefix = self._cfg('store date-prefix')

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

        layers_to_format_cfg = process_cfg.get('layers-to-format')
        layers_to_format = []
        from tilequeue.command import lookup_formats
        for layer_to_format_cfg in layers_to_format_cfg:
            layer_name = layer_to_format_cfg['layer']
            format_names = layer_to_format_cfg['formats']
            formats = lookup_formats(format_names)
            zoom_start = int(layer_to_format_cfg.get('zoom-start', 0))
            zoom_until = int(layer_to_format_cfg.get('zoom-until', 20))
            layer_to_format = layer_name, formats, zoom_start, zoom_until
            layers_to_format.append(layer_to_format)
        self.layers_to_format = layers_to_format

        self.postgresql_conn_info = self.yml['postgresql']
        dbnames = self.postgresql_conn_info.get('dbnames')
        assert dbnames is not None, 'Missing postgresql dbnames'
        assert isinstance(dbnames, (tuple, list)), \
            "Expecting postgresql 'dbnames' to be a list"
        assert len(dbnames) > 0, 'No postgresql dbnames configured'

        self.wof = self.yml.get('wof')

        self.metatile_size = self._cfg('metatile size')
        self.store_orig = self._cfg('metatile store_metatile_and_originals')

    def _cfg(self, yamlkeys_str):
        yamlkeys = yamlkeys_str.split()
        yamlval = self.yml
        for subkey in yamlkeys:
            yamlval = yamlval[subkey]
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
            },
            'intersect': {
                'expired-location': None,
                'parent-zoom-until': None,
            },
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
            'layers-to-format': [],
            'buffer': {},
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
            'store_metatile_and_originals': False,
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


def make_config_from_argparse(config_path, opencfg=open):
    # opencfg for testing
    cfg = default_yml_config()
    with opencfg(config_path) as config_fp:
        yml_data = load(config_fp.read())
        cfg = merge_cfg(cfg, yml_data)
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
