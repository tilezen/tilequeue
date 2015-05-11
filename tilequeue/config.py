from yaml import load


class Configuration(object):
    """
    Flatten configuration from yaml
    """

    def __init__(self, yml):
        self.yml = yml

        self.aws_access_key_id = self._cfg('aws credentials aws_access_key_id')
        self.aws_secret_access_key = self._cfg('aws credentials '
                                               'aws_secret_access_key')
        self.queue_name = self._cfg('aws sqs name')
        self.queue_type = 'sqs'
        self.s3_bucket = self._cfg('aws s3 bucket')
        self.s3_reduced_redundancy = self._cfg('aws s3 reduced-redundancy')
        self.s3_path = self._cfg('aws s3 path')
        self.tilestache_config = self._cfg('tilestache config')
        self.expired_tiles_location = self.yml['tiles']['expired-location']
        self.output_formats = self._cfg('tilestache formats')

        seed_cfg = self.yml['tiles']['seed']
        self.seed_all_zoom_start = seed_cfg['all']['zoom-start']
        self.seed_all_zoom_until = seed_cfg['all']['zoom-until']

        seed_metro_cfg = seed_cfg['metro-extract']
        self.seed_metro_extract_url = seed_metro_cfg['url']
        self.seed_metro_extract_zoom_start = seed_metro_cfg['zoom-start']
        self.seed_metro_extract_zoom_until = seed_metro_cfg['zoom-until']

        seed_top_tiles_cfg = seed_cfg['top-tiles']
        self.seed_top_tiles_url = seed_top_tiles_cfg['url']
        self.seed_top_tiles_zoom_start = seed_top_tiles_cfg['zoom-start']
        self.seed_top_tiles_zoom_until = seed_top_tiles_cfg['zoom-until']

        self.logconfig = self._cfg('logging config')
        self.redis_host = self._cfg('redis host')
        self.redis_port = self._cfg('redis port')
        self.redis_db = self._cfg('redis db')
        self.redis_cache_set_key = self._cfg('redis cache-set-key')
        self.explode_until = self._cfg('tiles explode-until')
        self.n_simultaneous_query_sets = \
            self.yml['process']['n-simultaneous-query-sets']
        self.log_queue_sizes = self.yml['process']['log-queue-sizes']
        self.log_queue_sizes_interval_seconds = \
            self.yml['process']['log-queue-sizes-interval-seconds']
        self.postgresql_conn_info = self.yml['postgresql']
        dbnames = self.postgresql_conn_info.get('dbnames')
        assert dbnames is not None, 'Missing postgresql dbnames'
        assert isinstance(dbnames, (tuple, list)), \
            "Expecting postgresql 'dbnames' to be a list"
        assert len(dbnames) > 0, 'No postgresql dbnames configured'

    def _cfg(self, yamlkeys_str):
        yamlkeys = yamlkeys_str.split()
        yamlval = self.yml
        for subkey in yamlkeys:
            yamlval = yamlval[subkey]
        return yamlval


def default_yml_config():
    return {
        'aws': {
            'sqs': {
                'name': None,
                'timeout-seconds': 20,
            },
            's3': {
                'bucket': None,
                'path': 'osm',
                'reduced-redundancy': False,
            },
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
                },
                'top-tiles': {
                    'url': None,
                    'zoom-start': None,
                    'zoom-until': None,
                },
            },
            'explode-until': 0,
            'expired-location': None,
        },
        'process': {
            'n-simultaneous-query-sets': 0,
            'log-queue-sizes': True,
            'log-queue-sizes-interval-seconds': 1,
        },
        'tilestache': {
            'config': None,
            'formats': ('json', 'vtm'),
        },
        'logging': {
            'config': None
        },
        'redis': {
            'host': 'localhost',
            'port': 6379,
            'db': 0,
            'cache-set-key': 'tilequeue.tiles-of-interest',
        },
        'postgresql': {
            'host': 'localhost',
            'port': 5432,
            'dbnames': ('osm',),
            'user': 'osm',
            'password': None,
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
