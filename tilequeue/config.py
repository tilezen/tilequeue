from tilequeue.postgresql import RoundRobinConnectionFactory
from yaml import load


class CliConfiguration(object):
    """
    Given args from argparse and yaml from configuration, expose methods that
    try the cli args first and delegate to the yaml config. The yaml config is
    expected to have been merged with defaults
    """

    def __init__(self, args, yml):
        self.args = args
        self.yml = yml

        self.aws_access_key_id = self._cfg('aws_access_key_id',
                                           'aws credentials aws_access_key_id')
        self.aws_secret_access_key = self._cfg('aws_secret_access_key',
                                               'aws credentials '
                                               'aws_secret_access_key')
        self.queue_name = self._cfg('queue_name',
                                    'aws sqs name')
        self.queue_type = getattr(self.args, 'queue_type', 'sqs')
        self.s3_bucket = self._cfg('s3_bucket', 'aws s3 bucket')
        self.s3_reduced_redundancy = self._cfg('s3_reduced_redundancy',
                                               'aws s3 reduced-redundancy',
                                               False)
        self.s3_path = self._cfg('s3_path', 'aws s3 path', '')
        self.tilestache_config = self._cfg('tilestache_config',
                                           'tilestache config')
        self.expired_tiles_file = self._cfg('expired_tiles_file',
                                            'tiles expired')
        self.expired_tiles_location = self.yml['tiles']['expired-location']
        self.output_formats = self._cfg('output_formats', 'tilestache formats')
        self.zoom_start = self._cfg('zoom_start', 'tiles zoom-start', 0)
        self.zoom_until = self._cfg('zoom_until', 'tiles zoom-until', 0)
        self.unique_tiles = self._cfg('unique-tiles', 'tiles unique')
        self.filter_metro_zoom = self._cfg('filter_metro_zoom',
                                           'tiles metro-extract zoom-filter',
                                           0)
        self.metro_extract_url = self._cfg('metro_extract_url',
                                           'tiles metro-extract url')
        self.daemon = self._cfg('daemon', 'tiles daemon', False)
        self.tile = getattr(self.args, 'tile', None)
        self.logconfig = self._cfg('logconfig', 'logging config')
        self.redis_host = self._cfg('redis_host', 'redis host')
        self.redis_port = self._cfg('redis_port', 'redis port')
        self.redis_db = self._cfg('redis_db', 'redis db')
        self.redis_cache_set_key = self._cfg('redis_cache_set_key',
                                             'redis cache-set-key')
        self.redis_diff_set_key = self._cfg('redis_diff_set_key',
                                            'redis diff-set-key')
        self.explode_until = self._cfg('explode_until',
                                       'tiles explode-until')
        self.workers = self._cfg('workers', 'workers')
        self.messages_at_once = self._cfg('messages_at_once',
                                          'messages_at_once')
        self.postgresql_conn_info = self.yml['postgresql']
        assert 'host' not in self.postgresql_conn_info, \
            "postgresql 'host' option is unused, use 'hosts'"
        assert 'connection_factory' not in self.postgresql_conn_info, \
            'connection_factory postgresql option will be overridden'
        hosts = self.postgresql_conn_info.pop('hosts')
        assert isinstance(hosts, (tuple, list)), \
            "Expecting postgresql 'hosts' to be a list"
        assert len(hosts) > 0, 'No postgresql hosts configured'
        conn_info = dict(self.postgresql_conn_info)
        conn_factory = RoundRobinConnectionFactory(conn_info, hosts)
        self.postgresql_conn_info['connection_factory'] = conn_factory

        self.top_tiles = self.yml['tiles']['top-tiles']
        self.top_tiles_url = self.top_tiles['url']
        self.top_tiles_zoom_start = self.top_tiles['zoom-start']
        self.top_tiles_zoom_until = self.top_tiles['zoom-until']

    def _cfg(self, argname, yamlkeys_str, default_arg_value=None):
        argval = getattr(self.args, argname, default_arg_value)
        if argval is not default_arg_value:
            return argval
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
            'metro-extract': {
                'url': None,
                'zoom-filter': 0,
            },
            'top-tiles': {
                'url': None,
                'zoom-start': 0,
                'zoom-until': 0
            },
            'unique': False,
            'zoom-start': 0,
            'zoom-until': 0,
            'explode-until': 0,
            'daemon': False,
            'expired': None,
            'expired-location': None,
        },
        'workers': 4,
        'messages_at_once': 4,
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
            'cache-set-key': 'tilestache.cache',
            'diff-set-key': None,
        },
        'postgresql': {
            'hosts': ('localhost',),
            'port': 5432,
            'database': 'osm',
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


def make_config_from_argparse(args, opencfg=open):
    # opencfg for testing
    cfg = default_yml_config()
    if args.config is not None:
        with opencfg(args.config) as config_fp:
            yml_data = load(config_fp.read())
            cfg = merge_cfg(cfg, yml_data)
    return CliConfiguration(args, cfg)
