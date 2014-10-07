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
        self.read_timeout = self._cfg('sqs_read_timeout',
                                      'aws sqs timeout-seconds',
                                      0)
        self.s3_bucket = self._cfg('s3_bucket', 'aws s3 bucket')
        self.s3_reduced_redundancy = self._cfg('s3_reduced_redundancy',
                                               'aws s3 reduced-redundancy',
                                               False)
        self.s3_path = self._cfg('s3_path', 'aws s3 path', '')
        self.tilestache_config = self._cfg('tilestache_config',
                                           'tilestache config')
        self.expired_tiles_file = self._cfg('expired_tiles_file',
                                            'tiles expired')
        self.output_formats = self._cfg('output_formats',
                                        'tilestache formats',
                                        ('json', 'vtm'))
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
            'unique': False,
            'zoom-start': 0,
            'zoom-until': 0,
            'daemon': False,
            'expired': None,
        },
        'tilestache': {
            'config': None,
            'formats': ('json', 'vtm'),
        },
        'logging': {
            'config': None
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
