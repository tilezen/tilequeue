from tilequeue.query.fixture import make_fixture_data_fetcher
from tilequeue.query.pool import DBConnectionPool
from tilequeue.query.postgres import make_db_data_fetcher
from tilequeue.query.rawr import make_rawr_data_fetcher
from tilequeue.query.split import make_split_data_fetcher
from tilequeue.process import Source
from tilequeue.store import make_s3_tile_key_generator


__all__ = [
    'DBConnectionPool',
    'make_db_data_fetcher',
    'make_fixture_data_fetcher',
    'make_data_fetcher',
]


def make_data_fetcher(cfg, layer_data, query_cfg, io_pool,
                      s3_role_arn=None,
                      s3_role_session_duration_s=None):
    """ Make data fetcher from RAWR store and PostgreSQL database.
        When s3_role_arn and s3_role_session_duration_s are available
        the RAWR store will use the s3_role_arn to access the RAWR S3 bucket
    """
    db_fetcher = make_db_data_fetcher(
        cfg.postgresql_conn_info, cfg.template_path, cfg.reload_templates,
        query_cfg, io_pool)

    if cfg.yml.get('use-rawr-tiles'):
        rawr_fetcher = _make_rawr_fetcher(
            cfg, layer_data, s3_role_arn, s3_role_session_duration_s)

        group_by_zoom = cfg.yml.get('rawr').get('group-zoom')
        assert group_by_zoom is not None, 'Missing group-zoom rawr config'
        return make_split_data_fetcher(
            group_by_zoom, db_fetcher, rawr_fetcher)

    else:
        return db_fetcher


class _NullRawrStorage(object):

    def __init__(self, data_source, table_sources):
        self.data_source = data_source
        self.table_sources = table_sources

    def __call__(self, tile):
        # returns a "tables" object, which responds to __call__(table_name)
        # with tuples for that table.
        data = {}
        for location in self.data_source(tile):
            data[location.name] = location.records

        def _tables(table_name):
            from tilequeue.query.common import Table
            source = self.table_sources[table_name]
            return Table(source, data.get(table_name, []))

        return _tables


def _make_rawr_fetcher(cfg, layer_data,
                       s3_role_arn=None,
                       s3_role_session_duration_s=None):
    """
        When s3_role_arn and s3_role_session_duration_s are available
        the RAWR store will use the s3_role_arn to access the RAWR S3
        bucket
    """
    rawr_yaml = cfg.yml.get('rawr')
    assert rawr_yaml is not None, 'Missing rawr configuration in yaml'

    group_by_zoom = rawr_yaml.get('group-zoom')
    assert group_by_zoom is not None, 'Missing group-zoom rawr config'

    rawr_source_yaml = rawr_yaml.get('source')
    assert rawr_source_yaml, 'Missing rawr source config'

    table_sources = rawr_source_yaml.get('table-sources')
    assert table_sources, 'Missing definitions of source per table'

    # map text for table source onto Source objects
    for tbl, data in table_sources.items():
        source_name = data['name']
        source_value = data['value']
        table_sources[tbl] = Source(source_name, source_value)

    label_placement_layers = rawr_yaml.get('label-placement-layers', {})
    for geom_type, layers in label_placement_layers.items():
        assert geom_type in ('point', 'polygon', 'linestring'), \
            'Geom type %r not understood, expecting point, polygon or ' \
            'linestring.' % (geom_type,)
        label_placement_layers[geom_type] = set(layers)

    indexes_cfg = rawr_yaml.get('indexes')
    assert indexes_cfg, 'Missing definitions of table indexes.'

    # source types are:
    #   s3       - to fetch RAWR tiles from S3
    #   store    - to fetch RAWR tiles from any tilequeue tile source
    #   generate - to generate RAWR tiles directly, rather than trying to load
    #              them from S3. this can be useful for standalone use and
    #              testing. provide a postgresql subkey for database connection
    #              settings.
    source_type = rawr_source_yaml.get('type')

    if source_type == 's3':
        rawr_source_s3_yaml = rawr_source_yaml.get('s3')
        bucket = rawr_source_s3_yaml.get('bucket')
        assert bucket, 'Missing rawr source s3 bucket'
        region = rawr_source_s3_yaml.get('region')
        assert region, 'Missing rawr source s3 region'
        prefix = rawr_source_s3_yaml.get('prefix')
        assert prefix, 'Missing rawr source s3 prefix'
        extension = rawr_source_s3_yaml.get('extension')
        assert extension, 'Missing rawr source s3 extension'
        allow_missing_tiles = rawr_source_s3_yaml.get(
            'allow-missing-tiles', False)

        import boto3
        import botocore
        from tilequeue.rawr import RawrS3Source
        if s3_role_arn:
            # use provided role to access S3
            assert s3_role_session_duration_s, \
                's3_role_session_duration_s is either None or 0'
            session = botocore.session.get_session()
            client = session.create_client('sts')
            assume_role_object = \
                client.assume_role(RoleArn=s3_role_arn,
                                   RoleSessionName='tilequeue_dataaccess',
                                   DurationSeconds=s3_role_session_duration_s)
            creds = assume_role_object['Credentials']
            s3_client = boto3.client('s3',
                                     region_name=region,
                                     aws_access_key_id=creds['AccessKeyId'],
                                     aws_secret_access_key=creds[
                                         'SecretAccessKey'],
                                     aws_session_token=creds['SessionToken'])
        else:
            s3_client = boto3.client('s3', region_name=region)

        tile_key_gen = make_s3_tile_key_generator(rawr_source_s3_yaml)
        storage = RawrS3Source(
            s3_client, bucket, prefix, extension, table_sources, tile_key_gen,
            allow_missing_tiles)

    elif source_type == 'generate':
        from raw_tiles.source.conn import ConnectionContextManager
        from raw_tiles.source.osm import OsmSource

        postgresql_cfg = rawr_source_yaml.get('postgresql')
        assert postgresql_cfg, 'Missing rawr postgresql config'

        conn_ctx = ConnectionContextManager(postgresql_cfg)
        rawr_osm_source = OsmSource(conn_ctx)
        storage = _NullRawrStorage(rawr_osm_source, table_sources)

    elif source_type == 'store':
        from tilequeue.store import make_store
        from tilequeue.rawr import RawrStoreSource

        store_cfg = rawr_source_yaml.get('store')
        store = make_store(store_cfg)
        storage = RawrStoreSource(store, table_sources)

    else:
        assert False, 'Source type %r not understood. ' \
            'Options are s3, generate and store.' % (source_type,)

    # TODO: this needs to be configurable, everywhere! this is a long term
    # refactor - it's hard-coded in a bunch of places :-(
    max_z = 16

    layers = _make_layer_info(layer_data, cfg.process_yaml_cfg)

    return make_rawr_data_fetcher(
        group_by_zoom, max_z, storage, layers, indexes_cfg,
        label_placement_layers)


def _make_layer_info(layer_data, process_yaml_cfg):
    from tilequeue.query.common import LayerInfo, ShapeType

    layers = {}
    functions = _parse_yaml_functions(process_yaml_cfg)

    for layer_datum in layer_data:
        name = layer_datum['name']
        min_zoom_fn, props_fn = functions[name]
        shape_types = ShapeType.parse_set(layer_datum['geometry_types'])
        layer_info = LayerInfo(min_zoom_fn, props_fn, shape_types)
        layers[name] = layer_info

    return layers


def _parse_yaml_functions(process_yaml_cfg):
    from tilequeue.command import make_output_calc_mapping
    from tilequeue.command import make_min_zoom_calc_mapping

    output_layer_data = make_output_calc_mapping(process_yaml_cfg)
    min_zoom_layer_data = make_min_zoom_calc_mapping(process_yaml_cfg)

    keys = set(output_layer_data.keys())
    assert keys == set(min_zoom_layer_data.keys())

    functions = {}
    for key in keys:
        min_zoom_fn = min_zoom_layer_data[key]
        output_fn = output_layer_data[key]
        functions[key] = (min_zoom_fn, output_fn)

    return functions
