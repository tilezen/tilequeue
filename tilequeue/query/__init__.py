from tilequeue.query.fixture import make_fixture_data_fetcher
from tilequeue.query.pool import DBConnectionPool
from tilequeue.query.postgres import make_db_data_fetcher
from tilequeue.query.rawr import make_rawr_data_fetcher
from tilequeue.query.split import make_split_data_fetcher


__all__ = [
    'DBConnectionPool',
    'make_db_data_fetcher',
    'make_fixture_data_fetcher',
    'make_data_fetcher',
]


def make_data_fetcher(cfg, layer_data, query_cfg, io_pool):
    db_fetcher = make_db_data_fetcher(
        cfg.postgresql_conn_info, cfg.template_path, cfg.reload_templates,
        query_cfg, io_pool)

    if cfg.yml.get('use-rawr-tiles'):
        rawr_fetcher = _make_rawr_fetcher(
            cfg, layer_data, query_cfg, io_pool)

        group_by_zoom = cfg.yml.get('rawr').get('group-zoom')
        assert group_by_zoom is not None, 'Missing group-zoom rawr config'
        return make_split_data_fetcher(
            group_by_zoom, db_fetcher, rawr_fetcher)

    else:
        return db_fetcher


class _NullRawrStorage(object):

    def __init__(self, source):
        self.source = source

    def __call__(self, tile):
        # returns a "tables" object, which responds to __call__(table_name)
        # with tuples for that table.
        data = {}
        for location in self.source(tile):
            data[location.name] = location.records

        def _tables(table_name):
            return data.get(table_name, [])

        return _tables


def _make_rawr_fetcher(cfg, layer_data, query_cfg, io_pool):
    rawr_yaml = cfg.yml.get('rawr')
    assert rawr_yaml is not None, 'Missing rawr configuration in yaml'

    group_by_zoom = rawr_yaml.get('group-zoom')
    assert group_by_zoom is not None, 'Missing group-zoom rawr config'

    rawr_source_yaml = rawr_yaml.get('source')
    assert rawr_source_yaml, 'Missing rawr source config'

    # set this flag, and provide a postgresql subkey, to generate RAWR tiles
    # directly, rather than trying to load them from S3. this can be useful
    # for standalone use and testing.
    is_s3_storage = not rawr_source_yaml.get('generate-from-scratch')

    if is_s3_storage:
        bucket = rawr_source_yaml.get('bucket')
        assert bucket, 'Missing rawr sink bucket'
        prefix = rawr_source_yaml.get('prefix')
        assert prefix, 'Missing rawr sink prefix'
        suffix = rawr_source_yaml.get('suffix')
        assert suffix, 'Missing rawr sink suffix'

        import boto3
        from tilequeue.rawr import RawrS3Source
        s3_client = boto3.client('s3')
        storage = RawrS3Source(s3_client, bucket, prefix, suffix, io_pool)

    else:
        from raw_tiles.source.conn import ConnectionContextManager
        from raw_tiles.source.osm import OsmSource

        postgresql_cfg = rawr_source_yaml.get('postgresql')
        assert postgresql_cfg, 'Missing rawr postgresql config'

        conn_ctx = ConnectionContextManager(postgresql_cfg)
        rawr_osm_source = OsmSource(conn_ctx)
        storage = _NullRawrStorage(rawr_osm_source)

    # TODO: this needs to be configurable, everywhere!
    max_z = 16

    # TODO: this is just wrong - need to refactor this per-"table"?
    source = 'osm'

    # TODO: put this in the config!
    label_placement_layers = {
        'point': set(['earth', 'water']),
        'polygon': set(['buildings', 'earth', 'landuse', 'water']),
        'linestring': set(['earth', 'landuse', 'water']),
    }

    layers = _make_layer_info(layer_data, cfg.process_yaml_cfg)

    return make_rawr_data_fetcher(
        group_by_zoom, max_z, storage, layers, source, label_placement_layers)


def _make_layer_info(layer_data, process_yaml_cfg):
    from tilequeue.query.common import LayerInfo

    layers = {}
    functions = _parse_yaml_functions(process_yaml_cfg)

    for layer_datum in layer_data:
        name = layer_datum['name']
        min_zoom_fn, props_fn = functions[name]
        shape_types = _parse_shape_types(layer_datum['geometry_types'])
        layer_info = LayerInfo(min_zoom_fn, props_fn, shape_types)
        layers[name] = layer_info

    return layers


def _parse_shape_types(inputs):
    from tilequeue.query.common import shape_type_lookup

    outputs = set()
    for value in inputs:
        outputs.add(shape_type_lookup(value))

    if outputs:
        return outputs
    else:
        return None


def _parse_yaml_functions(process_yaml_cfg):
    from vectordatasource.meta.python import make_function_name_props
    from vectordatasource.meta.python import make_function_name_min_zoom
    from vectordatasource.meta.python import output_kind
    from vectordatasource.meta.python import output_min_zoom
    from vectordatasource.meta.python import parse_layers
    import os.path

    # can't handle "callable" type - how do we get the min zoom fn?
    assert process_yaml_cfg['type'] == 'parse'

    parse_cfg = process_yaml_cfg['parse']
    yaml_path = parse_cfg['path']

    assert os.path.isdir(yaml_path)

    output_layer_data = parse_layers(
        yaml_path, output_kind, make_function_name_props)
    min_zoom_layer_data = parse_layers(
        yaml_path, output_min_zoom, make_function_name_min_zoom)

    keys = set(output_layer_data.keys())
    assert keys == set(min_zoom_layer_data.keys())

    functions = {}
    for key in keys:
        min_zoom_fn = min_zoom_layer_data[key]
        output_fn = output_layer_data[key]
        functions[key] = (min_zoom_fn, output_fn)

    return functions
