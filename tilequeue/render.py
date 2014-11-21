from TileStache.Goodies.VecTiles.server import MultiResponse


class RenderJob(object):

    # This isn't ideal, because it will execute the query for each format
    # But, it's least invasive with most code re-use

    def __init__(self, coord, format, tilestache_config, layer_names):
        self.coord = coord
        self.format = format
        self.tilestache_config = tilestache_config
        self.layer_names = layer_names

    def __call__(self, out):
        response = MultiResponse(
            self.tilestache_config, self.layer_names, self.coord)
        response.save(out, self.format.name)

    def __repr__(self):
        return 'RenderJob(%s, %s)' % (self.coord, self.format)


class RenderJobCreator(object):

    def __init__(self, tilestache_config, formats, store):
        self.tilestache_config = tilestache_config
        self.formats = formats
        self.store = store
        layers = tilestache_config.layers
        all_layer = layers['all']
        self.layer_names = all_layer.provider.names

    def create(self, coord):
        return [RenderJob(coord, format,
                          self.tilestache_config, self.layer_names)
                for format in self.formats]

    def process_jobs_for_coord(self, coods):
        jobs = job_creator.create(coord)
        for job in jobs:
            with closing(self.store.output_fp(coord, job.format)) as store_fp:
                 job(store_fp)
