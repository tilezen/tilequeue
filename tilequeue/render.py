from cStringIO import StringIO
from TileStache.Goodies.VecTiles.server import MultiResponse


class RenderJob(object):

    # This isn't ideal, because it will execute the query for each format
    # But, it's least invasive with most code re-use

    def __init__(self, coord, format, tilestache_config, layer_names):
        self.coord = coord
        self.format = format
        self.tilestache_config = tilestache_config
        self.layer_names = layer_names

    def __call__(self):
        out = StringIO()
        response = MultiResponse(
            self.tilestache_config, self.layer_names, self.coord)
        response.save(out, self.format.name)
        return out.getvalue()


class RenderJobCreator(object):

    def __init__(self, tilestache_config, formats):
        self.tilestache_config = tilestache_config
        self.formats = formats
        layers = tilestache_config.layers
        all_layer = layers['all']
        self.layer_names = all_layer.provider.names

    def create(self, coord):
        return [RenderJob(coord, format,
                          self.tilestache_config, self.layer_names)
                for format in self.formats]
