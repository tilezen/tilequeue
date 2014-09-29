class OutputFormat(object):

    def __init__(self, name, extension, mimetype):
        self.name = name
        self.extension = extension
        self.mimetype = mimetype

    def __repr__(self):
        return 'OutputFormat(%s, %s, %s)' % \
            (self.name, self.extension, self.mimetype)

json_format = OutputFormat('JSON', 'json', 'application/json')
topojson_format = OutputFormat('TopoJSON', 'topojson', 'application/json')
# TODO image/png mimetype? app doesn't work unless image/png?
vtm_format = OutputFormat('OpenScienceMap', 'vtm', 'image/png')
mapbox_format = OutputFormat('Mapbox', 'mapbox', 'image/png')

extension_to_format = dict(
    json=json_format,
    topojson=topojson_format,
    vtm=vtm_format,
    mapbox=mapbox_format,
)

name_to_format = {
    'JSON': 'json_format',
    'OpenScienceMap': vtm_format,
    'TopoJSON': topojson_format,
    'Mapbox': mapbox_format,
}


def lookup_format_by_extension(extension):
    return extension_to_format.get(extension)


def lookup_format_by_name(name):
    return name_to_format.get(name)
