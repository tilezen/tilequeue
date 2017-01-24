import ujson as json


def update_arc_indexes(geometry, merged_arcs, old_arcs):
    """ Updated geometry arc indexes, and add arcs to merged_arcs along the way.

        Arguments are modified in-place, and nothing is returned.
    """
    if geometry['type'] in ('Point', 'MultiPoint'):
        return

    elif geometry['type'] == 'LineString':
        for arc_index, old_arc in enumerate(geometry['arcs']):
            geometry['arcs'][arc_index] = len(merged_arcs)
            merged_arcs.append(old_arcs[old_arc])

    elif geometry['type'] == 'Polygon':
        for ring in geometry['arcs']:
            for arc_index, old_arc in enumerate(ring):
                ring[arc_index] = len(merged_arcs)
                merged_arcs.append(old_arcs[old_arc])

    elif geometry['type'] == 'MultiLineString':
        for part in geometry['arcs']:
            for arc_index, old_arc in enumerate(part):
                part[arc_index] = len(merged_arcs)
                merged_arcs.append(old_arcs[old_arc])

    elif geometry['type'] == 'MultiPolygon':
        for part in geometry['arcs']:
            for ring in part:
                for arc_index, old_arc in enumerate(ring):
                    ring[arc_index] = len(merged_arcs)
                    merged_arcs.append(old_arcs[old_arc])

    else:
        raise NotImplementedError("Can't do %s geometries" % geometry['type'])


def get_transform(bounds, size=4096):
    """ Return a TopoJSON transform dictionary and a point-transforming function.

        Size is the tile size in pixels and sets the implicit output
        resolution.
    """
    tx, ty = bounds[0], bounds[1]
    sx, sy = (bounds[2] - bounds[0]) / size, (bounds[3] - bounds[1]) / size

    def forward(lon, lat):
        """ Transform a longitude and latitude to TopoJSON integer space.
        """
        return int(round((lon - tx) / sx)), int(round((lat - ty) / sy))

    return dict(translate=(tx, ty), scale=(sx, sy)), forward


def diff_encode(line, transform):
    """ Differentially encode a shapely linestring or ring.
    """
    coords = [transform(x, y) for (x, y) in line.coords]

    pairs = zip(coords[:], coords[1:])
    diffs = [(x2 - x1, y2 - y1) for ((x1, y1), (x2, y2)) in pairs]

    return coords[:1] + [(x, y) for (x, y) in diffs if (x, y) != (0, 0)]


def encode(file, features_by_layer, bounds):
    """ Encode a dict of layername: (shape, props, id) features into a
        TopoJSON stream.

        If no id is available, pass in None

        Geometries in the features list are assumed to be unprojected
        lon, lats.  Bounds are given in geographic coordinates as
        (xmin, ymin, xmax, ymax).
    """
    transform, forward = get_transform(bounds)
    arcs = []

    geometries_by_layer = {}

    for layer, features in features_by_layer.iteritems():
        geometries = []
        for shape, props, fid in features:
            if shape.type == 'GeometryCollection':
                continue

            geometry = dict(properties=props)

            if fid is not None:
                geometry['id'] = fid

            elif shape.type == 'Point':
                geometry.update(dict(
                    type='Point',
                    coordinates=forward(shape.x, shape.y)))

            elif shape.type == 'LineString':
                geometry.update(dict(type='LineString', arcs=[len(arcs)]))
                arcs.append(diff_encode(shape, forward))

            elif shape.type == 'Polygon':
                geometry.update(dict(type='Polygon', arcs=[]))

                rings = [shape.exterior] + list(shape.interiors)

                for ring in rings:
                    geometry['arcs'].append([len(arcs)])
                    arcs.append(diff_encode(ring, forward))

            elif shape.type == 'MultiPoint':
                geometry.update(dict(type='MultiPoint', coordinates=[]))

                for point in shape.geoms:
                    geometry['coordinates'].append(forward(point.x, point.y))

            elif shape.type == 'MultiLineString':
                geometry.update(dict(type='MultiLineString', arcs=[]))

                for line in shape.geoms:
                    geometry['arcs'].append([len(arcs)])
                    arcs.append(diff_encode(line, forward))

            elif shape.type == 'MultiPolygon':
                geometry.update(dict(type='MultiPolygon', arcs=[]))

                for polygon in shape.geoms:
                    rings = [polygon.exterior] + list(polygon.interiors)
                    polygon_arcs = []

                    for ring in rings:
                        polygon_arcs.append([len(arcs)])
                        arcs.append(diff_encode(ring, forward))

                    geometry['arcs'].append(polygon_arcs)

            else:
                raise NotImplementedError("Can't do %s geometries" %
                                          shape.type)

            geometries.append(geometry)

        geometries_by_layer[layer] = dict(
            type='GeometryCollection',
            geometries=geometries,
        )

    result = dict(
        type='Topology',
        transform=transform,
        objects=geometries_by_layer,
        arcs=arcs,
    )

    json.dump(result, file)
