from collections import defaultdict
from collections import namedtuple
from tilequeue.tile import coord_marshall_int


CoordGroup = namedtuple('CoordGroup', 'coords metadata')


class StartZoomGrouper(object):

    """
    group coords by a parent zoom tile, like z10

    Take a sequence of coordinates, and emit a sequence of CoordGroup
    instances. These contains coords grouped by their parent zoom, and
    metadata that contains the parent zoom used.
    """

    def __init__(self, start_zoom_to_group):
        self.start_zoom_to_group = start_zoom_to_group

    def __call__(self, coords):
        grouped = defaultdict(list)
        for coord in coords:
            if coord.zoom >= self.start_zoom_to_group:
                coord_at_group_zoom = (
                    coord.zoomTo(self.start_zoom_to_group).container())
                group_key = coord_marshall_int(coord_at_group_zoom)
                grouped[group_key].append(coord)
            else:
                metadata = dict(
                    grouped=False,
                    zoom=coord.zoom,
                )
                coord_group = CoordGroup([coord], metadata)
                yield coord_group

        for group_key, coords in grouped.iteritems():
            metadata = dict(
                grouped=True,
                zoom=self.start_zoom_to_group,
            )
            coord_group = CoordGroup(coords, metadata)
            yield coord_group
