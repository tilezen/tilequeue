from collections import defaultdict
from collections import namedtuple

from tilequeue.tile import coord_marshall_int


# this is what gets returned by the group function
# each group represents what the payload to the queue should be
# the queue_id exists to allow dispatching to different underlying
# tile queue implementations. This can be useful when having several
# priority queues, each dedicated to their own zoom level range
CoordGroup = namedtuple('CoordGroup', 'coords queue_id')


class SingleQueueMapper(object):

    def __init__(self, queue_name, tile_queue):
        self.queue_name = queue_name
        self.tile_queue = tile_queue

    def group(self, coords):
        for coord in coords:
            yield CoordGroup([coord], self.queue_name)

    def get_queue(self, queue_id):
        assert queue_id == self.queue_name, 'Unknown queue_id: %s' % queue_id
        return self.tile_queue

    def queues_in_priority_order(self):
        return ((self.queue_name, self.tile_queue),)


# what gets passed into the zoom range mapper
# pass in None for start/end to add queues that are read from
# but that aren't considered for enqueueing directly when dispatching
# eg a high priority queue
ZoomRangeQueueSpec = namedtuple(
    'ZoomRangeCfgSpec',
    'start end queue_name queue group_by_zoom in_toi')
# set the last parameter to default to None
ZoomRangeQueueSpec.__new__.__defaults__ = (None,)

# what the mapper uses internally
# these are what will get checked for queue dispatch
ZoomRangeQueueItem = namedtuple(
    'ZoomRangeItem',
    'start end queue_id group_by_zoom in_toi')


class ZoomRangeAndZoomGroupQueueMapper(object):

    def __init__(self, zoom_range_specs, toi=None):
        # NOTE: zoom_range_specs should be passed in priority order
        self.zoom_range_items = []
        self.queue_mapping = []
        for i, zrs in enumerate(zoom_range_specs):
            self.queue_mapping.append(zrs.queue)
            zri = ZoomRangeQueueItem(zrs.start, zrs.end, i, zrs.group_by_zoom,
                                     zrs.in_toi)
            self.zoom_range_items.append(zri)

        # check that if any queue item uses the TOI as part of the check, then
        # we have been passed a TOI object to check it against.
        uses_toi = any(zri.in_toi is not None for zri in self.zoom_range_items)
        if uses_toi:
            assert toi is not None, 'If any zoom range item depends on ' \
                'whether a coordinate is in the TOI then a TOI object must ' \
                'be provided, but there is only None.'

            # NOTE: this is a one-off operation, so for long-running processes,
            # we must either re-create the mapper object, or periodically
            # refresh the TOI set.
            self.toi_set = toi.fetch_tiles_of_interest()

    def group(self, coords):
        """return CoordGroups that can be used to send to queues

        Each CoordGroup represents a message that can be sent to a
        particular queue, stamped with the queue_id. The list of
        coords, which can be 1, is what should get used for the
        payload for each queue message.
        """

        groups = []
        for i in range(len(self.zoom_range_items)):
            groups.append([])

        # first group the coordinates based on their queue
        for coord in coords:
            for i, zri in enumerate(self.zoom_range_items):
                toi_match = zri.in_toi is None or \
                    (coord in self.toi_set) == zri.in_toi
                if zri.start <= coord.zoom < zri.end and toi_match:
                    groups[i].append(coord)
                    break

        # now, we need to just verify that for each particular group,
        # should they be further grouped, eg by a particular zoom 10
        # tile
        for i, zri in enumerate(self.zoom_range_items):
            group = groups[i]
            if not group:
                continue
            if zri.group_by_zoom is None:
                for coord in group:
                    yield CoordGroup([coord], zri.queue_id)
            else:
                by_parent_coords = defaultdict(list)
                for coord in group:
                    if coord.zoom >= zri.group_by_zoom:
                        group_coord = coord.zoomTo(zri.group_by_zoom)
                        group_key = coord_marshall_int(group_coord)
                        by_parent_coords[group_key].append(coord)
                    else:
                        # this means that a coordinate belonged to a
                        # particular queue but the zoom was lower than
                        # the group by zoom
                        # this probably shouldn't happen
                        # should it be an assert instead?
                        yield CoordGroup([coord], zri.queue_id)

                for group_key, coords in by_parent_coords.iteritems():
                    yield CoordGroup(coords, zri.queue_id)

    def get_queue(self, queue_id):
        assert 0 <= queue_id < len(self.queue_mapping)
        return self.queue_mapping[queue_id]

    def queues_in_priority_order(self):
        return enumerate(self.queue_mapping)
