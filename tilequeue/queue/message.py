from collections import namedtuple
from tilequeue.tile import deserialize_coord
from tilequeue.tile import serialize_coord
import threading


class MessageHandle(object):

    """
    represents a message read from a queue

    This encapsulates both the payload and an opaque message handle
    that's queue specific. When a job is complete, this handle is
    given back to the queue, to allow for implementations to mark
    completion for those that support it.
    """

    def __init__(self, handle, payload, metadata=None):
        # metadata is optional, and can capture information like the
        # timestamp and age of the message, which can be useful to log
        self.handle = handle
        self.payload = payload
        self.metadata = metadata


class QueueHandle(object):
    """
    message handle combined with a queue id
    """

    def __init__(self, queue_id, handle):
        self.queue_id = queue_id
        self.handle = handle


class SingleMessageMarshaller(object):

    """marshall/unmarshall a single coordinate from a queue message"""

    def marshall(self, coords):
        assert len(coords) == 1
        coord = coords[0]
        return serialize_coord(coord)

    def unmarshall(self, payload):
        coord = deserialize_coord(payload)
        assert coord
        return [coord]


class CommaSeparatedMarshaller(object):

    """
    marshall/unmarshall coordinates in a comma separated format

    coordinates are represented textually as z/x/y separated by commas
    """

    def marshall(self, coords):
        return ','.join(serialize_coord(x) for x in coords)

    def unmarshall(self, payload):
        coord_strs = payload.split(',')
        coords = []
        for coord_str in coord_strs:
            coord_str = coord_str.strip()
            if coord_str:
                coord = deserialize_coord(coord_str)
                assert coord
                coords.append(coord)
        return coords


MessageDoneResult = namedtuple(
    'MessageDoneResult',
    ('queue_handle all_done parent_tile'))


class SingleMessagePerCoordTracker(object):

    """
    one-to-one mapping between queue handles and coordinates
    """

    def track(self, queue_handle, coords):
        assert len(coords) == 1
        return [queue_handle]

    def done(self, coord_handle):
        queue_handle = coord_handle
        all_done = True
        parent_tile = None
        return MessageDoneResult(queue_handle, all_done, parent_tile)


class MultipleMessagesPerCoordTracker(object):

    """
    track a mapping for multiple coordinates

    Support tracking a mapping for multiple coordinates to a single
    queue handle.
    """

    def __init__(self, msg_tracker_logger):
        self.msg_tracker_logger = msg_tracker_logger
        self.queue_handle_map = {}
        self.coord_ids_map = {}
        self.pyramid_map = {}
        # TODO we might want to have a way to purge this, or risk
        # running out of memory if a coordinate never completes
        self.lock = threading.Lock()

    def track(self, queue_handle, coords, parent_tile=None):
        is_pyramid = len(coords) > 1
        if is_pyramid:
            assert parent_tile is not None, "parent tile was not provided, " \
                "but is required for tracking pyramids of tiles."

        with self.lock:
            # rely on the queue handle token as the mapping key
            queue_handle_id = queue_handle.handle
            self.queue_handle_map[queue_handle_id] = queue_handle

            coord_ids = set()
            coord_handles = []
            for coord in coords:
                coord_id = (int(coord.zoom), int(coord.column), int(coord.row))
                coord_handle = (coord_id, queue_handle_id)
                assert coord_id not in coord_ids
                coord_ids.add(coord_id)
                coord_handles.append(coord_handle)

            self.coord_ids_map[queue_handle_id] = coord_ids

        if is_pyramid:
            self.pyramid_map[queue_handle_id] = parent_tile

        return coord_handles

    def done(self, coord_handle):
        queue_handle = None
        all_done = False
        parent_tile = None

        with self.lock:
            coord_id, queue_handle_id = coord_handle

            coord_ids = self.coord_ids_map.get(queue_handle_id)
            queue_handle = self.queue_handle_map.get(queue_handle_id)

            if queue_handle is None or coord_ids is None:
                self.msg_tracker_logger.unknown_queue_handle_id(
                    coord_id, queue_handle_id)
                return MessageDoneResult(None, False, None)

            if coord_id not in coord_ids:
                self.msg_tracker_logger.unknown_coord_id(
                    coord_id, queue_handle_id)
            else:
                coord_ids.remove(coord_id)

            if not coord_ids:
                # we're done with all coordinates for the queue message
                try:
                    del self.queue_handle_map[queue_handle_id]
                except KeyError:
                    pass
                try:
                    del self.coord_ids_map[queue_handle_id]
                except KeyError:
                    pass
                all_done = True
                parent_tile = self.pyramid_map.pop(queue_handle_id, None)

        return MessageDoneResult(queue_handle, all_done, parent_tile)
