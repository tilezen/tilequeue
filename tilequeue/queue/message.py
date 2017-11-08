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

    def __init__(self, queue_id, handle, metadata=None):
        self.queue_id = queue_id
        self.handle = handle
        self.metadata = metadata


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
        return queue_handle, all_done


class MultipleMessagesPerCoordTracker(object):

    """
    track a mapping for multiple coordinates

    Support tracking a mapping for multiple coordinates to a single
    queue handle.
    """

    def __init__(self):
        self.queue_handle_map = {}
        self.coord_ids_map = {}
        # TODO we might want to have a way to purge this, or risk
        # running out of memory if a coordinate never completes
        self.lock = threading.Lock()

    def track(self, queue_handle, coords):
        with self.lock:
            # rely on the queue handle token as the mapping key
            queue_handle_id = queue_handle.handle
            self.queue_handle_map[queue_handle_id] = queue_handle

            coord_ids = set()
            coord_handles = []
            for coord in coords:
                coord_id = (int(coord.zoom), int(coord.column), int(coord.row))
                coord_handle = (coord_id, queue_handle_id)
                coord_ids.add(coord_id)
                coord_handles.append(coord_handle)

            self.coord_ids_map[queue_handle_id] = coord_ids

        return coord_handles

    def done(self, coord_handle):
        with self.lock:
            coord_id, queue_handle_id = coord_handle
            coord_ids = self.coord_ids_map[queue_handle_id]
            coord_ids.remove(coord_id)
            queue_handle = self.queue_handle_map[queue_handle_id]

            all_done = False
            if not coord_ids:
                # we're done with all coordinates in this set, and can ask
                # the queue to complete the message
                del self.queue_handle_map[queue_handle_id]
                del self.coord_ids_map[queue_handle_id]
                all_done = True

        return queue_handle, all_done
