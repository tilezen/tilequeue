from tilequeue.tile import deserialize_coord
from tilequeue.tile import serialize_coord


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
    one-to-one mapping between message handles and coordinates

    Delegate to the queue directly and don't track any message mappings
    """

    def __init__(self, tile_queue):
        self.tile_queue = tile_queue

    def track(self, msg_handle, coords):
        assert len(coords) == 1
        # treat the message handle itself as the coordinate handle
        return [msg_handle]

    def done(self, coord_handle):
        msg_handle = coord_handle
        self.tile_queue.job_done(msg_handle)
        return msg_handle


class MultipleMessagesPerCoordTracker(object):

    """
    track a mapping for multiple coordinates

    Support tracking a mapping for multiple coordinates to a single
    queue message handle. When all coordinates for a particular
    message have been completed, ask the queue to mark the message
    complete.
    """

    def __init__(self, tile_queue):
        self.tile_queue = tile_queue

        # use an opaque id for indirect reference to message handles
        self.handle_id_to_msg_handle = {}
        # lookup the opaque handle id for a coordinate id
        self.coord_id_to_handle_id = {}
        # state for the open set of coordinates for a given message handle id
        # TODO we might want to have a way to purge this, or risk
        # running out of memory if a coordinate never completes
        self.handle_id_to_coords = {}

    def track(self, msg_handle, coords):

        handle_id = id(msg_handle)
        self.handle_id_to_msg_handle.setdefault(handle_id, msg_handle)

        coord_id_set = set()
        coord_handles = []
        for coord in coords:
            coord_id = id(coord)
            self.coord_id_to_handle_id[coord_id] = handle_id
            coord_id_set.add(coord_id)
            coord_handles.append(coord_id)

        self.handle_id_to_coords[handle_id] = coord_id_set

        return coord_handles

    def done(self, coord_handle):
        handle_id = self.coord_id_to_handle_id.get(coord_handle)
        assert handle_id
        coord_id_set = self.handle_id_to_coords.get(handle_id)
        assert coord_id_set is not None
        coord_id_set.remove(coord_handle)
        msg_handle = self.handle_id_to_msg_handle.get(handle_id, None)
        assert msg_handle
        if not coord_id_set:
            # we're done with all coordinates in this set, and can ask
            # the queue to complete the message
            # clear the state first, so in case of exception we don't
            # leak them
            del self.handle_id_to_msg_handle[handle_id]
            del self.handle_id_to_coords[handle_id]
            self.tile_queue.job_done(msg_handle)
        return msg_handle
