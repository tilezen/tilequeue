class MessageHandle(object):

    """
    represents a message read from a queue

    This encapsulates both the coordinate and an opaque message handle
    that's queue specific. When a job is complete, this handle is
    given back to the queue, to allow for implementations to mark
    completion for those that support it.
    """

    def __init__(self, handle, coord, metadata=None):
        # metadata is optional, and can capture information like the
        # timestamp and age of the message, which can be useful to log
        self.handle = handle
        self.coord = coord
        self.metadata = metadata
