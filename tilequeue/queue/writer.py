# coordinates all the pieces required to enqueue coordinates


class QueueWriter(object):

    def __init__(self, tile_queue, coord_grouper, msg_marshaller,
                 inflight_manager, enqueue_batch_size):
        self.tile_queue = tile_queue
        self.coord_grouper = coord_grouper
        self.msg_marshaller = msg_marshaller
        self.inflight_manager = inflight_manager
        self.enqueue_batch_size = enqueue_batch_size

    def _enqueue_batch(self, queue_send_data_chunk):
        payloads = []
        all_coords = []
        # used for metadata
        zooms = []
        for payload, coords, metadata in queue_send_data_chunk:
            payloads.append(payload)
            all_coords.extend(coords)
            zoom = metadata.get('zoom')
            assert zoom is not None
            zooms.append(zoom)

        metadata = dict(zooms=zooms)
        self.tile_queue.enqueue_batch(payloads, metadata)
        all_coords = set(all_coords)
        self.inflight_manager.mark_inflight(all_coords)

    def enqueue_batch(self, coords):
        coords = self.inflight_manager.filter(coords)
        coord_groups = self.coord_grouper(coords)

        # buffers the tile_queue send
        queue_send_data_chunk = []
        for coord_group in coord_groups:
            coords = coord_group.coords
            metadata = coord_group.metadata
            payload = self.msg_marshaller.marshall(coords)
            queue_send_data = payload, coords, metadata
            queue_send_data_chunk.append(queue_send_data)
            if len(queue_send_data_chunk) >= self.enqueue_batch_size:
                self._enqueue_batch(queue_send_data_chunk)
                del queue_send_data_chunk[:]
        if queue_send_data_chunk:
            self._enqueue_batch(queue_send_data_chunk)
