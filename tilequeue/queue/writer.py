# coordinates all the pieces required to enqueue coordinates


from collections import defaultdict


class InFlightCounter(object):
    """store state while filtering in inflight"""

    def __init__(self, inflight_mgr):
        self.n_inflight = 0
        self.n_not_inflight = 0
        self.inflight_mgr = inflight_mgr

    def filter(self, coords):
        for coord in coords:
            if self.inflight_mgr.is_inflight(coord):
                self.n_inflight += 1
            else:
                self.n_not_inflight += 1
                yield coord


class QueueWriter(object):

    def __init__(self, queue_mapper, msg_marshaller, inflight_mgr,
                 enqueue_batch_size):
        self.queue_mapper = queue_mapper
        self.msg_marshaller = msg_marshaller
        self.inflight_mgr = inflight_mgr
        self.enqueue_batch_size = enqueue_batch_size

    def _enqueue_batch(self, queue_id, coords_chunks):
        queue = self.queue_mapper.get_queue(queue_id)
        assert queue, 'No queue found for: %s' % queue_id
        payloads = []
        all_coords = []
        for coords_chunk in coords_chunks:
            payload = self.msg_marshaller.marshall(coords_chunk)
            payloads.append(payload)
            all_coords.extend(coords_chunk)
        queue.enqueue_batch(payloads)
        self.inflight_mgr.mark_inflight(all_coords)

    def enqueue_batch(self, coords):
        inflight_ctr = InFlightCounter(self.inflight_mgr)
        coords = inflight_ctr.filter(coords)
        coord_groups = self.queue_mapper.group(coords)

        # buffer the coords to send out per queue
        queue_send_buffer = defaultdict(list)

        for coord_group in coord_groups:
            coords = coord_group.coords
            queue_id = coord_group.queue_id
            send_data = queue_send_buffer[queue_id]
            send_data.append(coords)
            if len(send_data) >= self.enqueue_batch_size:
                tile_queue = self.queue_mapper.get_queue(queue_id)
                assert tile_queue, 'No tile_queue found for: %s' % queue_id
                self._enqueue_batch(queue_id, send_data)
                del send_data[:]

        for queue_id, send_data in queue_send_buffer.iteritems():
            if send_data:
                self._enqueue_batch(queue_id, send_data)

        return inflight_ctr.n_not_inflight, inflight_ctr.n_inflight
