import unittest


class QueueWriterTest(unittest.TestCase):

    def make_queue_writer(self):
        from mock import MagicMock
        from tilequeue.queue.inflight import NoopInFlightManager
        from tilequeue.queue.mapper import SingleQueueMapper
        from tilequeue.queue.message import SingleMessageMarshaller
        from tilequeue.queue.writer import QueueWriter

        queue = MagicMock()
        queue_mapper = SingleQueueMapper('queue_name', queue)
        msg_marshaller = SingleMessageMarshaller()
        inflight_mgr = NoopInFlightManager()
        enqueue_batch_size = 10
        queue_writer = QueueWriter(
            queue_mapper, msg_marshaller, inflight_mgr, enqueue_batch_size)
        return queue_writer

    def test_write_coords(self):
        from tilequeue.tile import deserialize_coord
        coords = [deserialize_coord('1/1/1'), deserialize_coord('15/1/1')]
        queue_writer = self.make_queue_writer()
        n_enqueued, n_inflight = queue_writer.enqueue_batch(coords)
        self.assertEquals(2, n_enqueued)
        self.assertEquals(0, n_inflight)
