import unittest

from tilequeue.queue import SqsQueue
from ModestMaps.Core import Coordinate
from mock import MagicMock
from boto.sqs.message import RawMessage
from tilequeue.tile import serialize_coord


class TestQueue(unittest.TestCase):
    def setUp(self):
        self.message = None
        self.mockQueue = MagicMock()
        self.mockQueue.write = self.fake_write
        self.mockQueue.write_batch = self.fake_write_batch
        self.mockRedis = MagicMock()
        self.sqs = SqsQueue(self.mockQueue, self.mockRedis)
        self.values = []
        self.key_name = None
        self.coords = None

    def fake_write(self, message):
        self.message = message

    def fake_write_batch(self, message_tuples):
        self.message_tuples = message_tuples

    def fake_sadd(self, name, *value):
        self.key_name = name
        if isinstance(value, (list, tuple)):
            for val in value:
                self.values.append(val)
        else:
            self.values.append(value)

    def test_enqueue_should_check_if_pending_work(self):
        from tilequeue.tile import coord_marshall_int
        coord = Coordinate(row=1, column=1, zoom=1)
        self.sqs.enqueue(coord)
        exp_value = coord_marshall_int(coord)
        self.mockRedis.sismember.assert_called_once_with(self.sqs.inflight_key,
                                                         exp_value)

    def test_enqueue_batch_adds_tiles(self):
        coords = [Coordinate(row=1, column=1, zoom=1),
                  Coordinate(row=2, column=2, zoom=2)]
        mock = MagicMock()
        mock.side_effect = [False, False]
        self.mockRedis.sismember = mock
        self.sqs.enqueue_batch(coords)
        self.assertEqual(2, len(self.message_tuples))
        self.assertEqual(self.message_tuples[0][1], "1/1/1")
        self.assertEqual(self.message_tuples[1][1], "2/2/2")

    def test_enqueue_batch_does_not_add_redundant_tile_in_flight(self):
        coords = [Coordinate(row=1, column=1, zoom=1),
                  Coordinate(row=2, column=2, zoom=2)]
        mock = MagicMock()
        mock.side_effect = [True, False]
        self.mockRedis.sismember = mock
        self.sqs.enqueue_batch(coords)
        self.assertEqual(1, len(self.message_tuples))
        self.assertEqual(self.message_tuples[0][1], "2/2/2")

    def test_enqueue_should_write_message_to_queue(self):
        self.mockRedis.sismember = MagicMock(return_value=False)
        coord = Coordinate(row=1, column=1, zoom=1)
        self.sqs.enqueue(coord)
        self.assertIsNotNone(self.message)
        self.assertEqual("1/1/1", self.message.get_body())

    def test_enqueue_should_not_write_message_to_queue(self):
        self.mockRedis.sismember = MagicMock(return_value=True)
        coord = Coordinate(row=1, column=1, zoom=1)
        self.sqs.enqueue(coord)
        self.assertEqual(None, self.message)

    def test_enqueue_adds_tile_as_in_flight(self):
        self.mockRedis.sismember = MagicMock(return_value=False)
        mock = MagicMock()
        self.mockRedis.sadd = mock
        coord = Coordinate(row=1, column=1, zoom=1)
        self.sqs.enqueue(coord)
        from tilequeue.tile import coord_marshall_int
        exp_value = coord_marshall_int(coord)
        self.mockRedis.sadd.assert_called_once_with(self.sqs.inflight_key,
                                                    exp_value)

    def test_enqueue_batch_adds_tiles_as_in_flight(self):
        from tilequeue.tile import coord_marshall_int
        coords = [Coordinate(row=1, column=1, zoom=1),
                  Coordinate(row=2, column=2, zoom=2)]
        mock = MagicMock()
        mock.side_effect = [False, False]
        self.mockRedis.sismember = mock
        self.mockRedis.sadd = self.fake_sadd
        self.sqs.enqueue_batch(coords)
        self.assertEqual(self.key_name, self.sqs.inflight_key)
        exp_values = map(coord_marshall_int, coords)
        self.assertEqual(exp_values, self.values)

    def test_job_done_removes_tile_from_in_flight(self):
        from tilequeue.tile import CoordMessage
        coord = Coordinate(row=1, column=1, zoom=1)
        payload = serialize_coord(coord)
        message = RawMessage()
        message.set_body(payload)
        coord_message = CoordMessage(coord, message)
        self.sqs.job_done(coord_message)
        from tilequeue.tile import coord_marshall_int
        exp_value = coord_marshall_int(coord)
        self.mockRedis.srem.assert_called_once_with(self.sqs.inflight_key,
                                                    exp_value)

    def test_clear_removes_in_flight(self):
        self.mockQueue.get_messages = MagicMock(return_value=[])
        self.sqs.clear()
        self.mockRedis.delete.assert_called_once_with(self.sqs.inflight_key)
