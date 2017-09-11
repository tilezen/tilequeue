import unittest

from ModestMaps.Core import Coordinate
from mock import MagicMock


class TestQueue(unittest.TestCase):
    def setUp(self):
        from tilequeue.queue import SqsQueue

        self.message = None
        self.mockQueue = MagicMock()
        self.mockQueue.write = self.fake_write
        self.mockQueue.write_batch = self.fake_write_batch
        self.sqs = SqsQueue(self.mockQueue)
        self.values = []
        self.key_name = None
        self.coords = None

    def fake_write(self, message):
        self.message = message

    def fake_write_batch(self, message_tuples):
        self.message_tuples = message_tuples

    def test_enqueue_batch_adds_tiles(self):
        coords = [Coordinate(row=1, column=1, zoom=1),
                  Coordinate(row=2, column=2, zoom=2)]
        mock = MagicMock()
        mock.side_effect = [False, False]
        self.sqs.enqueue_batch(coords)
        self.assertEqual(2, len(self.message_tuples))
        self.assertEqual(self.message_tuples[0][1], "1/1/1")
        self.assertEqual(self.message_tuples[1][1], "2/2/2")

    def test_enqueue_should_write_message_to_queue(self):
        coord = Coordinate(row=1, column=1, zoom=1)
        self.sqs.enqueue(coord)
        self.assertIsNotNone(self.message)
        self.assertEqual("1/1/1", self.message.get_body())
