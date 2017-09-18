import unittest


class TestQueue(unittest.TestCase):
    def setUp(self):
        from mock import MagicMock
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
        from mock import MagicMock
        coords = ['1/1/1', '2/2/2']
        mock = MagicMock()
        mock.side_effect = [False, False]
        self.sqs.enqueue_batch(coords)
        self.assertEqual(2, len(self.message_tuples))
        self.assertEqual(self.message_tuples[0][1], "1/1/1")
        self.assertEqual(self.message_tuples[1][1], "2/2/2")

    def test_enqueue_should_write_message_to_queue(self):
        self.sqs.enqueue('1/1/1')
        self.assertIsNotNone(self.message)
        self.assertEqual('1/1/1', self.message.get_body())
