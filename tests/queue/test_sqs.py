import unittest


class TestQueue(unittest.TestCase):
    def setUp(self):
        from mock import MagicMock
        from tilequeue.queue import SqsQueue

        self.mockClient = MagicMock()
        self.sqs = SqsQueue(self.mockClient, 'queue-url', 10, 20)

    def test_enqueue_batch_adds_tiles(self):
        from mock import MagicMock
        coords = ['1/1/1', '2/2/2']
        self.mockClient.send_message_batch = MagicMock(
            return_value=dict(ResponseMetadata=dict(HTTPStatusCode=200)),
        )
        self.sqs.enqueue_batch(coords)
        self.mockClient.send_message_batch.assert_called_with([
            {'Id': '0', 'MessageBody': '1/1/1'},
            {'Id': '1', 'MessageBody': '2/2/2'}])

    def test_enqueue_should_write_message_to_queue(self):
        from mock import MagicMock
        self.mockClient.send = MagicMock(
            return_value=dict(ResponseMetadata=dict(HTTPStatusCode=200)),
        )
        self.sqs.enqueue('1/1/1')
        self.mockClient.send.assert_called_with(
            MessageBody='1/1/1', QueueUrl='queue-url')
