import unittest


def _make_test_get_queue_name_for_zoom(queue_name):
    def test_get_queue_name_for_zoom(zoom):
        return queue_name
    return test_get_queue_name_for_zoom


def _test_q1_q2_get_queue_name_for_zoom(zoom):
    if zoom == 1:
        return 'q1'
    else:
        assert zoom == 2
        return 'q2'


class MultiSqsTest(unittest.TestCase):

    def setUp(self):
        self.redis_client = None
        self.mock_written = None
        self.mock_write_batch = None

    def _mock_write(self, msg):
        self.mock_written = msg

    def _mock_write_batch(self, msg_tuples):
        self.mock_write_batch = msg_tuples

    def _make_one(self, queues=(), get_queue_name_for_zoom=None,
                  redis_client=None, is_seeding=True):
        from mock import MagicMock
        from tilequeue.queue.multisqs import MultiSqsQueue
        if get_queue_name_for_zoom is None:
            get_queue_name_for_zoom = _make_test_get_queue_name_for_zoom('q1')
        if redis_client is None:
            self.redis_client = redis_client = MagicMock()
        result = MultiSqsQueue(
            queues, get_queue_name_for_zoom, redis_client, is_seeding)
        return result

    def _make_get_messages(self, coord_strs):
        qms = []

        def _make_get_body(s):
            def get_body():
                return s
            return get_body

        for coord_str in coord_strs:
            qm_type = type('queue-message', (), dict(
                attributes=dict(SentTimeStamp=None),
                get_body=None,
                ))
            qm = qm_type()
            qm.get_body = _make_get_body(coord_str)
            qms.append(qm)

        def _read(*args, **kw):
            return qms
        return _read

    def _mock_delete_message(self, handle):
        self.mock_handle = handle

    def test_enqueue(self):
        from mock import MagicMock
        from tilequeue.tile import deserialize_coord
        sqs_queue = MagicMock()
        sqs_queue.name = 'q1'
        msq = self._make_one([sqs_queue])
        sqs_queue.write = self._mock_write
        msq.enqueue(deserialize_coord('1/1/1'))
        self.assertIsNotNone(self.mock_written)
        self.assertEqual(self.mock_written.get_body(), '1/1/1')

    def test_enqueue_batch(self):
        from mock import MagicMock
        from tilequeue.tile import deserialize_coord

        sqs_queue1 = MagicMock()
        sqs_queue1.name = 'q1'
        sqs_queue1.write_batch = self._mock_write_batch

        sqs_queue2 = MagicMock()
        sqs_queue2.name = 'q2'
        sqs_queue2.write_batch = self._mock_write_batch

        msq = self._make_one([sqs_queue1, sqs_queue2])
        msq.enqueue_batch([
            deserialize_coord('1/1/1'),
            deserialize_coord('1/0/0')
        ])

        self.assertIsNotNone(self.mock_write_batch)
        self.assertEqual(2, len(self.mock_write_batch))
        coord_str1, coord_str2 = [x[1] for x in self.mock_write_batch]
        self.assertEqual(coord_str1, '1/1/1')
        self.assertEqual(coord_str2, '1/0/0')

    def test_read_single(self):
        from mock import MagicMock
        from tilequeue.tile import deserialize_coord
        from tilequeue.tile import serialize_coord

        sqs_queue = MagicMock()
        coord = deserialize_coord('1/1/1')
        sqs_queue.get_messages = self._make_get_messages(
            [serialize_coord(coord)])
        msq = self._make_one([sqs_queue])
        coord_msgs = msq.read()
        self.assertEqual(1, len(coord_msgs))
        self.assertEqual(coord, coord_msgs[0].coord)

    def test_read_multiple(self):
        from mock import MagicMock
        from tilequeue.tile import deserialize_coord
        from tilequeue.tile import serialize_coord

        coord1 = deserialize_coord('1/1/1')
        coord2 = deserialize_coord('2/2/2')
        sqs_queue1 = MagicMock()
        sqs_queue2 = MagicMock()

        sqs_queue1.name = 'q1'
        sqs_queue1.get_messages = self._make_get_messages(
            [serialize_coord(coord1)])
        sqs_queue2.name = 'q2'
        sqs_queue2.get_messages = self._make_get_messages(
            [serialize_coord(coord2)])

        msq = self._make_one(
            [sqs_queue1, sqs_queue2],
            _test_q1_q2_get_queue_name_for_zoom,
        )
        coord_msgs = msq.read()
        self.assertEqual(2, len(coord_msgs))
        self.assertEqual(coord1, coord_msgs[0].coord)
        self.assertEqual(coord2, coord_msgs[1].coord)

    def test_job_done_single(self):
        from mock import MagicMock
        from tilequeue.tile import CoordMessage
        from tilequeue.tile import deserialize_coord

        coord = deserialize_coord('1/1/1')
        sqs_queue = MagicMock()
        sqs_queue.name = 'q1'
        sqs_queue.delete_message = self._mock_delete_message
        cm = CoordMessage(coord, 'msg_handle', dict(queue_name=sqs_queue.name))
        msq = self._make_one([sqs_queue])
        msq.job_done(cm)
        self.assertIsNotNone(self.mock_handle)
        self.assertEqual(self.mock_handle, 'msg_handle')

    def test_job_done_multiple(self):
        from mock import MagicMock
        from tilequeue.tile import CoordMessage
        from tilequeue.tile import deserialize_coord

        coord1 = deserialize_coord('1/1/1')
        sqs_queue1 = MagicMock()
        sqs_queue1.name = 'q1'
        sqs_queue1.delete_message = self._mock_delete_message
        coord2 = deserialize_coord('2/2/2')
        sqs_queue2 = MagicMock()
        sqs_queue2.name = 'q2'
        sqs_queue2.delete_message = self._mock_delete_message

        msq = self._make_one(
            [sqs_queue1, sqs_queue2],
            _test_q1_q2_get_queue_name_for_zoom,
        )

        cm1 = CoordMessage(coord1, 'msg_handle1',
                           metadata=dict(queue_name=sqs_queue1.name))
        msq.job_done(cm1)
        self.assertIsNotNone(self.mock_handle)
        self.assertEqual(self.mock_handle, 'msg_handle1')
        self.mock_handle = None

        cm2 = CoordMessage(coord2, 'msg_handle2',
                           metadata=dict(queue_name=sqs_queue2.name))
        msq.job_done(cm2)
        self.assertIsNotNone(self.mock_handle)
        self.assertEqual(self.mock_handle, 'msg_handle2')
        self.mock_handle = None

    def test_job_done_no_queue_name(self):
        from mock import MagicMock
        from tilequeue.tile import CoordMessage
        from tilequeue.tile import deserialize_coord

        coord = deserialize_coord('1/1/1')
        sqs_queue = MagicMock()
        sqs_queue.name = 'q1'
        sqs_queue.delete_message = self._mock_delete_message
        cm = CoordMessage(coord, 'msg_handle')
        msq = self._make_one([sqs_queue])
        with self.assertRaises(AssertionError):
            msq.job_done(cm)
