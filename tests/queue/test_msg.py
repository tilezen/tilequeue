import unittest


class SingleMessageMarshallerTest(unittest.TestCase):

    def setUp(self):
        from tilequeue.queue.message import SingleMessageMarshaller
        self.msg_marshaller = SingleMessageMarshaller()

    def test_marshall_empty_list(self):
        with self.assertRaises(AssertionError):
            self.msg_marshaller.marshall([])

    def test_marshall_multiple_coords(self):
        from tilequeue.tile import deserialize_coord
        coords = map(deserialize_coord, ('1/1/1', '2/2/2'))
        with self.assertRaises(AssertionError):
            self.msg_marshaller.marshall(coords)

    def test_marshall_single_coord(self):
        from tilequeue.tile import deserialize_coord
        result = self.msg_marshaller.marshall([deserialize_coord('1/1/1')])
        self.assertEqual('1/1/1', result)

    def test_unmarshall_invalid(self):
        with self.assertRaises(AssertionError):
            self.msg_marshaller.unmarshall('invalid')

    def test_unmarshall_single(self):
        from tilequeue.tile import serialize_coord
        coords = self.msg_marshaller.unmarshall('1/1/1')
        self.assertEqual(1, len(coords))
        self.assertEqual('1/1/1', serialize_coord(coords[0]))

    def test_unmarshall_multiple(self):
        with self.assertRaises(AssertionError):
            self.msg_marshaller.unmarshall('1/1/1,2/2/2')


class MultipleMessageMarshallerTest(unittest.TestCase):

    def setUp(self):
        from tilequeue.queue.message import CommaSeparatedMarshaller
        self.msg_marshaller = CommaSeparatedMarshaller()

    def test_marshall_empty_list(self):
        actual = self.msg_marshaller.marshall([])
        self.assertEqual('', actual)

    def test_marshall_multiple_coords(self):
        from tilequeue.tile import deserialize_coord
        coords = map(deserialize_coord, ('1/1/1', '2/2/2'))
        actual = self.msg_marshaller.marshall(coords)
        self.assertEqual('1/1/1,2/2/2', actual)

    def test_marshall_single_coord(self):
        from tilequeue.tile import deserialize_coord
        result = self.msg_marshaller.marshall([deserialize_coord('1/1/1')])
        self.assertEqual('1/1/1', result)

    def test_unmarshall_invalid(self):
        with self.assertRaises(AssertionError):
            self.msg_marshaller.unmarshall('invalid')

    def test_unmarshall_empty(self):
        actual = self.msg_marshaller.unmarshall('')
        self.assertEqual([], actual)

    def test_unmarshall_single(self):
        from tilequeue.tile import serialize_coord
        coords = self.msg_marshaller.unmarshall('1/1/1')
        self.assertEqual(1, len(coords))
        self.assertEqual('1/1/1', serialize_coord(coords[0]))

    def test_unmarshall_multiple(self):
        from tilequeue.tile import deserialize_coord
        actual = self.msg_marshaller.unmarshall('1/1/1,2/2/2')
        self.assertEqual(2, len(actual))
        self.assertEqual(actual[0], deserialize_coord('1/1/1'))
        self.assertEqual(actual[1], deserialize_coord('2/2/2'))


class SingleMessageTrackerTest(unittest.TestCase):

    def setUp(self):
        from tilequeue.queue.message import SingleMessagePerCoordTracker
        self.tracker = SingleMessagePerCoordTracker()

    def test_track_and_done(self):
        from tilequeue.tile import deserialize_coord
        from tilequeue.queue.message import QueueHandle
        queue_id = 1
        queue_handle = QueueHandle(queue_id, 'handle')
        coords = [deserialize_coord('1/1/1')]
        coord_handles = self.tracker.track(queue_handle, coords)
        self.assertEqual(1, len(coord_handles))
        coord_handle = coord_handles[0]
        self.assertIs(queue_handle, coord_handle)

        returned_queue_handle, all_done = self.tracker.done(coord_handle)
        self.assertIs(queue_handle, returned_queue_handle)
        self.assertTrue(all_done)


class MultipleMessageTrackerTest(unittest.TestCase):

    def setUp(self):
        from mock import MagicMock
        from tilequeue.queue.message import MultipleMessagesPerCoordTracker
        msg_tracker_logger = MagicMock()
        self.tracker = MultipleMessagesPerCoordTracker(msg_tracker_logger)

    def test_track_and_done(self):
        from tilequeue.tile import deserialize_coord
        from tilequeue.queue.message import QueueHandle
        queue_id = 1
        queue_handle = QueueHandle(queue_id, 'handle')
        coords = map(deserialize_coord, ('1/1/1', '2/2/2'))
        coord_handles = self.tracker.track(queue_handle, coords)
        self.assertEqual(2, len(coord_handles))

        with self.assertRaises(ValueError):
            self.tracker.done('bogus-coord-handle')

        queue_handle_result, all_done = self.tracker.done(coord_handles[0])
        self.assertFalse(all_done)

        queue_handle_result, all_done = self.tracker.done(coord_handles[1])
        self.assertTrue(all_done)
        self.assertIs(queue_handle, queue_handle_result)
