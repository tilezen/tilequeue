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
        from tilequeue.queue import MessageHandle
        from tilequeue.queue.message import QueueMessageHandle
        msg_handle = MessageHandle('handle', 'payload')
        queue_id = 1
        queue_msg_handle = QueueMessageHandle(queue_id, msg_handle)
        coords = [deserialize_coord('1/1/1')]
        coord_handles = self.tracker.track(queue_msg_handle, coords)
        self.assertEqual(1, len(coord_handles))
        coord_handle = coord_handles[0]
        self.assertIs(queue_msg_handle, coord_handle)

        returned_msg_handle, all_done = self.tracker.done(coord_handle)
        self.assertIs(queue_msg_handle, returned_msg_handle)
        self.assertTrue(all_done)


class MultipleMessageTrackerTest(unittest.TestCase):

    def setUp(self):
        from tilequeue.queue.message import MultipleMessagesPerCoordTracker
        self.tracker = MultipleMessagesPerCoordTracker()

    def test_track_and_done(self):
        from tilequeue.tile import deserialize_coord
        from tilequeue.queue import MessageHandle
        from tilequeue.queue.message import QueueMessageHandle
        msg_handle = MessageHandle('handle', 'payload')
        queue_id = 1
        queue_msg_handle = QueueMessageHandle(queue_id, msg_handle)
        coords = map(deserialize_coord, ('1/1/1', '2/2/2'))
        coord_handles = self.tracker.track(queue_msg_handle, coords)
        self.assertEqual(2, len(coord_handles))

        with self.assertRaises(AssertionError):
            self.tracker.done('bogus-coord-handle')

        msg_handle_result, all_done = self.tracker.done(coord_handles[0])
        self.assertFalse(all_done)

        msg_handle_result, all_done = self.tracker.done(coord_handles[1])
        self.assertTrue(all_done)
        self.assertIs(queue_msg_handle, msg_handle_result)
