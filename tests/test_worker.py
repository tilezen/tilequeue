import unittest


class TestWorker(unittest.TestCase):
    def _build_message(self, row=1, column=1, zoom=1):
        from tilequeue.tile import CoordMessage
        from ModestMaps.Core import Coordinate
        from tilequeue.tile import serialize_coord
        from boto.sqs.message import RawMessage
        coord = Coordinate(row=row, column=column, zoom=zoom)
        payload = serialize_coord(coord)
        message = RawMessage()
        message.set_body(payload)
        message.attributes = dict(SentTimestamp=1)
        return CoordMessage(coord, message)

    def test_defaults_to_non_daemonized(self):
        from tilequeue.worker import Worker
        worker = Worker(None, None)
        self.assertFalse(worker.daemonized)

    def test_sets_to_daemonized(self):
        from tilequeue.worker import Worker
        worker = Worker(None, None)
        worker.daemonized = True
        self.assertTrue(worker.daemonized)

    def test_defaults_to_no_logger(self):
        from tilequeue.worker import Worker
        worker = Worker(None, None)
        self.assertIsNone(worker.logger)

    def test_set_logger(self):
        from tilequeue.worker import Worker
        from mock import MagicMock
        worker = Worker(None, None)
        worker.logger = MagicMock()
        self.assertIsNotNone(worker.logger)

    def test_process_doesnt_call_process_jobs_for_coord(self):
        from tilequeue.worker import Worker
        from mock import MagicMock
        job_creator_mock = MagicMock()
        queue_mock = MagicMock()
        worker = Worker(queue_mock, job_creator_mock)
        queue_mock.read = MagicMock(return_value=[])
        worker.process()
        calls = len(job_creator_mock.process_jobs_for_coord.call_args_list)
        self.assertTrue(calls == 0)

    def test_process_calls_process_jobs_for_coord_for_messages(self):
        from tilequeue.worker import Worker
        from mock import MagicMock
        job_creator_mock = MagicMock()
        queue_mock = MagicMock()
        worker = Worker(queue_mock, job_creator_mock)
        message = self._build_message(row=1, column=2, zoom=3)
        queue_mock.read = MagicMock(return_value=[message, message])
        worker.process()
        calls = len(job_creator_mock.process_jobs_for_coord.call_args_list)
        self.assertTrue(calls == 2)

    def test_process_calls_process_jobs_for_coord_with_same_coord(self):
        from tilequeue.worker import Worker
        from mock import MagicMock
        from tilequeue.tile import serialize_coord
        job_creator_mock = MagicMock()
        queue_mock = MagicMock()
        worker = Worker(queue_mock, job_creator_mock)
        message = self._build_message(row=1, column=2, zoom=3)
        queue_mock.read = MagicMock(return_value=[message])
        worker.process()
        argument = \
            job_creator_mock.process_jobs_for_coord.call_args_list[0][0][0]
        self.assertEqual(serialize_coord(argument), "3/2/1")

    def test_process_with_multiple_read_messages(self):
        from tilequeue.worker import Worker
        from mock import MagicMock
        job_creator_mock = MagicMock()
        queue_mock = MagicMock()
        worker = Worker(queue_mock, job_creator_mock)
        message = self._build_message(row=1, column=2, zoom=3)
        queue_mock.read = MagicMock(return_value=[message])
        worker.process(4)
        queue_mock.read.assert_called_once_with(max_to_read=4)

    def test_process_marks_job_done(self):
        from tilequeue.worker import Worker
        from mock import MagicMock
        job_creator_mock = MagicMock()
        queue_mock = MagicMock()
        message_mock = MagicMock()
        message_mock.get_body = lambda: "1/1/1"
        message_mock.message_handle = message_mock
        queue_mock.read = MagicMock(return_value=[message_mock])
        worker = Worker(queue_mock, job_creator_mock)
        worker.process()
        queue_mock.job_done.assert_called_once_with(message_mock)

    def test_process_logs_timing(self):
        from tilequeue.worker import Worker
        from mock import MagicMock
        job_creator_mock = MagicMock()
        queue_mock = MagicMock()
        message = self._build_message(row=1, column=2, zoom=3)
        queue_mock.read = MagicMock(return_value=[message])
        logger_mock = MagicMock()
        worker = Worker(queue_mock, job_creator_mock)
        worker.logger = logger_mock
        worker.process()
        timing = False
        for call in logger_mock.info.call_args_list:
            if "done took" in call[0][0]:
                timing = True
        self.assertTrue(timing)
        logger_mock.info.assert_any_call('processing 3/2/1 ...')
