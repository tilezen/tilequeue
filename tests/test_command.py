import unittest


class TestUniquifyGenerator(unittest.TestCase):

    enqueued_list = set()

    def fake_enqueue(self, value):
        self.enqueued_list.add(value)

    def test_uniquify_generator(self):
        from tilequeue.command import uniquify_generator
        from itertools import cycle, islice, tee
        gen = islice(cycle(xrange(5)), 10)
        gen, gencopy = tee(gen)
        uniqued_gen = uniquify_generator(gencopy)
        self.assertEqual(range(5) + range(5), list(gen))
        self.assertEqual(range(5), list(uniqued_gen))

    def test_tilequeue_intersect_removes_expired_list_file(self):
        from mock import MagicMock
        from tilequeue.command import tilequeue_intersect
        import os
        import shutil
        cfg_mock = MagicMock()
        cfg_mock.queue_type = 'sqs'
        periperals_mock = MagicMock()
        sample_file = os.getcwd() + \
            "/tests/fixtures/sample_expire_list"
        expected_file = '/tmp/expire_list.txt'
        shutil.copy2(sample_file, expected_file)
        cfg_mock.expired_tiles_file = expected_file
        cfg_mock.logconfig = None
        tilequeue_intersect(cfg_mock, periperals_mock)
        self.assertFalse(os.path.isfile(expected_file))

    def test_tilequeue_intersect_does_not_enqueue_coords(self):
        from mock import MagicMock
        from tilequeue.command import tilequeue_intersect
        from ModestMaps.Core import Coordinate
        from tilequeue.tile import serialize_coord
        cfg_mock = MagicMock()
        cfg_mock.queue_type = 'sqs'
        periperals_mock = MagicMock()
        c0 = Coordinate(row=0, column=0, zoom=0)
        c1 = Coordinate(row=1, column=1, zoom=1)
        periperals_mock.redis_cache_index = \
            MagicMock(get_list=lambda: ())
        queue_mock = MagicMock()
        periperals_mock.queue = queue_mock
        queue_mock.enqueue = self.fake_enqueue
        expected_file = '/tmp/expire_list.txt'
        with open(expected_file, "w+") as fp:
            fp.write(serialize_coord(c0) + "\n" + serialize_coord(c1))
        cfg_mock.expired_tiles_file = expected_file
        cfg_mock.logconfig = None
        tilequeue_intersect(cfg_mock, periperals_mock)
        self.assertNotIn(c0, self.enqueued_list)
        self.assertNotIn(c1, self.enqueued_list)

    def test_tilequeue_intersect_enqueues_coords(self):
        from mock import MagicMock
        from tilequeue.command import tilequeue_intersect
        from ModestMaps.Core import Coordinate
        from tilequeue.cache import serialize_coord_to_redis_value
        from tilequeue.tile import serialize_coord
        cfg_mock = MagicMock()
        cfg_mock.queue_type = 'sqs'
        periperals_mock = MagicMock()
        c0 = Coordinate(row=0, column=0, zoom=0)
        c1 = Coordinate(row=1, column=1, zoom=1)
        redis_coords = [serialize_coord_to_redis_value(c0),
                        serialize_coord_to_redis_value(c1)]
        periperals_mock.redis_cache_index = \
            MagicMock(get_list=lambda: ([str(redis_coords[0]),
                                         str(redis_coords[1])]))
        queue_mock = MagicMock()
        periperals_mock.queue = queue_mock
        queue_mock.enqueue = self.fake_enqueue
        expected_file = '/tmp/expire_list.txt'
        with open(expected_file, "w+") as fp:
            fp.write(serialize_coord(c0) + "\n" + serialize_coord(c1))
        cfg_mock.expired_tiles_file = expected_file
        cfg_mock.logconfig = None
        tilequeue_intersect(cfg_mock, periperals_mock)
        self.assertIn(c0, self.enqueued_list)
        self.assertIn(c1, self.enqueued_list)
