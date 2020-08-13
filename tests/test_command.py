import unittest


class tempdir(object):

    def __enter__(self):
        import tempfile
        self.tempdir = tempfile.mkdtemp()
        return self.tempdir

    def __exit__(self, type, value, traceback):
        import shutil
        shutil.rmtree(self.tempdir)


class TestUniquifyGenerator(unittest.TestCase):

    enqueued_list = set()

    def fake_enqueue(self, value):
        self.enqueued_list.add(value)

    def fake_enqueue_batch(self, values):
        n = 0
        for value in values:
            n += 1
            self.fake_enqueue(value)
        return n, 0

    def test_uniquify_generator(self):
        from tilequeue.command import uniquify_generator
        from itertools import cycle, islice, tee
        gen = islice(cycle(range(5)), 10)
        gen, gencopy = tee(gen)
        uniqued_gen = uniquify_generator(gencopy)
        self.assertEqual(range(5) + range(5), list(gen))
        self.assertEqual(range(5), list(uniqued_gen))

    def test_tilequeue_explode_and_intersect(self):
        from tilequeue.command import explode_and_intersect
        from tilequeue.tile import coord_marshall_int
        from tilequeue.tile import coord_unmarshall_int
        from ModestMaps.Core import Coordinate
        sample_coord = Coordinate(zoom=14, column=250, row=250)
        sample_coord_int = coord_marshall_int(sample_coord)
        tiles_of_interest = [sample_coord_int]
        for i in (10, 11, 12, 13):
            coord = sample_coord.zoomTo(i)
            coord_int = coord_marshall_int(coord)
            tiles_of_interest.append(coord_int)
        exploded, metrics = explode_and_intersect(
            [sample_coord_int], tiles_of_interest, until=11)
        coord_ints = list(exploded)
        for coord_int in coord_ints:
            coord = coord_unmarshall_int(coord_int)
            self.failUnless(coord.zoom > 10)

        self.assertEqual(4, len(coord_ints))

        self.assertEqual(4, metrics['hits'])
        self.assertEqual(0, metrics['misses'])
        self.assertEqual(4, metrics['total'])


class ZoomToQueueNameMapTest(unittest.TestCase):

    def test_bad_map(self):
        from tilequeue.command import make_get_queue_name_for_zoom
        zoom_queue_map_cfg = {'badzoom-20': 'q1'}
        queue_names = ['q1']
        with self.assertRaises(AssertionError):
            make_get_queue_name_for_zoom(zoom_queue_map_cfg, queue_names)

    def test_single_queue_name_for_zoom(self):
        from tilequeue.command import make_get_queue_name_for_zoom
        zoom_queue_map_cfg = {'0-20': 'q1'}
        queue_names = ['q1']
        get_queue = make_get_queue_name_for_zoom(
            zoom_queue_map_cfg, queue_names)
        zoom = 7
        queue_name = get_queue(zoom)
        self.assertEqual(queue_name, 'q1')

    def test_multiple_queues(self):
        from tilequeue.command import make_get_queue_name_for_zoom
        zoom_queue_map_cfg = {'0-5': 'q1', '6-20': 'q2'}
        queue_names = ['q1', 'q2']
        get_queue = make_get_queue_name_for_zoom(
            zoom_queue_map_cfg, queue_names)

        zoom = 5
        queue_name = get_queue(zoom)
        self.assertEqual(queue_name, 'q1')

        zoom = 15
        queue_name = get_queue(zoom)
        self.assertEqual(queue_name, 'q2')

    def test_missing_queue_name(self):
        from tilequeue.command import make_get_queue_name_for_zoom
        zoom_queue_map_cfg = {'0-5': 'q1', '6-20': 'q3'}
        queue_names = ['q1', 'q2']
        with self.assertRaises(AssertionError):
            make_get_queue_name_for_zoom(zoom_queue_map_cfg, queue_names)

    def test_overlapping_queue_names(self):
        from tilequeue.command import make_get_queue_name_for_zoom
        zoom_queue_map_cfg = {'0-5': 'q1', '4-20': 'q2'}
        queue_names = ['q1', 'q2']
        with self.assertRaises(AssertionError):
            make_get_queue_name_for_zoom(zoom_queue_map_cfg, queue_names)

    def test_zoom_invalid_lookup(self):
        from tilequeue.command import make_get_queue_name_for_zoom
        zoom_queue_map_cfg = {'0-20': 'q1'}
        queue_names = ['q1']
        get_queue = make_get_queue_name_for_zoom(
            zoom_queue_map_cfg, queue_names)
        zoom = 21
        with self.assertRaises(AssertionError):
            get_queue(zoom)

    def test_zoom_out_of_range(self):
        from tilequeue.command import make_get_queue_name_for_zoom
        zoom_queue_map_cfg = {'0-5': 'q1'}
        queue_names = ['q1']
        get_queue = make_get_queue_name_for_zoom(
            zoom_queue_map_cfg, queue_names)
        zoom = 7
        with self.assertRaises(AssertionError):
            get_queue(zoom)

    def test_zoom_is_long(self):
        # the zoom (or row/col) in a Coordinate can be a long simply because
        # the coordinate it was derived from in unmarshall_coord_int was a
        # long.
        from tilequeue.command import make_get_queue_name_for_zoom
        zoom_queue_map_cfg = {'0-20': 'q1'}
        queue_names = ['q1']
        get_queue = make_get_queue_name_for_zoom(
            zoom_queue_map_cfg, queue_names)
        zoom = long(7)
        queue_name = get_queue(zoom)
        self.assertEqual(queue_name, 'q1')
