import unittest


class SingleQueueMapperTest(unittest.TestCase):

    def make_queue_mapper(self, queue_name, tile_queue):
        from tilequeue.queue.mapper import SingleQueueMapper
        return SingleQueueMapper(queue_name, tile_queue)

    def test_single_queue_mapper(self):
        from mock import MagicMock
        from tilequeue.tile import deserialize_coord
        tile_queue_mock = MagicMock()
        queue_mapper = self.make_queue_mapper('queue_name', tile_queue_mock)
        coords = [deserialize_coord('1/1/1'), deserialize_coord('15/1/1')]
        coord_groups = list(queue_mapper.group(coords))
        self.assertEquals(2, len(coord_groups))
        cg1, cg2 = coord_groups
        self.assertEquals('queue_name', cg1.queue_id)
        self.assertEquals('queue_name', cg2.queue_id)
        self.assertEquals(1, len(cg1.coords))
        self.assertEquals(coords[0], cg1.coords[0])
        self.assertEquals(1, len(cg2.coords))
        self.assertEquals(coords[1], cg2.coords[0])

        self.assertIs(tile_queue_mock, queue_mapper.get_queue('queue_name'))

        qs = queue_mapper.queues_in_priority_order()
        self.assertEquals(1, len(qs))
        qn, q = qs[0]
        self.assertEquals('queue_name', qn)
        self.assertIs(tile_queue_mock, q)


class MultipleQueueMapperTest(unittest.TestCase):

    def make_queue_mapper(self, specs):
        from tilequeue.queue.mapper import ZoomRangeAndZoomGroupQueueMapper
        from tilequeue.queue.mapper import ZoomRangeQueueSpec
        zoom_range_specs = []
        for zs, ze, qn, tq, gbz in specs:
            zrs = ZoomRangeQueueSpec(zs, ze, qn, tq, gbz)
            zoom_range_specs.append(zrs)
        qm = ZoomRangeAndZoomGroupQueueMapper(zoom_range_specs)
        return qm

    def test_group_coords(self):
        from tilequeue.tile import deserialize_coord
        specs = (
            (0, 10, 'tile_queue_1', object(), None),
            (10, 16, 'tile_queue_2', object(), 10),
        )
        qm = self.make_queue_mapper(specs)
        coord_strs = (
            '1/1/1',
            '9/0/0',
            '10/0/0',
            '14/65536/65536',
            '15/0/0',
        )
        coords = map(deserialize_coord, coord_strs)
        coord_groups = list(qm.group(coords))
        assert len(coord_groups) == 4

        cg1, cg2, cg3, cg4 = coord_groups

        lo_zoom_queue_id = 0
        hi_zoom_queue_id = 1

        # low zooms are grouped separately
        self.assertEquals(1, len(cg1.coords))
        self.assertEquals([deserialize_coord('1/1/1')], cg1.coords)
        self.assertEquals(lo_zoom_queue_id, cg1.queue_id)

        self.assertEquals(1, len(cg2.coords))
        self.assertEquals([deserialize_coord('9/0/0')], cg2.coords)
        self.assertEquals(lo_zoom_queue_id, cg2.queue_id)

        # common z10 parents are grouped together
        self.assertEquals(2, len(cg3.coords))
        self.assertEquals(map(deserialize_coord, ['10/0/0', '15/0/0']),
                          cg3.coords)
        self.assertEquals(hi_zoom_queue_id, cg3.queue_id)

        # different z10 parent grouped separately, even though it
        # should be sent to the same queue
        self.assertEquals(1, len(cg4.coords))
        self.assertEquals([deserialize_coord('14/65536/65536')], cg4.coords)
        self.assertEquals(hi_zoom_queue_id, cg4.queue_id)

    def test_group_coord_out_of_range(self):
        from tilequeue.tile import deserialize_coord
        specs = (
            (0, 10, 'tile_queue_1', object(), None),
            (10, 16, 'tile_queue_2', object(), 10),
        )
        qm = self.make_queue_mapper(specs)

        coords = [deserialize_coord('20/0/0')]
        coord_groups = list(qm.group(coords))
        self.assertEquals(0, len(coord_groups))

        coords = map(deserialize_coord, ['20/0/0', '1/1/1', '16/0/0'])
        coord_groups = list(qm.group(coords))
        self.assertEquals(1, len(coord_groups))
        self.assertEquals([deserialize_coord('1/1/1')], coord_groups[0].coords)
        self.assertEquals(0, coord_groups[0].queue_id)

    def test_queue_mappings(self):
        q1 = object()
        q2 = object()
        q3 = object()
        specs = (
            (None, None, 'tile_queue_1', q1, None),
            (0, 10, 'tile_queue_2', q2, None),
            (10, 16, 'tile_queue_3', q3, 10),
        )
        qm = self.make_queue_mapper(specs)

        q1_id, q2_id, q3_id = range(3)
        self.assertIs(q1, qm.get_queue(q1_id))
        self.assertIs(q2, qm.get_queue(q2_id))
        self.assertIs(q3, qm.get_queue(q3_id))

        ordered_queue_result = list(qm.queues_in_priority_order())
        self.assertEquals(3, len(ordered_queue_result))
        r1_id, r1_q = ordered_queue_result[0]
        r2_id, r2_q = ordered_queue_result[1]
        r3_id, r3_q = ordered_queue_result[2]
        self.assertIs(q1, r1_q)
        self.assertEquals(q1_id, r1_id)
        self.assertIs(q2, r2_q)
        self.assertEquals(q2_id, r2_id)
        self.assertIs(q3, r3_q)
        self.assertEquals(q3_id, r3_id)

        from tilequeue.tile import deserialize_coord

        # verify that the queue ids line up with those that have zooms
        # specified
        coord_groups = list(qm.group([deserialize_coord('5/0/0')]))
        self.assertEquals(1, len(coord_groups))
        self.assertEquals(1, coord_groups[0].queue_id)

        coord_groups = list(qm.group([deserialize_coord('15/0/0')]))
        self.assertEquals(1, len(coord_groups))
        self.assertEquals(2, coord_groups[0].queue_id)

    def test_toi_priority(self):
        from tilequeue.queue.mapper import ZoomRangeAndZoomGroupQueueMapper
        from tilequeue.queue.mapper import ZoomRangeQueueSpec
        from tilequeue.tile import create_coord

        specs = [
            ZoomRangeQueueSpec(0, 10, 'q1', object(), None, in_toi=True),
            ZoomRangeQueueSpec(0, 10, 'q2', object(), None, in_toi=False),
        ]

        coord_in_toi = create_coord(1, 1, 1)
        coord_not_in_toi = create_coord(2, 2, 2)

        class FakeToi(object):
            def __init__(self, toi):
                self.toi = toi

            def fetch_tiles_of_interest(self):
                return self.toi

        toi = FakeToi(set([coord_in_toi]))
        mapper = ZoomRangeAndZoomGroupQueueMapper(specs, toi=toi)

        for coord in (coord_in_toi, coord_not_in_toi):
            group = list(mapper.group([coord]))
            self.assertEquals(1, len(group))
            self.assertEquals(coord == coord_in_toi, group[0].queue_id == 0)
