import unittest


class TestCoordsByParent(unittest.TestCase):

    def test_empty(self):
        from tilequeue.utils import CoordsByParent

        cbp = CoordsByParent(10)

        count = 0
        for key, coords in cbp:
            count += 1

        self.assertEqual(0, count)

    def test_lower_zooms_not_grouped(self):
        from tilequeue.utils import CoordsByParent
        from ModestMaps.Core import Coordinate

        cbp = CoordsByParent(10)

        low_zoom_coords = [(9, 0, 0), (9, 0, 1), (9, 1, 0), (9, 1, 1)]
        for z, x, y in low_zoom_coords:
            coord = Coordinate(zoom=z, column=x, row=y)
            cbp.add(coord)

        count = 0
        for key, coords in cbp:
            self.assertEqual(1, len(coords))
            count += 1

        self.assertEqual(len(low_zoom_coords), count)

    def test_higher_zooms_grouped(self):
        from tilequeue.utils import CoordsByParent
        from ModestMaps.Core import Coordinate

        cbp = CoordsByParent(10)

        def _c(z, x, y):
            return Coordinate(zoom=z, column=x, row=y)

        groups = {
            _c(10, 0, 0): [_c(10, 0, 0), _c(11, 0, 0), _c(11, 0, 1)],
            _c(10, 1, 1): [_c(11, 2, 2), _c(11, 3, 3), _c(12, 4, 4)],
        }

        for coords in groups.values():
            for coord in coords:
                cbp.add(coord)

        count = 0
        for key, coords in cbp:
            self.assertIn(key, groups)
            self.assertEqual(set(groups[key]), set(coords))
            count += 1

        self.assertEqual(len(groups), count)

    def test_with_extra_data(self):
        from tilequeue.utils import CoordsByParent
        from ModestMaps.Core import Coordinate

        cbp = CoordsByParent(10)

        coord = Coordinate(zoom=10, column=0, row=0)
        cbp.add(coord, 'foo', 'bar')

        count = 0
        for key, coords in cbp:
            self.assertEqual(1, len(coords))
            self.assertEqual((coord, 'foo', 'bar'), coords[0])
            count += 1

        self.assertEqual(1, count)
