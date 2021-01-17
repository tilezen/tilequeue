import unittest
import tempfile
from tilequeue.tile import deserialize_coord, coord_marshall_int


class TestToiSet(unittest.TestCase):
    def _coord_str_to_int(self, coord_str):
        coord = deserialize_coord(coord_str)
        coord_int = coord_marshall_int(coord)
        return coord_int

    def test_save_set_to_fp(self):
        from tilequeue.toi import save_set_to_fp

        toi_set = set()
        toi_set.add(self._coord_str_to_int('0/0/0'))
        toi_set.add(self._coord_str_to_int('1/0/0'))
        toi_set.add(self._coord_str_to_int('1/1/0'))

        with tempfile.TemporaryFile() as fp:
            save_set_to_fp(toi_set, fp)

            self.assertEquals(fp.tell(), 18)

            fp.seek(0)
            self.assertEquals(fp.read(), '0/0/0\n1/0/0\n1/1/0\n')

    def test_load_set_from_fp(self):
        from tilequeue.toi import load_set_from_fp

        with tempfile.TemporaryFile() as fp:
            fp.write('0/0/0\n1/0/0\n1/1/0\n')
            fp.seek(0)

            actual_toi_set = load_set_from_fp(fp)
            expected_toi_set = set()
            expected_toi_set.add(self._coord_str_to_int('0/0/0'))
            expected_toi_set.add(self._coord_str_to_int('1/0/0'))
            expected_toi_set.add(self._coord_str_to_int('1/1/0'))

            self.assertEquals(expected_toi_set, actual_toi_set)

    def test_load_set_from_fp_accidental_dupe(self):
        from tilequeue.toi import load_set_from_fp

        with tempfile.TemporaryFile() as fp:
            fp.write('0/0/0\n1/0/0\n1/1/0\n1/0/0\n')
            fp.seek(0)

            actual_toi_set = load_set_from_fp(fp)
            expected_toi_set = set()
            expected_toi_set.add(self._coord_str_to_int('0/0/0'))
            expected_toi_set.add(self._coord_str_to_int('1/0/0'))
            expected_toi_set.add(self._coord_str_to_int('1/1/0'))

            self.assertEquals(expected_toi_set, actual_toi_set)

    def test_save_set_to_gzipped_fp(self):
        import gzip
        from tilequeue.toi import save_set_to_gzipped_fp

        toi_set = set()
        toi_set.add(self._coord_str_to_int('0/0/0'))
        toi_set.add(self._coord_str_to_int('1/0/0'))
        toi_set.add(self._coord_str_to_int('1/1/0'))

        with tempfile.TemporaryFile() as fp:
            save_set_to_gzipped_fp(toi_set, fp)

            self.assertEquals(fp.tell(), 31)

            fp.seek(0)
            with gzip.GzipFile(fileobj=fp, mode='r') as gz:
                self.assertEquals(gz.read(), '0/0/0\n1/0/0\n1/1/0\n')

    def test_load_set_from_gzipped_fp(self):
        import gzip
        from tilequeue.toi import load_set_from_gzipped_fp

        with tempfile.TemporaryFile() as fp:
            with gzip.GzipFile(fileobj=fp, mode='w') as gz:
                gz.write('0/0/0\n1/0/0\n1/1/0\n')
            fp.seek(0)

            actual_toi_set = load_set_from_gzipped_fp(fp)
            expected_toi_set = set()
            expected_toi_set.add(self._coord_str_to_int('0/0/0'))
            expected_toi_set.add(self._coord_str_to_int('1/0/0'))
            expected_toi_set.add(self._coord_str_to_int('1/1/0'))

            self.assertEquals(expected_toi_set, actual_toi_set)
