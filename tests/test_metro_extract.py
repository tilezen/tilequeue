import unittest


class TestMetroExtractParse(unittest.TestCase):

    def _call_fut(self, fp):
        from tilequeue.metro_extract import parse_metro_extract
        return parse_metro_extract(fp)

    def test_invalid_json(self):
        from io import BytesIO
        from tilequeue.metro_extract import MetroExtractParseError
        fp = BytesIO(b'{"foo": "bar"}')
        try:
            self._call_fut(fp)
        except MetroExtractParseError:
            # expecting error to be raised
            pass
        else:
            self.fail('Expected MetroExtractParseError to be raised')

    def _generate_stub(self):
        return dict(
            regions=dict(
                region1=dict(
                    cities=dict(
                        city1=self._city_bounds(1, 1, 2, 2),
                        city2=self._city_bounds(3, 3, 4, 4),
                    )
                )
            )
        )

    def _city_bounds(self, minx, miny, maxx, maxy):
        return dict(
            bbox=dict(
                left=str(minx),
                right=str(maxx),
                top=str(maxy),
                bottom=str(miny),
            )
        )

    def test_valid_parse(self):
        from json import dumps
        stub = self._generate_stub()
        from io import StringIO
        fp = StringIO(dumps(stub))
        results = self._call_fut(fp)
        self.assertEqual(2, len(results))
        results.sort(key=lambda x: x.city)
        city1, city2 = results

        self.assertEqual('region1', city1.region)
        self.assertEqual('city1', city1.city)
        self.assertEqual((1, 1, 2, 2), city1.bounds)

        self.assertEqual('region1', city2.region)
        self.assertEqual('city2', city2.city)
        self.assertEqual((3, 3, 4, 4), city2.bounds)

    def test_city_bounds(self):
        from json import dumps
        stub = self._generate_stub()
        from io import StringIO
        fp = StringIO(dumps(stub))
        results = self._call_fut(fp)
        self.assertEqual(2, len(results))
        results.sort(key=lambda x: x.city)

        from tilequeue.metro_extract import city_bounds
        bounds = city_bounds(results)
        self.assertEqual(2, len(bounds))
        bounds1, bounds2 = bounds
        self.assertEqual((1, 1, 2, 2), bounds1)
        self.assertEqual((3, 3, 4, 4), bounds2)
