import unittest


class TestRedisCacheIndex(unittest.TestCase):

    def test_get_list(self):
        from mock import MagicMock
        from tilequeue.cache import RedisCacheIndex
        redis_client_mock = MagicMock()
        redis_cache_index = RedisCacheIndex(redis_client_mock)
        redis_cache_index.get_list()
        redis_client_mock.smembers.assert_called_once_with(
            redis_cache_index.cache_set_key)
