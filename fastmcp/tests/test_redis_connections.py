import unittest

import redis_store


class RedisConnectionTests(unittest.TestCase):
    def tearDown(self) -> None:
        redis_store._redis_client = None
        redis_store._rq_redis_client = None

    def test_metadata_and_rq_connections_use_distinct_response_modes(self) -> None:
        metadata_client = redis_store.get_redis()
        rq_client = redis_store.get_rq_redis()

        self.assertTrue(metadata_client.get_encoder().decode_responses)
        self.assertFalse(rq_client.get_encoder().decode_responses)
        self.assertIsNot(metadata_client, rq_client)


if __name__ == "__main__":
    unittest.main()
