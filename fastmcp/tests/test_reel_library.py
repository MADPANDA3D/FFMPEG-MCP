import hashlib
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import reel_library


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.sets = {}
        self.lists = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, ex=None):
        self.values[key] = value

    def delete(self, key):
        self.values.pop(key, None)

    def sadd(self, key, value):
        self.sets.setdefault(key, set()).add(value)

    def smembers(self, key):
        return self.sets.get(key, set())

    def llen(self, key):
        return len(self.lists.get(key, []))

    def rpush(self, key, value):
        self.lists.setdefault(key, []).append(value)

    def lrange(self, key, start, end):
        values = self.lists.get(key, [])
        return values[start:] if end == -1 else values[start : end + 1]


class ReelLibraryTests(unittest.TestCase):
    def setUp(self):
        self.redis = FakeRedis()
        self.temp = tempfile.TemporaryDirectory()
        self.settings = SimpleNamespace(
            storage_temp_dir=self.temp.name,
            public_base_url="https://ffmpeg.example.invalid",
        )
        self.redis_patch = patch.object(reel_library, "get_redis", return_value=self.redis)
        self.settings_patch = patch.object(reel_library, "settings", self.settings)
        self.redis_patch.start()
        self.settings_patch.start()

    def tearDown(self):
        self.settings_patch.stop()
        self.redis_patch.stop()
        self.temp.cleanup()

    def test_direct_upload_verifies_and_deduplicates_sha256(self):
        payload = b"synthetic-safe-media"
        digest = hashlib.sha256(payload).hexdigest()
        begun = reel_library.begin_upload("sample.mp4", "video/mp4", len(payload), digest)
        session = reel_library.get_upload(begun["upload_id"])
        with open(session["temp_path"], "wb") as handle:
            handle.write(payload)

        def promote(path, clip_id, ext):
            os.remove(path)
            return f"aa/{clip_id}{ext}", f"local://aa/{clip_id}{ext}", len(payload)

        with patch.object(reel_library, "put_file", side_effect=promote):
            first = reel_library.complete_upload(
                begun["upload_id"],
                {"topic": "Hostinger", "tags": ["approved", "ui-proof"]},
            )
        self.assertFalse(first["deduplicated"])
        self.assertEqual(first["clip"]["sha256"], digest)
        self.assertNotIn("storage_key", first["clip"])

        begun_again = reel_library.begin_upload("copy.mp4", "video/mp4", len(payload), digest)
        again_session = reel_library.get_upload(begun_again["upload_id"])
        with open(again_session["temp_path"], "wb") as handle:
            handle.write(payload)
        duplicate = reel_library.complete_upload(begun_again["upload_id"])
        self.assertTrue(duplicate["deduplicated"])
        self.assertEqual(duplicate["clip"]["clip_id"], first["clip"]["clip_id"])

    def test_template_versions_are_immutable_and_latest_is_default(self):
        first = reel_library.save_template("hostinger_reel", {"headline": "v1"}, "initial")
        second = reel_library.save_template("hostinger_reel", {"headline": "v2"}, "approved")
        self.assertEqual(first["version"], 1)
        self.assertEqual(second["version"], 2)
        self.assertEqual(reel_library.get_template("hostinger_reel", 1)["definition"]["headline"], "v1")
        self.assertEqual(reel_library.get_template("hostinger_reel")["version"], 2)

    def test_reel_contract_enforces_duration_and_required_tracks(self):
        reel_library.save_template(
            "madpanda_vertical_reel",
            {"kind": "madpanda_vertical_reel"},
            "approved synthetic QA template",
        )
        valid = {
            "duration_sec": 35,
            "shots": [{"clip_id": "clip_a", "start_sec": 0, "duration_sec": 29.5}],
            "narration_clip_id": "clip_voice",
            "music_clip_id": "clip_music",
            "outro_clip_id": "clip_outro",
            "outro_duration_sec": 5.5,
            "template_id": "madpanda_vertical_reel",
            "template_version": 1,
            "quality": "high",
        }
        self.assertTrue(reel_library.validate_reel_plan(valid)["ok"])
        invalid = {**valid, "duration_sec": 29, "outro_clip_id": ""}
        result = reel_library.validate_reel_plan(invalid)
        self.assertFalse(result["ok"])
        self.assertEqual(len(result["input_fingerprint"]), 64)

        mismatched = {
            **valid,
            "shots": [{"clip_id": "clip_a", "duration_sec": 20}],
        }
        self.assertIn(
            "shot durations plus outro_duration_sec must equal duration_sec",
            reel_library.validate_reel_plan(mismatched)["errors"],
        )


if __name__ == "__main__":
    unittest.main()
