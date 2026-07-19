import os
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from madpanda_ffmpeg_mcp import storage


def _settings(root: str, temp_dir: str):
    return SimpleNamespace(
        storage_backend="local",
        storage_local_dir=root,
        storage_temp_dir=temp_dir,
        public_base_url="https://downloads.example.invalid",
        download_signing_secret="test-signing-secret-00000000000000",
        download_url_ttl_seconds=60,
        storage_asgi_operation_timeout_seconds=5,
        job_storage_max_materialize_bytes=1_024,
    )


class StoragePathSecurityTests(unittest.TestCase):
    def test_valid_nested_key_resolves_under_storage_root(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = os.path.join(temp_dir, "assets")
            os.makedirs(root)
            with patch.object(storage, "settings", _settings(root, temp_dir)):
                resolved = storage.local_path_from_key("ab/cd/asset.mp4")
            self.assertEqual(resolved, os.path.join(root, "ab", "cd", "asset.mp4"))

    def test_traversal_absolute_and_ambiguous_keys_fail_closed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = os.path.join(temp_dir, "assets")
            os.makedirs(root)
            with patch.object(storage, "settings", _settings(root, temp_dir)):
                for key in [
                    "../outside",
                    "aa/../../outside",
                    "/etc/passwd",
                    "aa//asset.mp4",
                    "aa/./asset.mp4",
                    "aa\\asset.mp4",
                    "aa/asset\x00.mp4",
                ]:
                    with (
                        self.subTest(key=key),
                        self.assertRaises(storage.StorageError),
                    ):
                        storage.local_path_from_key(key)

    def test_parent_and_leaf_symlinks_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = os.path.join(temp_dir, "assets")
            outside = os.path.join(temp_dir, "outside")
            os.makedirs(os.path.join(root, "aa"))
            os.makedirs(outside)
            outside_file = os.path.join(outside, "secret.txt")
            with open(outside_file, "wb") as handle:
                handle.write(b"secret")

            os.symlink(outside, os.path.join(root, "aa", "bb"))
            os.symlink(outside_file, os.path.join(root, "leaf"))
            with patch.object(storage, "settings", _settings(root, temp_dir)):
                with self.assertRaisesRegex(storage.StorageError, "Symlinks"):
                    storage.local_path_from_key("aa/bb/secret.txt")
                with self.assertRaisesRegex(storage.StorageError, "Symlinks"):
                    storage.download_to_temp("leaf")

    def test_delete_refuses_symlink_without_touching_target(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = os.path.join(temp_dir, "assets")
            os.makedirs(root)
            outside_file = os.path.join(temp_dir, "outside.txt")
            with open(outside_file, "wb") as handle:
                handle.write(b"keep")
            os.symlink(outside_file, os.path.join(root, "linked.txt"))

            with (
                patch.object(storage, "settings", _settings(root, temp_dir)),
                self.assertRaises(storage.StorageError),
            ):
                storage.delete_file("linked.txt")
            self.assertTrue(os.path.exists(outside_file))

    def test_persist_asset_rejects_symlink_before_reserve_or_commit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = os.path.join(temp_dir, "assets")
            stage = os.path.join(temp_dir, "stage")
            os.makedirs(root)
            os.makedirs(stage)
            source = os.path.join(temp_dir, "source.mp4")
            linked = os.path.join(temp_dir, "linked.mp4")
            with open(source, "wb") as handle:
                handle.write(b"media")
            os.symlink(source, linked)

            with (
                patch.object(storage, "settings", _settings(root, stage)),
                patch.object(storage, "reserve_asset") as reserve_asset,
                patch.object(storage, "commit_asset") as commit_asset,
                self.assertRaisesRegex(storage.StorageError, "non-symlink"),
            ):
                storage.persist_asset(
                    linked,
                    {
                        "asset_id": "a" * 32,
                        "expires_at": int(time.time()) + 60,
                    },
                    ".mp4",
                )
            reserve_asset.assert_not_called()
            commit_asset.assert_not_called()

    def test_s3_stream_is_bounded_and_partial_file_is_removed(self):
        class Body:
            def __init__(self):
                self.chunks = iter((b"1234", b"5678"))
                self.closed = False

            def read(self, _size):
                return next(self.chunks, b"")

            def close(self):
                self.closed = True

        with tempfile.TemporaryDirectory() as temp_dir:
            body = Body()
            client = SimpleNamespace(
                get_object=lambda **_kwargs: {"Body": body, "ContentLength": 4}
            )
            configured = SimpleNamespace(
                storage_backend="s3",
                storage_local_dir=temp_dir,
                storage_temp_dir=temp_dir,
                s3_bucket="bucket",
                max_ingest_bytes=6,
                max_output_bytes=6,
                storage_asgi_operation_timeout_seconds=5,
                job_storage_max_materialize_bytes=6,
            )
            with (
                patch.object(storage, "settings", configured),
                patch.object(storage, "get_storage_client", return_value=client),
                self.assertRaisesRegex(storage.StorageError, "size limit"),
            ):
                storage.download_to_temp("aa/bb/media.mp4")

            self.assertTrue(body.closed)
            self.assertEqual(os.listdir(temp_dir), [])

    def test_s3_content_length_is_rejected_before_writing(self):
        body = SimpleNamespace(close=lambda: None)
        client = SimpleNamespace(get_object=lambda **_kwargs: {"Body": body, "ContentLength": 7})
        with tempfile.TemporaryDirectory() as temp_dir:
            configured = SimpleNamespace(
                storage_backend="s3",
                storage_local_dir=temp_dir,
                storage_temp_dir=temp_dir,
                s3_bucket="bucket",
                max_ingest_bytes=6,
                max_output_bytes=6,
                storage_asgi_operation_timeout_seconds=5,
                job_storage_max_materialize_bytes=6,
            )
            with (
                patch.object(storage, "settings", configured),
                patch.object(storage, "get_storage_client", return_value=client),
                self.assertRaisesRegex(storage.StorageError, "size limit"),
            ):
                storage.download_to_temp("aa/bb/media.mp4")
            self.assertEqual(os.listdir(temp_dir), [])


if __name__ == "__main__":
    unittest.main()
