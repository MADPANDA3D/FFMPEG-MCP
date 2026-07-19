import asyncio
import logging
import os
import stat
import time

from .config import settings
from .redis_store import delete_job, list_expired_assets, list_expired_jobs
from .storage import delete_managed_asset

logger = logging.getLogger("ffmpeg_mcp.cleanup")


def cleanup_stale_staging_files(*, now: float | None = None) -> int:
    """Remove stale regular files directly inside the staging root."""

    cutoff = (time.time() if now is None else now) - settings.storage_staging_max_age_seconds
    open_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
    try:
        root_fd = os.open(settings.storage_temp_dir, open_flags)
    except FileNotFoundError:
        return 0
    except OSError:
        logger.exception("staging_cleanup_root_unavailable")
        return 0

    deleted = 0
    try:
        with os.scandir(root_fd) as entries:
            for entry in entries:
                try:
                    metadata = entry.stat(follow_symlinks=False)
                    if not stat.S_ISREG(metadata.st_mode) or metadata.st_mtime >= cutoff:
                        continue
                    os.unlink(entry.name, dir_fd=root_fd)
                    deleted += 1
                except FileNotFoundError:
                    continue
                except OSError:
                    logger.exception("staging_cleanup_file_failed")
    except OSError:
        logger.exception("staging_cleanup_scan_failed")
    finally:
        os.close(root_fd)
    return deleted


def cleanup_once() -> None:
    cleanup_stale_staging_files()

    for asset_id in list_expired_assets():
        try:
            if not delete_managed_asset(asset_id):
                logger.warning("cleanup_deferred asset_id=%s", asset_id)
        except Exception:
            logger.exception("cleanup_failed asset_id=%s", asset_id)

    for job_id in list_expired_jobs():
        try:
            delete_job(job_id)
        except Exception:
            logger.exception("job_cleanup_failed job_id=%s", job_id)


async def cleanup_loop() -> None:
    interval = max(settings.cleanup_interval_seconds, 60)
    while True:
        try:
            await asyncio.to_thread(cleanup_once)
        except Exception:
            logger.exception("cleanup_loop_error")
        await asyncio.sleep(interval)
