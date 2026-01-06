import asyncio
import logging

from config import settings
from redis_store import (
    delete_asset,
    delete_job,
    get_asset,
    list_expired_assets,
    list_expired_jobs,
)
from storage import delete_file

logger = logging.getLogger("ffmpeg_mcp.cleanup")


async def cleanup_loop() -> None:
    interval = max(settings.cleanup_interval_seconds, 60)
    while True:
        try:
            expired_assets = list_expired_assets()
            for asset_id in expired_assets:
                asset = get_asset(asset_id)
                if asset and asset.get("storage_key"):
                    try:
                        delete_file(asset["storage_key"])
                    except Exception:
                        logger.warning("cleanup_failed asset_id=%s", asset_id)
                delete_asset(asset_id)

            expired_jobs = list_expired_jobs()
            for job_id in expired_jobs:
                delete_job(job_id)
        except Exception:
            logger.exception("cleanup_loop_error")

        await asyncio.sleep(interval)
