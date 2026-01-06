import logging

from rq import Worker

from config import settings
from redis_store import get_redis


if __name__ == "__main__":
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    worker = Worker(settings.queue_names(), connection=get_redis())
    worker.work()
