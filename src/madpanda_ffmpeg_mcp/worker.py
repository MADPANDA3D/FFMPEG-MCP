import logging

from rq import Worker

from .config import settings
from .redis_store import get_rq_redis


def main() -> None:
    settings.validate_worker_runtime()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    worker = Worker(settings.queue_names(), connection=get_rq_redis())
    worker.work()


if __name__ == "__main__":
    main()
