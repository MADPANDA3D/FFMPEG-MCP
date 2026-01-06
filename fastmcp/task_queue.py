from rq import Queue

from config import settings
from redis_store import get_redis


_queue: Queue | None = None


def get_queue() -> Queue:
    global _queue
    if _queue is None:
        _queue = Queue(settings.queue_name, connection=get_redis())
    return _queue
