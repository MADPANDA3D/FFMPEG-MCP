from rq import Queue

from config import settings
from redis_store import get_redis


_queues: dict[str, Queue] = {}


def _queue_name_for_priority(priority: str | None) -> str:
    if not priority:
        return settings.queue_name
    priority = priority.lower()
    if priority == "urgent" and settings.queue_name_urgent:
        return settings.queue_name_urgent
    if priority == "batch" and settings.queue_name_batch:
        return settings.queue_name_batch
    return settings.queue_name


def get_queue(priority: str | None = None) -> Queue:
    name = _queue_name_for_priority(priority)
    queue = _queues.get(name)
    if queue is None:
        queue = Queue(name, connection=get_redis())
        _queues[name] = queue
    return queue
