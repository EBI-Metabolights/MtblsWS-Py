import datetime
import hashlib
from typing import Set

import redis

from app.config import get_settings
from app.utils import current_time
from app.ws.redis.redis import get_redis_server


def generate_random_name(length=4, current_names: Set[str] = None):
    current_datetime = str(current_time())
    # Update the hash object with the datetime string
    name = None
    sha1 = hashlib.sha1()
    while True:
        sha1.update(current_datetime.encode())
        sha1_hash = sha1.hexdigest()
        name = sha1_hash[:length]
        if not current_names or name not in current_names:
            return name


def check_and_get_monitor_session(key: str, timeout: int):
    redis = get_redis_server()

    monitor_status = redis.get_value(key)
    if monitor_status and monitor_status.decode() == "1":
        return False
    redis.set_value(key, "1", ex=timeout)
    return True
