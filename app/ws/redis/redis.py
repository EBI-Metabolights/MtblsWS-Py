from functools import lru_cache
import redis

from app.ws.settings.redis import RedisSettings

class RedisStorage(object):

    def __init__(self):
        settings = RedisSettings()
        host = settings.redis_host
        password = settings.redis_password
        port = settings.redis_port
        db = settings.redis_db
        self.redis = redis.Redis(host=host, password=password, port=port, db=db)

    def set_value_with_expiration_time(self, key, value, expiration_time):
        return self.redis.set(key, value, exat=expiration_time)
    
    def set_value(self, key, value, ex=None):
        if ex:
            return self.redis.set(key, value, ex=ex)
        return self.redis.set(key, value)

    def is_key_in_store(self, key):
        value = self.redis.get(key)
        if value:
            return True
        return False
    def get_value(self, key):
        value = self.redis.get(key)
        return value
    def delete_value(self, key):
        self.redis.delete(key)
           
@lru_cache(1)
def get_redis_server() -> RedisStorage:
    return RedisStorage()