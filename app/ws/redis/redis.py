from functools import lru_cache
import redis
from app.config import get_settings

from app.config.model import redis_cache

class RedisStorage(object):

    def __init__(self, connection: redis_cache.RedisConnection=None):
        self.connection = connection
        if not connection:
            self.connection = get_settings().redis_cache.connection
        # host = self.connection.redis_host
        # password = self.connection.redis_password
        # port = self.connection.redis_port
        # db = self.connection.redis_db
        # redis = redis.Redis(host=host, password=password, port=port, db=db)

        
    def get_redis(self, readonly: bool=False):

        rs = get_settings().redis_cache.connection
        if rs.connection_type == "redis":
            rc = rs.redis_connection
            # host = f"redis://:{rs.redis_password}@{rc}:{rc.redis_port}/{rs.redis_db}"
            master: redis.Redis = redis.Redis(host=rc.redis_host, password=rs.redis_password, port=rc.redis_port, db=rs.redis_db)
        else:
            sc = rs.sentinel_connection
            sentinel_hosts = [(host.name, host.port) for host in sc.hosts]
            sentinel = redis.Sentinel(sentinel_hosts, sentinel_kwargs={'password': rs.redis_password})
            if readonly:
                master: redis.Redis = sentinel.slave_for(rs.sentinel_connection.master_name, password=rs.redis_password, db=rs.redis_db)
            else:
                master: redis.Redis = sentinel.master_for(rs.sentinel_connection.master_name, password=rs.redis_password, db=rs.redis_db)
        return master
    
    def set_value_with_expiration_time(self, key, value, expiration_time):
        redis = self.get_redis()
        return redis.set(key, value, exat=expiration_time)
    
    def set_value(self, key, value, ex=None):
        redis = self.get_redis()
        if ex:
            return redis.set(key, value, ex=ex)
        return redis.set(key, value)

    def is_key_in_store(self, key):
        redis = self.get_redis(readonly=True)
        value = redis.get(key)
        if value:
            return True
        return False
    def get_value(self, key):
        redis = self.get_redis(readonly=True)
        value = redis.get(key)
        return value
    
    def search_keys(self, pattern):
        redis = self.get_redis(readonly=True)
        value = redis.keys(pattern)
        return value
    
    def delete_value(self, key):
        redis = self.get_redis()
        redis.delete(key)
           
@lru_cache(1)
def get_redis_server() -> RedisStorage:
    return RedisStorage()