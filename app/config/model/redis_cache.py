from typing import List, Union
from pydantic import BaseModel


class Host(BaseModel):
    name: str
    port: int
    
class SentinelConnection(BaseModel):
    hosts: List[Host]
    master_name: str = "master-redis-ws"

class StandaloneRedisConnection(BaseModel):
    redis_host: str
    redis_port: int
    

class RedisConnection(BaseModel):
    connection_type: str = "redis"
    redis_password: str
    redis_db: int
    redis_connection: Union[None, StandaloneRedisConnection] = None
    sentinel_connection: Union[None, SentinelConnection] = None


class RedisConfiguration(BaseModel):
    banner_message_key: str = "metabolights:banner:message"
    species_tree_cache_key: str = "metabolights:species:tree"
    study_folder_maintenance_mode_key_prefix: str = "metabolights:maintenance:mode"


class RedisSettings(BaseModel):
    connection: RedisConnection
    configuration: RedisConfiguration = RedisConfiguration()
