from pydantic import BaseModel


class RedisConnection(BaseModel):
    redis_host: str
    redis_password: str
    redis_port: int
    redis_db: int


class RedisConfiguration(BaseModel):
    banner_message_key: str = "metabolights:banner:message"
    species_tree_cache_key: str = "metabolights:species:tree"
    study_folder_maintenance_mode_key_prefix: str = "metabolights:maintenance:mode"


class RedisSettings(BaseModel):
    connection: RedisConnection
    configuration: RedisConfiguration
