from app.ws.settings.base import MetabolightsBaseSettings


class RedisSettings(MetabolightsBaseSettings):
    redis_host: str
    redis_password: str
    redis_port: str
    redis_db: str
