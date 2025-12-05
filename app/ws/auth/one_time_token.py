import datetime
import hashlib
import logging
import uuid

import jwt

from app.config import get_settings
from app.ws.redis.redis import RedisStorage, get_redis_server

logger = logging.getLogger(__name__)


def create_one_time_token(
    jwt_token: str, expire_time_in_seconds: None | int = None
) -> None | str:
    if not jwt_token:
        return None
    try:
        redis: RedisStorage = get_redis_server()
        options = {"verify_signature": False}
        payload: dict[str, str | list | dict] = jwt.decode(jwt_token, options=options)
        subject = payload.get("sub", "")
        token_base: str = "_".join(
            [
                "mtbls_one_time_passcode",
                subject,
                datetime.datetime.now(datetime.UTC).isoformat(),
                str(uuid.uuid4()),
            ]
        )
        token = hashlib.sha256(token_base.encode()).hexdigest()
        token_key = f"one-time-token-request:token:{token}"
        ex = expire_time_in_seconds
        if not ex:
            ex = get_settings().auth.configuration.one_time_token_expires_in_seconds
        redis.set_value(token_key, jwt_token, ex=ex)
        return token
    except Exception as ex:
        logger.error("Error while creating one-time-token: %s", ex)
        return None


def get_jwt_with_one_time_token(one_time_token: str) -> None | str:
    if not one_time_token:
        return None
    try:
        redis: RedisStorage = get_redis_server()
        token_key = f"one-time-token-request:token:{one_time_token}"

        jwt = redis.get_value(token_key) or ""
        redis.delete_value(token_key)
        jwt = jwt.decode("utf-8") if jwt else None
        return jwt
    except Exception as ex:
        logger.error("Error while creating one-time-token: %s", ex)
        return None
