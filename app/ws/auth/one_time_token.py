import datetime
import hashlib
import logging
import uuid

import jwt

from app.config import get_settings
from app.ws.redis.redis import RedisStorage, get_redis_server

logger = logging.getLogger(__name__)


def _decode_redis_value(value: None | bytes | str) -> str:
    if not value:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def create_one_time_token(
    jwt_token: str, expire_time_in_seconds: None | int = None
) -> None | str:
    if not jwt_token:
        return None

    try:
        redis: RedisStorage = get_redis_server()
        options = {"verify_signature": False}
        payload: dict[str, str | list | dict] = jwt.decode(jwt_token, options=options)
        subject = payload.get("sub")
        if not subject:
            raise ValueError("Invalid JWT token: missing email")
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
        created = redis.set_value(token_key, jwt_token, ex=ex)
        if not created:
            logger.error("Redis did not store one-time-token for subject: %s", subject)
            return None
        logger.info(
            "One-time-token created for subject: %s. JWT TOKEN: ...%s",
            subject,
            jwt_token[-4:],
        )
        return token
    except Exception as ex:
        logger.error("Error while creating one-time-token: %s", ex)
        return None


def get_jwt_with_one_time_token(one_time_token: str) -> None | str:
    if not one_time_token:
        return None
    token_key = f"one-time-token-request:token:{one_time_token}"
    redis: None | RedisStorage = None
    try:
        redis = get_redis_server()
        jwt_value: None | bytes | str = redis.get_value(token_key, readonly=False)
        jwt_data = _decode_redis_value(jwt_value)
        logger.info(
            "Fetching JWT with one-time-token: ...%s, JWT TOKEN: ...%s",
            one_time_token[-4:],
            jwt_data[-4:],
        )
        try:
            redis.delete_value(token_key)
            logger.info("one-time-token: ...%s is removed", one_time_token[-4:])
        except Exception as ex:
            logger.error("Error while deleting one-time-token: %s", ex)
        return jwt_data
    except Exception as ex:
        logger.error("Error while fetching one-time-token: %s", ex)
        if redis:
            try:
                redis.delete_value(token_key)
            except Exception as delete_ex:
                logger.error("Error while deleting one-time-token: %s", delete_ex)
        return None
