import hashlib
from typing import Union

import jwt
from sqlalchemy import func

from app.config import get_settings
from app.config.model.auth import AuthConfiguration
from app.utils import MetabolightsAuthorizationException, current_time
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import SimplifiedUserModel
from app.ws.db.schemes import User
from app.ws.db.types import UserStatus


def validate_jwt_token(
    token: str,
    config: None | AuthConfiguration = None,
    audience: Union[None, str] = None,
    issuer_name: Union[None, str] = None,
    db_session=None,
) -> SimplifiedUserModel:
    user = None
    try:
        if not config:
            config = get_settings().auth.configuration
        if not audience:
            audience = config.access_token_allowed_audience
        if not issuer_name:
            issuer_name = config.access_token_issuer_name

        options = {
            "verify_signature": False,
            "verify_exp": True,
            "verify_jti": True,
            "verify_sub": True,
            "verify_iss": True,
        }
        payload = jwt.decode(
            token,
            key=config.application_secret_key,
            algorithms=[config.access_token_hash_algorithm],
            audience=audience,
            issuer=issuer_name,
            options=options,
        )
        exp = payload.get("exp")
        now = int(current_time().timestamp())
        if now > exp:
            raise MetabolightsAuthorizationException(
                message="Autantication token is expired"
            )
        jti = payload.get("jti")
        key = hashlib.sha256(
            bytes(f"{config.application_secret_key}-{jti}", "utf-8")
        ).hexdigest()
        payload = jwt.decode(
            token,
            key=key,
            algorithms=[config.access_token_hash_algorithm],
            audience=audience,
            issuer=issuer_name,
        )
        username: str = payload.get("sub")
        if username is None:
            raise MetabolightsAuthorizationException(
                message="Could not validate credentials or no username"
            )
        if not db_session:
            db_session = DBManager.get_instance().session_maker()

        with db_session:
            query = db_session.query(User)
            db_user = query.filter(
                func.lower(User.username) == username.lower()
            ).first()
        if not db_user:
            raise MetabolightsAuthorizationException(
                message="Could not validate credentials or no username"
            )
        user = SimplifiedUserModel.model_validate(db_user)
    except Exception as e:
        raise e

    if UserStatus(user.status) != UserStatus.ACTIVE:
        raise MetabolightsAuthorizationException(message="Not an active user")

    return user
