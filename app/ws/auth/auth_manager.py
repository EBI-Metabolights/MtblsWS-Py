import base64
import hashlib
import os
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Optional, List

from flask import current_app as app
from flask_restful import abort
from jose import jwt, JWTError
from passlib.context import CryptContext
from pydantic import ValidationError, BaseModel, BaseSettings

from app.ws.db.dbmanager import DBManager
from app.ws.db.models import SimplifiedUserModel
from app.ws.db.schemes import User
from app.ws.db.types import UserStatus, UserRole
from app.ws.study.user_service import UserService


class TokenData(BaseModel):
    username: Optional[str] = None
    scopes: List[str] = []


app_secrets_dir = os.path.join(os.getcwd(), ".secrets")
if "SECRETS_DIR" in os.environ and os.environ["SECRETS_DIR"]:
    app_secrets_dir = os.environ["SECRETS_DIR"]


class SecuritySettings(BaseSettings):
    application_secret_key: str = ""
    access_token_hash_algorithm: str = "HS256"
    access_token_expires_delta: int = 300
    access_token_allowed_audience: str = None
    access_token_issuer_name: str = "Metabolights PythonWS"


@lru_cache(1)
def get_security_settings(app):
    settings = SecuritySettings()
    if app:
        settings.access_token_hash_algorithm = app.config.get("ACCESS_TOKEN_HASH_ALGORITHM")
        settings.access_token_expires_delta = app.config.get("ACCESS_TOKEN_EXPIRES_DELTA")
        settings.access_token_allowed_audience = app.config.get("ACCESS_TOKEN_ALLOWED_AUDIENCE")
        settings.access_token_issuer_name = app.config.get("ACCESS_TOKEN_ISSUER_NAME")
        settings.application_secret_key = app.config.get("APPLICATION_SECRET_KEY")
    return settings


class AuthenticationManager(object):
    def __init__(self, settings: SecuritySettings = None, app=None):
        self.settings = settings
        if not self.settings and app:
            self.settings = get_security_settings(app)
        else:
            self.settings = SecuritySettings()
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

    instance = None

    @classmethod
    def get_instance(cls, app):
        if not cls.instance:
            cls.instance = AuthenticationManager(app=app)
        return cls.instance

    def create_oauth2_token(self, username, password, audience=None, db_session=None):
        user = self.authenticate_user(username, password, db_session=db_session)
        if not user:
            abort(http_status_code=400, detail="Incorrect username or password")
        if not audience:
            audience = self.settings.access_token_allowed_audience
        access_token_expires = timedelta(minutes=self.settings.access_token_expires_delta)
        issuer_name = self.settings.access_token_issuer_name
        token_base_data = {"sub": user.username, "scopes": [UserRole(user.role).name], "role": UserRole(user.role).name,
                           "iss": issuer_name, "aud": audience, "Name": username}

        access_token = self._create_jwt_token(data=token_base_data, expires_delta=access_token_expires)
        return access_token

    def validate_oauth2_token(self, token: str, audience: str = None, issuer_name: str = None, db_session=None):

        user = None
        try:
            if not audience:
                audience = self.settings.access_token_allowed_audience
            if not issuer_name:
                issuer_name = self.settings.access_token_issuer_name
            payload = jwt.decode(token, self.settings.application_secret_key, audience=audience, issuer=issuer_name,
                                 algorithms=[self.settings.access_token_hash_algorithm])
            username: str = payload.get("sub")
            if username is None:
                abort(http_status_code=401, detail="Could not validate credentials, no username")
            if not db_session:
                db_session = DBManager.get_instance(app).session_maker()

            with db_session:
                query = db_session.query(User)
                db_user = query.filter(User.username == username).first()
            if not db_user:
                abort(401, detail="Could not validate credentials, username is not in db", )
            user = SimplifiedUserModel.from_orm(db_user)
        except (JWTError, ValidationError) as e:
            abort(http_status_code=401, detail="Could not validate credentials")

        if UserStatus(user.status) != UserStatus.ACTIVE:
            abort(http_status_code=401, detail="Not active user")

        return user

    def authenticate_user(self, username: str, password: str, db_session=None):
        user = UserService.get_instance(app).validate_username_with_submitter_or_super_user_role(username)
        if not self._verify_password(password, user.password):
            return False
        return user

    def _create_jwt_token(self, data: dict, expires_delta: Optional[timedelta] = None):
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=self.settings.access_token_expires_delta)
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, self.settings.application_secret_key,
                                 algorithm=self.settings.access_token_hash_algorithm)
        return encoded_jwt

    def _verify_password(self, plain_password, hashed_password):
        try:
            verified = self.pwd_context.verify(plain_password, hashed_password)
            if verified:
                return True
        except Exception as e:
            # Check with legacy password hash algorithm
            current_hash = self._get_password_sha1_hash(plain_password)
            if current_hash == hashed_password:
                # TODO Calculate password hash with new algorithm and update DB
                # new_password_hash = get_password_hash(plain_password)
                #
                return True
        return False

    @staticmethod
    def _get_password_sha1_hash(password):
        # SHA1 is not secure but current db contains passwords with SHA1
        byte_value = str.encode(password)
        hash_object = hashlib.sha1(byte_value)
        hex_string_hash = hash_object.hexdigest()
        hash_bytes = bytes.fromhex(hex_string_hash)
        base64_bytes = base64.b64encode(hash_bytes)
        hashed_password = base64_bytes.decode('ascii')
        return hashed_password

    def _get_password_hash(self, password):
        return self.pwd_context.hash(password)
