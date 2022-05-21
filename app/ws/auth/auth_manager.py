import base64
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict

from fastapi import HTTPException
from fastapi import status
from fastapi.requests import Request
from fastapi.security import (
    OAuth2PasswordBearer, SecurityScopes,
)
from jose import jwt, JWTError
from passlib.context import CryptContext
from pydantic import ValidationError

from app.db.schemes import UserTable
from app.security.models import TokenData, User
from app.security.types import UserStatus
from app.settings import SecuritySettings
from app.utils.redis import RedisStorage


class AuthenticationManager(object):

    def __init__(self, settings: SecuritySettings, storage: RedisStorage, role_scope_mapping: Dict):
        self.settings = settings
        self.storage = storage
        self.role_scope_mapping = role_scope_mapping
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        self.oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token",
                                                  description="Please login to use web services require authorization.")



    def create_oauth2_token(self, username, password, db_session):
        user = self.authenticate_user(username, password, db_session=db_session)
        if not user:
            raise HTTPException(status_code=400, detail="Incorrect username or password")
        access_token_expires = timedelta(minutes=self.settings.access_token_expires_delta)
        access_token = self._create_access_token(
            data={"sub": user.username, "scopes": self.role_scope_mapping[user.role]},
            expires_delta=access_token_expires,
        )
        return access_token

    def revoke_oauth2_token(self, token: str):
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
        )
        try:
            if self.storage.is_key_in_store("%s%s" %(self.settings.revoced_acces_token_prefix, token)):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Token has already revoked.")

            payload = jwt.decode(token, self.settings.application_secret_key,
                                 algorithms=[self.settings.access_token_hash_algorithm])
            username: str = payload.get("sub")
            if username is None:
                raise credentials_exception

            exp = payload.get("exp")
            if exp >= int(datetime.utcnow().timestamp()):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Token has already expired.")

            key = self.settings.revoced_acces_token_prefix + token
            self.storage.set_value_with_expiration_time(key, username, exp)
            return {"status": "token is revoked"}
        except (JWTError, ValidationError):
            raise credentials_exception

    def validate_oauth2_token(self, security_scopes: SecurityScopes, token: str, request: Request, db_session ):
        if security_scopes.scopes:
            authenticate_value = f'Bearer scope="{security_scopes.scope_str}"'
        else:
            authenticate_value = f"Bearer"
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": authenticate_value},
        )
        try:
            payload = jwt.decode(token, self.settings.application_secret_key,
                                 algorithms=[self.settings.access_token_hash_algorithm])
            username: str = payload.get("sub")
            if username is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Could not validate credentials",
                    headers={"WWW-Authenticate": authenticate_value},
                )

            if self.storage.is_key_in_store(self.settings.revoced_acces_token_prefix + token):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Used authorization session has been revoked.",
                    headers={"WWW-Authenticate": authenticate_value},
                )
            token_scopes = payload.get("scopes", [])
            token_data = TokenData(scopes=token_scopes, username=username)
        except (JWTError, ValidationError) as e:
            raise credentials_exception
        db_user = self.get_user(username=token_data.username, db_session=db_session)
        if db_user is None:
            raise credentials_exception

        user = User.from_orm(db_user)

        if UserStatus(user.status) != UserStatus.ACTIVE:
            raise HTTPException(status_code=400, detail="Not active user")

        for scope in security_scopes.scopes:
            if scope not in token_data.scopes:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Not enough permissions",
                    headers={"WWW-Authenticate": authenticate_value},
                )

        request.state.user = user
        request.state.token = token
        request.state.token_data = token_data
        request.state.valid_user = True

        return user

    def get_user(self, username: str, db_session):

        try:
            db_user = db_session.query(UserTable).filter(UserTable.username == username).first()
            if db_user:
                return db_user
            return None
        except Exception as e:
            raise HTTPException(status_code=500, detail="Server internal error: %s" % str(e))


    def authenticate_user(self, username: str, password: str, db_session) :
        user = self.get_user(username, db_session=db_session)
        if not user:
            return False
        if not self._verify_password(password, user.password):
            return False
        return user

    def _create_access_token(self, data: dict, expires_delta: Optional[timedelta] = None):
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
