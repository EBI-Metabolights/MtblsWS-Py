import base64
import hashlib
import logging
import uuid
from datetime import timedelta
from typing import Any, List, Union

import jwt
from keycloak import KeycloakAuthenticationError, KeycloakOpenID
from passlib.context import CryptContext
from sqlalchemy import func

from app.config import get_settings
from app.config.model.auth import AuthConfiguration
from app.utils import (
    MetabolightsAuthenticationException,
    MetabolightsAuthorizationException,
    current_time,
)
from app.ws.auth.service import AbstractAuthManager, AuthToken, AuthUser
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import SimplifiedUserModel
from app.ws.db.schemes import User
from app.ws.db.types import (
    UserRole,
    UserStatus,
)
from app.ws.study.user_service import UserService

logger = logging.getLogger("wslog")


class KeycloakAuthService:
    def __init__(self, config: AuthConfiguration):
        self.config = config

    def get_keycloak_openid(self) -> KeycloakOpenID:
        settings = self.config.openid_connect_client
        keycloak_openid = KeycloakOpenID(
            server_url=settings.server_url,
            realm_name=settings.realm_name,
            client_id=settings.client_id,
            client_secret_key=settings.client_secret,
        )
        return keycloak_openid

    def authenticate(self, username: str, password: str) -> AuthToken:
        try:
            auth_server: KeycloakOpenID = self.get_keycloak_openid()
            token = auth_server.token(username=username, password=password)
            auth_token = AuthToken(
                access_token=token.get("access_token"),
                refresh_token=token.get("refresh_token"),
            )
            return self.set_auth_user_info(token.get("access_token"), auth_token)
        except KeycloakAuthenticationError:
            message = f"Authentication error for user {username}"
            logger.error(message)
            raise MetabolightsAuthenticationException(message=message)

    def refresh_token(self, refresh_token: str) -> AuthToken:
        try:
            auth_server: KeycloakOpenID = self.get_keycloak_openid()
            token = auth_server.refresh_token(refresh_token)
            auth_token = AuthToken(
                access_token=token.get("access_token"),
                refresh_token=token.get("refresh_token"),
            )
            return self.set_auth_user_info(token.get("access_token"), auth_token)
        except KeycloakAuthenticationError:
            message = "Authentication error"
            logger.error(message)
            raise MetabolightsAuthenticationException(message=message)

    def set_auth_user_info(
        self, jwt_token: str, user: None | AuthUser = None
    ) -> AuthUser:
        if not user:
            user = AuthUser()

        options = {"verify_signature": False}
        payload: dict[str, str | list | dict] = jwt.decode(jwt_token, options=options)
        orcid = payload.get("orcid", "").replace("https://orcid.org/", "") or ""
        roles = [x for x in payload.get("realm_access", {}).get("roles", [])]
        user.email = payload.get("email")
        user.email_verified = payload.get("email_verified")
        user.first_name = payload.get("given_name")
        user.last_name = payload.get("family_name")
        user.orcid = orcid
        user.roles = roles
        user.country = payload.get("address", {}).get("country", "")
        user.affiliation = payload.get("affiliation", "")
        user.affiliation_url = payload.get("affiliation_url", "")

        return user

    def validate_token(self, jwt: str) -> AuthUser:
        try:
            auth_server: KeycloakOpenID = self.get_keycloak_openid()
            auth_server.userinfo(jwt)
            auth_user = self.set_auth_user_info(jwt)

            if not auth_user.email_verified:
                raise MetabolightsAuthenticationException(
                    http_code=401,
                    message=f"Email address '{auth_user.email}' is not verified",
                )
            return auth_user
        except (KeycloakAuthenticationError, MetabolightsAuthenticationException) as ex:
            message = f"JWT token is not validated: {ex}"
            logger.error(message)
            raise MetabolightsAuthenticationException(message=message)


class AuthenticationManager(AbstractAuthManager):
    def __init__(self, settings: Union[None, AuthConfiguration] = None):
        self.settings = settings
        if not self.settings:
            self.settings: AuthConfiguration = get_settings().auth.configuration
        else:
            self.settings: AuthConfiguration = settings
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        self.external_auth_service = None
        if self.settings.active_authentication_service == "keycloak":
            self.external_auth_service = KeycloakAuthService(self.settings)

    instance = None

    def validate_standalone_jwt_token(
        self,
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

    @classmethod
    def get_instance(cls):
        if not cls.instance:
            cls.instance = AuthenticationManager()
        return cls.instance

    def refresh_token(
        self,
        token: str,
        audience: None | str = None,
        db_session: None | str = None,
        scopes: list[str] = [],
        exp_period_in_mins: int = -1,
    ) -> tuple[str, str]:
        if self.external_auth_service:
            auth_token = self.external_auth_service.refresh_token(token)
            return auth_token.access_token, auth_token.refresh_token

        try:
            user = self.validate_standalone_jwt_token(token)
            additional_data = {
                "name": user.userName,
                "role": UserRole(user.role) if user.role else "",
                "email": user.userName,
                "given_name": user.firstName,
                "family_name": user.lastName,
                "partner": user.partner == 1,
            }
            return self.create_oauth2_token_by_user(
                user.userName,
                additional_data=additional_data,
                audience=audience,
                db_session=db_session,
                scopes=scopes,
                exp_period_in_mins=exp_period_in_mins,
            )
        except Exception as ex:
            raise MetabolightsAuthorizationException(
                message=f"Refresh token task failed. {str(ex)}"
            )

    def create_oauth2_token(
        self,
        username,
        password,
        audience=None,
        db_session=None,
        scopes: List[str] = [],
        exp_period_in_mins: int = -1,
    ):
        if self.external_auth_service:
            auth_token = self.external_auth_service.authenticate(username, password)
            return auth_token.access_token, auth_token.refresh_token

        user = self.authenticate_user(username, password, db_session=db_session)
        additional_data = {
            "name": user["username"],
            "role": UserRole(user["role"]) if user["role"] else "",
            "email": user["username"],
            "given_name": user["firstname"],
            "family_name": user["lastname"],
            "partner": user["partner"] == 1,
        }
        return self.create_oauth2_token_by_user(
            user["username"],
            additional_data,
            audience,
            db_session,
            scopes=scopes,
            exp_period_in_mins=exp_period_in_mins,
        )

    def create_oauth2_token_by_api_token(
        self,
        token,
        audience=None,
        db_session=None,
        scopes: List[str] = [],
        exp_period_in_mins: int = -1,
    ):
        if self.external_auth_service:
            raise MetabolightsAuthorizationException(
                http_code=400, message="Authorization with user token is not supported"
            )
        user = UserService.get_instance(
            self
        ).validate_user_has_submitter_or_super_user_role(token)
        additional_data = {
            "name": user["username"],
            "role": UserRole(user["role"]) if user["role"] else "",
            "email": user["username"],
            "given_name": user["firstname"],
            "family_name": user["lastname"],
            "partner": user["partner"] == 1,
        }
        return self.create_oauth2_token_by_user(
            user["username"],
            additional_data,
            audience,
            db_session,
            scopes=scopes,
            exp_period_in_mins=exp_period_in_mins,
        )

    def create_oauth2_token_by_user(
        self,
        username,
        additional_data: None | dict[str, Any] = None,
        audience=None,
        db_session=None,
        scopes: List[str] = [],
        exp_period_in_mins: int = -1,
        refresh_token_exp_period_in_mins: int = -1,
    ):
        if self.external_auth_service:
            raise MetabolightsAuthorizationException(
                http_code=400, message="Authorization with user name is not supported"
            )
        if not username:
            raise MetabolightsAuthorizationException(
                http_code=400, message="Invalid user or credential"
            )
        if not audience:
            audience = self.settings.access_token_allowed_audience
        if exp_period_in_mins > 0:
            access_token_expires = timedelta(minutes=exp_period_in_mins)
        else:
            access_token_expires = timedelta(
                minutes=self.settings.access_token_expires_delta
            )

        issuer_name = self.settings.access_token_issuer_name
        jti = str(uuid.uuid4())
        if not scopes:
            scopes = ["login"]
        token_base_data = {
            "sub": username,
            "scopes": scopes,
            "iss": issuer_name,
            "aud": audience,
            "jti": jti,
        }
        if additional_data:
            token_base_data.update(additional_data)
        access_token = self._create_jwt_token(
            data=token_base_data, expires_delta=access_token_expires
        )

        if refresh_token_exp_period_in_mins > 0:
            refresh_token_token_expires = timedelta(minutes=exp_period_in_mins)
        else:
            refresh_token_token_expires = timedelta(
                minutes=self.settings.refresh_jwt_token_expires_in_mins
            )
        refresh_token_scopes = token_base_data.copy()
        refresh_token_scopes["scopes"] = ["refresh"]
        refresh_token = self._create_jwt_token(
            data=token_base_data, expires_delta=refresh_token_token_expires
        )
        return access_token, refresh_token

    def create_user_in_db(self, auth_user: AuthToken):
        orcid = auth_user.orcid.replace("https://orcid.org/", "") or None
        api_token = str(uuid.uuid4())

        with DBManager.get_instance().session_maker() as db_session:
            try:
                user = User(
                    firstname=auth_user.first_name,
                    lastname=auth_user.last_name,
                    email=auth_user.email,
                    affiliation=auth_user.affiliation,
                    affiliationurl=auth_user.affiliation_url,
                    address=auth_user.country,
                    orcid=orcid,
                    apitoken=api_token,
                    password=self._get_password_sha1_hash(api_token),
                    metaspace_api_key=None,
                    role=self.map_user_roles(auth_user.roles).value,
                    status=UserStatus.ACTIVE.value,
                    partner="",
                    username="",
                )
                db_session.add(user)
                db_session.commit()
                db_session.refresh(user)
                logger.info("%s user is created.", auth_user.email)
            except Exception as e:
                db_session.rollback()
                logger.error("%s user is not created.", auth_user.email)
                raise e

    def validate_oauth2_token(
        self,
        token: str,
        audience: Union[None, str] = None,
        issuer_name: Union[None, str] = None,
        db_session=None,
    ):
        if self.external_auth_service:
            auth_user = self.external_auth_service.validate_token(token)

            if not db_session:
                db_session = DBManager.get_instance().session_maker()

            with db_session:
                query = db_session.query(User)
                db_user = query.filter(
                    func.lower(User.username) == auth_user.email.lower()
                ).first()
            if not db_user:
                self.create_user_in_db(auth_user)
            user = SimplifiedUserModel.model_validate(db_user)
            return user

        return self.validate_standalone_jwt_token(
            config=self.settings,
            token=token,
            audience=audience,
            issuer_name=issuer_name,
            db_session=db_session,
        )

    role_map: dict[str, UserRole] = {
        "study_curation": UserRole.ROLE_SUPER_USER,
        "system_maintenance": UserRole.SYSTEM_ADMIN,
        "study_review": UserRole.REVIEWER,
        "study_submission": UserRole.ROLE_SUBMITTER,
        "public_study_access": UserRole.ANONYMOUS,
        "partner": UserRole.ROLE_SUBMITTER,
    }

    def map_user_roles(roles: list[str]) -> UserRole:
        if "system_maintenance" in roles:
            return UserRole.SYSTEM_ADMIN
        if "study_curation" in roles:
            return UserRole.ROLE_SUPER_USER
        if "partner" in roles:
            return UserRole.ROLE_SUBMITTER
        if "study_submission" in roles:
            return UserRole.ROLE_SUBMITTER
        if "study_review" in roles:
            return UserRole.REVIEWER
        if "public_study_access" in roles:
            return UserRole.ANONYMOUS
        return UserRole.ANONYMOUS

    def map_user_status(auth_token: AuthToken):
        return UserStatus.ACTIVE if auth_token.email_verified else UserStatus.NEW

    def authenticate_user(self, username: str, password: str, db_session=None):
        if self.external_auth_service:
            try:
                auth_token = self.external_auth_service.authenticate(username, password)
                user = UserService.get_instance(self).get_db_user_by_user_name(username)
                return {
                    "id": user.userId,
                    "username": auth_token.email,
                    "role": self.map_user_roles(auth_token.roles).value,
                    "status": self.map_user_status(auth_token),
                    "apitoken": user.apiToken,
                    "password": "",
                    "partner": "partner" in auth_token.roles,
                    "firstname": user.firstName,
                    "lastname": user.lastName,
                }
            except KeycloakAuthenticationError:
                raise MetabolightsAuthorizationException(
                    message="Invalid user or credential"
                )

        user = UserService.get_instance(
            self
        ).validate_username_with_submitter_or_super_user_role(username)
        if not self._verify_password(password, user.password):
            raise MetabolightsAuthorizationException(
                message="Invalid user or credential"
            )
        return user

    def _create_jwt_token(
        self, data: dict, expires_delta: Union[None, timedelta] = None
    ):
        to_encode = data.copy()
        if expires_delta:
            expire = current_time(utc_timezone=True) + expires_delta
        else:
            expire = current_time(utc_timezone=True) + timedelta(
                minutes=self.settings.access_token_expires_delta
            )
        to_encode.update({"exp": expire})
        jti = to_encode.get("jti")
        key = hashlib.sha256(
            bytes(f"{self.settings.application_secret_key}-{jti}", "utf-8")
        ).hexdigest()
        encoded_jwt = jwt.encode(
            to_encode, key=key, algorithm=self.settings.access_token_hash_algorithm
        )
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
        hashed_password = base64_bytes.decode("ascii")
        return hashed_password

    def _get_password_hash(self, password):
        return self.pwd_context.hash(password)
