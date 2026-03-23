import base64
import datetime
import hashlib
import logging
import re
import uuid
from datetime import timedelta
from typing import Any, List, Union

import jwt
from keycloak import KeycloakAdmin, KeycloakAuthenticationError, KeycloakOpenID
from passlib.context import CryptContext
from sqlalchemy import func

from app.config import get_settings
from app.config.model.auth import AuthConfiguration
from app.utils import (
    MetabolightsAuthenticationException,
    MetabolightsAuthorizationException,
    MetabolightsException,
    current_time,
)
from app.ws.auth.service import AbstractAuthManager, AuthToken, UserProfile
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import SimplifiedUserModel
from app.ws.db.schemes import User
from app.ws.db.types import (
    UserRole,
    UserStatus,
)
from app.ws.db.utils import datetime_to_int
from app.ws.study.user_service import UserService

logger = logging.getLogger("wslog")


class KeycloakAuthService:
    _keycloak_openid = None
    _keycloak_admin = None

    def __init__(self, config: AuthConfiguration):
        self.config = config

    def get_keycloak_admin(self) -> KeycloakAdmin:
        if self._keycloak_admin:
            return self._keycloak_admin

        settings = self.config.openid_connect_admin
        self._keycloak_admin = KeycloakAdmin(
            server_url=settings.server_url,
            realm_name=settings.realm_name,
            username=settings.username,
            password=settings.password,
            verify=True,
        )
        return self._keycloak_admin

    def get_keycloak_openid(self) -> KeycloakOpenID:
        if self._keycloak_openid:
            return self._keycloak_openid

        settings = self.config.openid_connect_client
        self._keycloak_openid = KeycloakOpenID(
            server_url=settings.server_url,
            realm_name=settings.realm_name,
            client_id=settings.client_id,
            client_secret_key=settings.client_secret,
        )
        return self._keycloak_openid

    def authenticate(self, username: str, password: str) -> AuthToken:
        try:
            auth_server: KeycloakOpenID = self.get_keycloak_openid()
            token = auth_server.token(username=username, password=password)
            auth_token = AuthToken(
                access_token=token.get("access_token"),
                refresh_token=token.get("refresh_token"),
            )
            return self.get_user_profile_from_jwt(token.get("access_token"), auth_token)
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
            return self.get_user_profile_from_jwt(token.get("access_token"), auth_token)
        except KeycloakAuthenticationError:
            message = "Authentication error"
            logger.error(message)
            raise MetabolightsAuthenticationException(message=message)

    def get_user_profile(self, username: str) -> UserProfile:
        user_id = self.get_keycloak_admin().get_user_id(username)
        if not user_id:
            raise MetabolightsException(
                message=f"Username '{username}' is not in auth service", http_code=501
            )
        user = self.get_keycloak_admin().get_user(
            user_id=user_id, user_profile_metadata=True
        )
        if user:
            roles = self.get_keycloak_admin().get_composite_realm_roles_of_user(user_id)
            return self.create_user_profile_from_dict(user, roles)
        return None

    def create_user_profile_from_dict(
        self, dict_data: dict, roles: list[dict]
    ) -> UserProfile:
        user = UserProfile()
        if not dict_data:
            return user

        realm_roles = {x["name"] for x in roles} if roles else set()
        if "study_curation" in realm_roles or "system_maintenance" in realm_roles:
            role = UserRole.ROLE_SUPER_USER
        elif "study_submission" in realm_roles:
            role = UserRole.ROLE_SUBMITTER
        elif "study_review" in realm_roles:
            role = UserRole.REVIEWER
        else:
            role = UserRole.ANONYMOUS
        partner = "partner" in realm_roles
        payload = dict_data
        enabled = payload.get("enabled", False)
        email_verified = payload.get("emailVerified")
        if enabled:
            status = UserStatus.ACTIVE if email_verified else UserStatus.NEW
        else:
            status = UserStatus.FROZEN
        join_date = datetime.datetime.fromtimestamp(
            payload.get("createdTimestamp") / 1000.0
        )

        attributes: dict = dict_data.get("attributes", {})
        orcid = attributes.get("orcid") or [""]
        orcid = re.sub(r"https?://orcid\.org/", "", (orcid[0] or "").lower())
        user.email = payload.get("email")
        user.username = user.email
        user.email_verified = email_verified
        user.first_name = payload.get("firstName")
        user.last_name = payload.get("lastName")
        user.orcid = orcid
        user.role = role
        user.enabled = enabled
        user.status = status
        user.join_date = join_date
        user.country = (attributes.get("country") or [""])[0]
        user.affiliation = (attributes.get("affiliation") or [""])[0]
        user.address = (attributes.get("affiliationAddress") or [""])[0]
        user.affiliation_url = (attributes.get("affiliationUrl") or [""])[0]
        user.globus_username = (attributes.get("globusUserName") or [""])[0]
        user.partner = partner
        return user

    def get_user_profile_from_jwt(
        self, jwt_token: str, user: None | UserProfile = None
    ) -> UserProfile:
        if not user:
            user = UserProfile()

        options = {"verify_signature": False}
        payload: dict[str, str | list | dict] = jwt.decode(jwt_token, options=options)
        orcid = payload.get("orcid", "").replace("https://orcid.org/", "") or ""
        roles = {x for x in payload.get("realm_access", {}).get("roles", [])}
        if "study_curation" in roles or "system_maintenance" in roles:
            role = UserRole.ROLE_SUPER_USER
        elif "study_submission" in roles:
            role = UserRole.ROLE_SUBMITTER
        elif "study_review" in roles:
            role = UserRole.REVIEWER
        else:
            role = UserRole.ANONYMOUS
        user.partner == "partner" in roles
        user.email = payload.get("email", "")
        user.username = user.email
        user.email_verified = payload.get("email_verified", None)
        user.first_name = payload.get("given_name", "")
        user.last_name = payload.get("family_name", "")
        user.full_name = payload.get("name", f"{user.first_name} {user.last_name}")
        user.orcid = orcid
        user.role = role
        user.enabled = payload.get("enabled", None)

        try:
            created_at = int(payload.get("created_datetime", 0)) / 1000.0
        except Exception:
            created_at = None
        user.join_date = datetime.datetime.fromtimestamp(created_at)
        user.country = payload.get("address", {}).get("country", "")
        user.affiliation = payload.get("affiliation", "")
        user.affiliation_url = payload.get("affiliation_url", "")
        user.globus_username = payload.get("globus_username", "")

        return user

    def validate_token(self, jwt: str) -> UserProfile:
        try:
            auth_server: KeycloakOpenID = self.get_keycloak_openid()
            auth_server.userinfo(jwt)
            auth_user = self.get_user_profile_from_jwt(jwt)

            if not auth_user.email_verified:
                raise MetabolightsAuthenticationException(
                    http_code=401,
                    message=f"Email address '{auth_user.email}' is not verified",
                )
            return auth_user
        except (KeycloakAuthenticationError, MetabolightsAuthenticationException) as ex:
            message = f"JWT token is not validated: {ex}"
            logger.error(message)
            if isinstance(ex, MetabolightsAuthenticationException):
                raise ex
            raise MetabolightsAuthenticationException(message=message, exception=ex)


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

    def get_user_profile(self, username: str) -> UserProfile:
        if self.external_auth_service:
            return self.external_auth_service.get_user_profile(username)

        raise NotImplementedError("Local profile is not supported.")

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
                raise MetabolightsAuthenticationException(
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
                raise MetabolightsAuthenticationException(
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
                raise MetabolightsAuthenticationException(
                    message="Could not validate credentials or no username"
                )

            user = self.create_simplified_user_model(db_user)
        except Exception as e:
            raise e

        if UserStatus(user.status) != UserStatus.ACTIVE:
            raise MetabolightsAuthenticationException(message="Not an active user")

        return user

    def create_simplified_user_model(self, db_user: User) -> None | SimplifiedUserModel:
        if not self.external_auth_service or not db_user or not db_user.username:
            username = db_user.username if db_user and db_user.username else ""
            raise MetabolightsException(
                message=f"Auth service or user is not defined. {username}"
            )
        user: UserProfile = self.external_auth_service.get_user_profile(
            username=db_user.username
        )
        if not user:
            raise MetabolightsException(
                message=f"Username is not in auth service '{db_user.username}'"
            )
        return SimplifiedUserModel(
            address=user.country,
            affiliation=user.affiliation,
            affiliationurl=user.affiliation_url,
            email=user.email.lower(),
            firstname=user.first_name,
            fullName=user.first_name + " " + user.last_name,
            joindate=datetime_to_int(user.join_date),
            lastname=user.last_name,
            orcid=user.orcid,
            role=user.role.name,
            status=user.status.name,
            partner=user.partner,
            username=user.username.lower(),
            globususername=user.globus_username,
        )

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
        except (
            MetabolightsAuthorizationException,
            MetabolightsAuthenticationException,
            Exception,
        ) as ex:
            if isinstance(ex, MetabolightsAuthenticationException):
                raise ex
            raise MetabolightsAuthenticationException(
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
    ) -> tuple[str, str]:
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
    ) -> tuple[str, str]:
        if self.external_auth_service:
            raise MetabolightsAuthenticationException(
                message="Authorization with user token is not supported"
            )
        user = UserService.get_instance(
            self
        ).validate_user_has_submitter_or_super_user_role(token)
        user = user._asdict()
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
    ) -> tuple[str, str]:
        if self.external_auth_service:
            raise MetabolightsAuthenticationException(
                message="Authorization with user name is not supported"
            )
        if not username:
            raise MetabolightsAuthenticationException(
                message="Invalid user or credential"
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
            "email": username,
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
                    role=auth_user.role.value,
                    status=auth_user.status.value,
                    partner=1 if auth_user.partner else 0,
                    username=auth_user.username,
                )
                db_session.add(user)
                db_session.commit()
                db_session.refresh(user)
                logger.info("%s user is created.", auth_user.email)
                return user
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
    ) -> UserProfile:
        if self.external_auth_service:
            auth_user = self.external_auth_service.validate_token(token)

            if not db_session:
                db_session = DBManager.get_instance().session_maker()

            with db_session:
                query = db_session.query(User)
                db_user = query.filter(
                    func.lower(User.username) == auth_user.username.lower()
                ).first()
            if not db_user:
                db_user = self.create_user_in_db(auth_user)
                auth_user.api_token = db_user.apitoken
            return auth_user

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
                raise MetabolightsAuthenticationException(
                    message="Invalid user or credential"
                )

        user = UserService.get_instance(
            self
        ).validate_username_with_submitter_or_super_user_role(username)
        if not self._verify_password(password, user.password):
            raise MetabolightsAuthenticationException(
                message="Invalid user or credential"
            )
        return user._asdict()

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
