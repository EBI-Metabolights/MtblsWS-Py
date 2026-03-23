import abc
import datetime

from pydantic import BaseModel

from app.ws.db.types import UserRole, UserStatus


class UserProfile(BaseModel):
    username: str = ""
    full_name: str = ""
    email: str = ""
    email_verified: bool = False
    first_name: str = ""
    last_name: str = ""
    orcid: str = ""
    role: None | UserRole = None
    address: str = ""
    affiliation: str = ""
    affiliation_url: str = ""
    country: str = ""
    globus_username: str = ""
    enabled: bool = False
    join_date: None | datetime.datetime = None
    status: None | UserStatus = None
    partner: bool = False
    api_token: str = ""


class AuthToken(UserProfile):
    access_token: str
    refresh_token: str


class AbstractAuthManager(abc.ABC):
    @abc.abstractmethod
    def validate_oauth2_token(
        self,
        token: str,
        audience: None | str = None,
        issuer_name: None | str = None,
        db_session=None,
    ) -> UserProfile: ...

    @abc.abstractmethod
    def create_oauth2_token(
        self,
        username,
        password,
        audience=None,
        db_session=None,
        scopes: list[str] = [],
        exp_period_in_mins: int = -1,
    ) -> tuple[str, str]: ...

    @abc.abstractmethod
    def refresh_token(
        self,
        token: str,
        audience: None | str = None,
        db_session: None | str = None,
        scopes: list[str] = [],
        exp_period_in_mins: int = -1,
    ) -> tuple[str, str]: ...

    @abc.abstractmethod
    def get_user_profile(self, username: str) -> UserProfile: ...
