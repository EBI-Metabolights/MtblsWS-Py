from pydantic import BaseModel

from app.ws.db.models import SimplifiedUserModel


class AuthUser(BaseModel):
    email: str = ""
    email_verified: bool = False
    first_name: str = ""
    last_name: str = ""
    orcid: str = ""
    roles: list[str] = []
    affiliation: str = ""
    affiliation_url: str = ""
    country: str = ""


class AuthToken(AuthUser):
    access_token: str
    refresh_token: str


class AbstractAuthManager:
    def validate_oauth2_token(
        self,
        token: str,
        audience: None | str = None,
        issuer_name: None | str = None,
        db_session=None,
    ) -> None | SimplifiedUserModel: ...

    def create_oauth2_token(
        self,
        username,
        password,
        audience=None,
        db_session=None,
        scopes: list[str] = [],
        exp_period_in_mins: int = -1,
    ) -> tuple[str, str]: ...

    def refresh_token(
        self,
        token: str,
        audience: None | str = None,
        db_session: None | str = None,
        scopes: list[str] = [],
        exp_period_in_mins: int = -1,
    ) -> tuple[str, str]: ...
