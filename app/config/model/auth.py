from pydantic import BaseModel


class AuthConfiguration(BaseModel):
    access_token_hash_algorithm: str = "HS256"
    access_token_expires_delta: int = 4 * 60
    admin_jwt_token_expires_in_mins: int = 8 * 60
    access_token_allowed_audience: str
    access_token_issuer_name: str = "Metabolights PythonWS"
    application_secret_key: str
    one_time_token_expires_in_seconds: int = 300


class MetabolightsServiceAccount(BaseModel):
    api_token: str
    email: str


class AuthSettings(BaseModel):
    configuration: AuthConfiguration
    service_account: MetabolightsServiceAccount
