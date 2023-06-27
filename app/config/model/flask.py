from pydantic import BaseModel


class FlaskConfiguration(BaseModel):
    ENV: str
    DEBUG: bool
    TESTING: bool
    SECRET_KEY: str
