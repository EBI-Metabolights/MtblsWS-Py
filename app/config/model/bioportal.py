from pydantic import BaseModel


class BioportalSettings(BaseModel):
    api_token: str
    url: str
