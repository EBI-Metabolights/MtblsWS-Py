from pydantic import BaseModel


class BlueSkyConnection(BaseModel):
    app_password: str = ""
    handle: str = "metabolights.bsky.social"
    max_post_length: int = 300


class BlueSkySettings(BaseModel):
    connection: BlueSkyConnection
