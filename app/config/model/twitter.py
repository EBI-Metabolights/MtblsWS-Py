from pydantic import BaseModel


class TwitterConnection(BaseModel):
    consumer_key: str
    consumer_secret: str
    token: str
    token_secret: str
    bearer: str


class TwitterSettings(BaseModel):
    connection: TwitterConnection
