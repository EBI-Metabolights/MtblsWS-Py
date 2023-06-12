from pydantic import BaseModel


class JiraConnection(BaseModel):
    username: str
    password: str


class JiraSettings(BaseModel):
    connection: JiraConnection
