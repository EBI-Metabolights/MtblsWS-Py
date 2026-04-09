from pydantic import BaseModel


class GlobusConfiguration(BaseModel):
    enabled: bool = False
    file_manager_url: str = "https://app.globus.org/file-manager"


class GlobusConnection(BaseModel):
    collection_id: str = ""
    client_id: str = ""
    client_secret: str = ""


class GlobusSettings(BaseModel):
    connection: GlobusConnection = GlobusConnection()
    configuration: GlobusConfiguration = GlobusConfiguration()
