from pydantic import BaseModel


class MetaspaceConnection(BaseModel):
    access_key_id: str
    secret_access_key: str
    bucket: str


class MetaspaceConfiguration(BaseModel):
    metaspace_database: str
    metaspace_fdr: str


class MetaspaceSettings(BaseModel):
    connection: MetaspaceConnection
    configuration: MetaspaceConfiguration
