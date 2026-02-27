from pydantic import BaseModel


class MetaspaceConnection(BaseModel):
    access_key_id: str
    secret_access_key: str
    bucket: str


class MetaspaceConfiguration(BaseModel):
    metaspace_database: str = "HMDB-v4"
    metaspace_fdr: str = "0.1"


class MetaspaceSettings(BaseModel):
    connection: MetaspaceConnection
    configuration: MetaspaceConfiguration = MetaspaceConfiguration()
