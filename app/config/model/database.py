from pydantic import BaseModel


class DatabaseConnection(BaseModel):
    host: str
    port: int = 5432
    user: str
    password: str
    database: str


class DatabaseConfiguration(BaseModel):
    conn_pool_min: int = 2
    conn_pool_max: int = 5


class DatabaseSettings(BaseModel):
    connection: DatabaseConnection
    configuration: DatabaseConfiguration = DatabaseConfiguration()
