from urllib.parse import quote

from psycopg_pool import ConnectionPool

from app.config import get_settings

global_postgresql_pool: None | ConnectionPool = None


def get_db_connection_pool():
    global global_postgresql_pool

    if global_postgresql_pool is None:
        settings = get_settings()
        db_config = settings.database.connection
        user = db_config.user
        password = db_config.password
        host = db_config.host
        port = db_config.port
        db_name = db_config.database
        db_url = f"postgresql://{quote(user)}:{quote(password)}@{host}:{port}/{db_name}"

        conn_pool_min = settings.database.configuration.conn_pool_min
        conn_pool_max = settings.database.configuration.conn_pool_max
        global_postgresql_pool = ConnectionPool(
            db_url,
            min_size=conn_pool_min,
            max_size=conn_pool_max,
            close_returns=True,
        )
    return global_postgresql_pool
