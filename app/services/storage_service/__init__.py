from app.config import get_settings
from app.services.storage_service.globus import GlobusClient

_globus_client: None | GlobusClient = None


def get_globus_client():
    global _globus_client
    if _globus_client:
        return _globus_client
    con = get_settings().globus.connection
    config = get_settings().globus.configuration
    _globus_client = GlobusClient(
        collection_id=con.collection_id,
        client_id=con.client_id,
        client_secret=con.client_secret,
        config=config,
    )
    return _globus_client
