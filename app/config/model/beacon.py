
from pydantic import BaseModel
from typing import List, Dict, Union
from datetime import datetime


class BeaconConfiguration(BaseModel):
    beacon_id: str
    beacon_name: str
    api_version: str
    uri: str
    default_beacon_granularity: str
    max_beacon_granularity: str
    org_id: str
    org_name: str
    org_description: str
    org_address: str
    org_welcome_url: str
    org_contact_url: str
    org_logo_url: str
    org_info: str
    description: str
    version: str
    welcome_url: str
    alternative_url: str
    create_datetime: Union[str, datetime, None]
    update_datetime: Union[str, datetime, None]
    service_type: str
    service_url: str
    entry_point: bool
    is_open: bool
    documentation_url: str
    environment: str
    ga4gh_service_type_group: str
    ga4gh_service_type_artifact: str
    ga4gh_service_type_version: str
    beacon_handovers: List[Dict[str, Union[Dict[str, str], str]]]
    # database_host: str
    # database_port: int
    # database_user: str
    # database_password: str
    # database_name: str
    # database_auth_source: str
    # beacon_host: str
    # beacon_port: int
    # beacon_tls_enabled: bool
    # beacon_tls_client: bool
    # beacon_cert: str
    # beacon_key: str
    # CA_cert: str
    # permissions_url: str
    # idp_user_info: str
    # lsaai_user_info: str
    # trusted_issuers: List[str]
    # autocomplete_limit: int
    # autocomplete_ellipsis: str
    # ontologies_folder: str
