from enum import Enum
from typing import List, Set, Union
import isatools
import metaspace
import mzml2isa
import pkg_resources
from pydantic import BaseModel
import app

isatools_version = pkg_resources.get_distribution(isatools.__name__).version
metaspace_version = metaspace.__version__
mzml2isa_version = mzml2isa.__version__

class EndpointMethodOption(str, Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE" 
    ANY = "*"   


class EndpointDescription(BaseModel):
    method: Union[EndpointMethodOption, Set[EndpointMethodOption]]
    path: str    
    

class ServerService(BaseModel):
    rest_api_port: int
    app_host_url: str
    ws_app_base_link: str
    mtbls_ws_host: str
    resources_path: str
    cors_hosts: str = "*"
    allowed_host_domains: List[str] = [r"https://.+\.ebi\.ac\.uk"]
    cors_resources_path: str
    api_doc: str
    maintenance_mode: bool = False
    config_file_check_period_in_seconds: int = 60
    banner_check_period_in_seconds: int = 60
    enabled_endpoints_under_maintenance: List[EndpointDescription]
    disabled_endpoints: List[EndpointDescription]


class ServerDescription(BaseModel):
    ws_app_name: str = "MtblsWS-Py"
    ws_app_description: str = "MetaboLights RESTful WebService"
    ws_app_version: str = app.__app_version__
    metabolights_api_version: str = app.__api_version__
    isa_api_version: str = isatools_version
    metaspace_api_version: str = metaspace_version
    mzml2isa_api_version: str = mzml2isa_version


class LogSettings(BaseModel):
    log_config_file_path: str = ""
    log_path: str = "./logs"
    log_headers: bool = True
    log_body: bool = False
    log_json: bool = False


class ServerSettings(BaseModel):
    service: ServerService
    description: ServerDescription = ServerDescription()
    log: LogSettings
    temp_directory_path: str
