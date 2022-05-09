from functools import lru_cache

from flask import current_app as app


class ChebiWsSettings(object):
    def __init__(self):
        self.chebi_ws_wsdl = None
        self.chebi_ws_wsdl_service = None
        self.chebi_ws_wsdl_service_port = None
        self.chebi_ws_strict = None
        self.chebi_ws_xml_huge_tree = None
        self.chebi_ws_service_binding_log_level = None


class ChebiWsSettingsFlaskConfig(ChebiWsSettings):

    def __init__(self, application=None):
        super().__init__()
        self.app = application
        if not self.app:
            self.app = app
        self.chebi_ws_wsdl = app.config.get("CHEBI_WS_WSDL")
        self.chebi_ws_wsdl_service = app.config.get("CHEBI_WS_WSDL_SERVICE")
        self.chebi_ws_wsdl_service_port = app.config.get("CHEBI_WS_WSDL_SERVICE_PORT")
        self.chebi_ws_strict = app.config.get("CHEBI_WS_STRICT")
        self.chebi_ws_xml_huge_tree = app.config.get("CHEBI_WS_XML_HUGE_TREE")
        self.chebi_ws_service_binding_log_level = app.config.get("CHEBI_WS_WSDL_SERVICE_BINDING_LOG_LEVEL")


@lru_cache
def get_chebi_ws_settings():
    settings = ChebiWsSettings()
    settings.chebi_ws_wsdl = app.config.get("CHEBI_WS_WSDL")
    settings.chebi_ws_wsdl_service = app.config.get("CHEBI_WS_WSDL_SERVICE")
    settings.chebi_ws_wsdl_service_port = app.config.get("CHEBI_WS_WSDL_SERVICE_PORT")
    settings.chebi_ws_strict = app.config.get("CHEBI_WS_STRICT")
    settings.chebi_ws_xml_huge_tree = app.config.get("CHEBI_WS_XML_HUGE_TREE")
    settings.chebi_ws_service_binding_log_level = app.config.get("CHEBI_WS_WSDL_SERVICE_BINDING_LOG_LEVEL")
    return settings
