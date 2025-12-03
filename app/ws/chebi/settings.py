from app.config import get_settings


class ChebiWsSettings(object):
    def __init__(self):
        self.chebi_ws_wsdl = None
        self.chebi_ws_wsdl_service = None
        self.chebi_ws_wsdl_service_port = None
        self.chebi_ws_strict = None
        self.chebi_ws_xml_huge_tree = None
        self.chebi_ws_service_binding_log_level = None


def get_chebi_ws_settings():
    settings = ChebiWsSettings()
    app_settings = get_settings()
    settings.chebi_ws_wsdl = app_settings.chebi.service.connection.chebi_ws_wsdl
    settings.chebi_ws_wsdl_service = (
        app_settings.chebi.service.connection.chebi_ws_wsdl_service
    )
    settings.chebi_ws_wsdl_service_port = (
        app_settings.chebi.service.connection.chebi_ws_wsdl_service_port
    )
    settings.chebi_ws_strict = app_settings.chebi.service.configuration.chebi_ws_strict
    settings.chebi_ws_xml_huge_tree = (
        app_settings.chebi.service.configuration.chebi_ws_xml_huge_tree
    )
    settings.chebi_ws_service_binding_log_level = (
        app_settings.chebi.service.configuration.chebi_ws_service_binding_log_level
    )
    return settings
