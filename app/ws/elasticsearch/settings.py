from pydantic import BaseSettings


class ElasticsearchSettings(BaseSettings):
    elasticsearch_host: str = "localhost"
    elasticsearch_port: int = 9200
    elasticsearch_use_tls: bool = True
    elasticsearch_user_name: str = ""
    elasticsearch_user_password: str = ""
    elasticsearch_all_mappings_json: str = "./resources/es_all_mappings.json"
    elasticsearch_study_mappings_json: str = "./resources/es_study_mappings.json"
    elasticsearch_compound_mappings_json: str = "./resources/es_compound_mappings.json"


def get_elasticsearch_settings(app) -> ElasticsearchSettings:
    settings = ElasticsearchSettings()
    if app.config:
        settings.elasticsearch_host = app.config.get("ELASTICSEARCH_HOST")
        settings.elasticsearch_port = app.config.get("ELASTICSEARCH_PORT")
        settings.elasticsearch_user_name = app.config.get("ELASTICSEARCH_USER_NAME")
        settings.elasticsearch_user_password = app.config.get("ELASTICSEARCH_USER_PASSWORD")
        settings.elasticsearch_use_tls = app.config.get("ELASTICSEARCH_USE_TLS")

    return settings
