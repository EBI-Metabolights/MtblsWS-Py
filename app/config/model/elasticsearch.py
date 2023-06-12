from pydantic import BaseModel


class ElasticsearchConnection(BaseModel):
    host: str
    port: int = 9200
    username: str
    password: str
    use_tls: bool = False


class ElasticsearchConfiguration(BaseModel):
    elasticsearch_all_mappings_json: str = "./resources/es_all_mappings.json"
    elasticsearch_study_mappings_json: str = "./resources/es_study_mappings.json"
    elasticsearch_compound_mappings_json: str = "./resources/es_compound_mappings.json"


class ElasticsearchSettings(BaseModel):
    connection: ElasticsearchConnection
    configuration: ElasticsearchConfiguration
