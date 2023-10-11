from pydantic import BaseModel


class ApiConfiguration(BaseModel):
    ols_api_url: str
    bioontology_api_url: str


class ExternalDependenciesSettings(BaseModel):
    api: ApiConfiguration