from pydantic import BaseModel


class ApiConfiguration(BaseModel):
    ols_api_url: str
    bioontology_api_url: str
    zooma_api_url: str
    marine_species_api_url: str
    europe_pmc_api_url: str


class ExternalDependenciesSettings(BaseModel):
    api: ApiConfiguration