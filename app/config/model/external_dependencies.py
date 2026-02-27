from pydantic import BaseModel


class ApiConfiguration(BaseModel):
    ols_api_url: str = "https://www.ebi.ac.uk/ols4/api"
    bioontology_api_url: str = "http://data.bioontology.org"
    zooma_api_url: str = "https://www.ebi.ac.uk/spot/zooma/v2/api"
    marine_species_api_url: str = "http://www.marinespecies.org/rest"
    europe_pmc_api_url: str = "https://www.ebi.ac.uk/europepmc/webservices/rest"
    policy_engine_url: str
    validation_service_url: str


class ExternalDependenciesSettings(BaseModel):
    api: ApiConfiguration
