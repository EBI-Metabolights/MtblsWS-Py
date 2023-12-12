from enum import Enum
from typing import Mapping, List, Any

import flask
import humps
from pydantic.main import BaseModel

from app.config import get_settings


class CamelModel(BaseModel):
    class Config:
        alias_generator = humps.camelize
        allow_population_by_field_name = True


class RequestMeta(CamelModel):
    requested_schemas: List[str] = []
    api_version: str = get_settings().beacon.api_version

class MtblsBeaconSchemas(Enum):
    ANALYSES = {"entityType": "analysis", "schema": "beacon-analysis-v2.0.0"}
    BIOSAMPLES = {"entityType": "biosample", "schema": "beacon-dataset-v2.0.0"}
    COHORTS = {"entityType": "cohort", "schema": "beacon-cohort-v2.0.0"}
    DATASETS = {"entityType": "dataset", "schema": "beacon-dataset-v2.0.0"}
    FILTERINGTERMS = {"entityType": "filteringterms", "schema": "beacon-dataset-v2.0.0"}


class Granularity(Enum):
    BOOLEAN = "boolean",
    COUNT = "count",
    RECORD = "record"


class IncludeResultsetResponses(Enum):
    ALL = "ALL",
    HIT = "HIT",
    MISS = "MISS",
    NONE = "NONE"


class Pagination(CamelModel):
    skip: int = 0
    limit: int = 10


class RequestQuery(CamelModel):
    filters: List[dict] = []
    include_resultset_responses: IncludeResultsetResponses = IncludeResultsetResponses.HIT
    pagination: Pagination = Pagination()
    request_parameters: dict = {}
    test_mode: bool = False
    requested_granularity: Granularity = Granularity(get_settings().beacon.default_beacon_granularity)


class RequestParams(CamelModel):
    meta: RequestMeta = RequestMeta()
    query: RequestQuery = RequestQuery()

    def from_request(self, request: flask.Request):
        if request.method != "POST" or not request.data:
            for k, v in request.json.items():
                if k == "requestedSchema":
                    self.meta.requested_schemas = [v]
                elif k == "skip":
                    self.query.pagination.skip = int(v)
                elif k == "limit":
                    self.query.pagination.limit = int(v)
                elif k == "includeResultsetResponses":
                    self.query.include_resultset_responses = IncludeResultsetResponses(v)
                else:
                    self.query.request_parameters[k] = v
        return self

    def summary(self):
        list_of_filters=[]
        for item in self.query.filters:
            for k,v in item.items():
                list_of_filters.append(v)
        return {
            "apiVersion": self.meta.api_version,
            "requestedSchemas": self.meta.requested_schemas,
            "filters": list_of_filters,
            "requestParameters": self.query.request_parameters,
            "includeResultsetResponses": self.query.include_resultset_responses,
            "pagination": self.query.pagination.dict(),
            "requestedGranularity": self.query.requested_granularity,
            "testMode": self.query.test_mode
        }
