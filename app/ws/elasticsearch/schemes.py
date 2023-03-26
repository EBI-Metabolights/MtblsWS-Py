

import datetime
from typing import Any, Optional, List, Dict, Set, Union

from pydantic import BaseModel, Field, validator
from app.ws.db.models import MetaboLightsCompoundIndexModel, StudyModel
from app.ws.db.types import UserStatus

from app.ws.db.utils import datetime_to_int


class Booster(BaseModel):
    fieldName: str = ""
    boost: float = 1

class FacetLine(BaseModel):
    value: str = ""
    checked: bool = False
    count: int = 1
    
class Facet(BaseModel):
    name: str = ""
    lines: List[FacetLine] = []
    
class Pagination(BaseModel):
    page: int = 1
    itemsCount: int = 0
    pageSize: int = 10


class SearchUser(BaseModel):
    id: str = "metabolights-anonymous"
    isAdmin: bool = False

class SearchQuery(BaseModel):
    text: Optional[str] = ""
    facets: List[Facet] = []
    boosters: List[Booster] = []
    pagination: Pagination = Pagination()
    searchUser: SearchUser = SearchUser()
    
class SearchResult(BaseModel):
    query: SearchQuery = SearchQuery()
    reportLines: List[str] = []
    results: List[Union[MetaboLightsCompoundIndexModel, StudyModel]] = []