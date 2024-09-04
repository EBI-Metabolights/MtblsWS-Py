from typing import  List, Union

from pydantic import BaseModel
from app.ws.db.models import MetaboLightsCompoundIndexModel, StudyModel


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
    text: Union[None, str] = ""
    facets: List[Facet] = []
    boosters: List[Booster] = []
    pagination: Pagination = Pagination()
    searchUser: SearchUser = SearchUser()
    
class SearchResult(BaseModel):
    query: SearchQuery = SearchQuery()
    reportLines: List[str] = []
    results: List[Union[MetaboLightsCompoundIndexModel, StudyModel]] = []