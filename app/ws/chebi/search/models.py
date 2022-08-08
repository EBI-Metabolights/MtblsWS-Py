from enum import Enum
from typing import List

from pydantic import BaseModel, Field


class CuratedMetabolitesFileColumn(Enum):
    CHEBI_ID = 0
    MOLECULAR_FORMULA = 1
    SMILES = 2
    INCHI = 4
    COMPOUND_NAME = 5
    PRIORITY = 6


class SearchResource(str, Enum):
    CURATED = "CURATED"
    CTS = "CTS"
    CHEBI = "CHEBI"
    CHEMSPIDER = "CHEMSPIDER"
    PUBCHEM = "PUBCHEM"


class CompoundSearchResultModel(BaseModel):
    name: str = None
    inchi: str = None
    databaseId: str = None
    formula: str = None
    smiles: str = None
    search_resource: SearchResource = None

    def is_complete(self):
        if self.name and self.inchi and self.formula and self.smiles and self.databaseId:
            return True
        return False

    def score(self):
        score = 0
        if self.name:
            score = score + 1000000
        if self.inchi:
            score = score + 100000
        if self.formula:
            score = score + 10000
        if self.smiles:
            score = score + 1000
        if self.databaseId:
            score = score + 100
            if "chebi" in self.databaseId.lower():
                score = score + 10
        return score


class CompoundSearchResponseModel(BaseModel):
    content: List[CompoundSearchResultModel] = Field([],
                                                     description="""
                         This field contains data of service result
                         """)
    message: str = Field(None,
                         description="""
                         This field contains message about content and service result
                         """)
    err: str = Field(None,
                     description="""
                     This field contains error message details, if service returns error code
                     """)
