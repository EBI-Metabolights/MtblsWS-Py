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


class SearchResource(Enum):
    CURATED = 1
    CTS = 2
    CHEBI = 3
    CHEMSPIDER = 4
    PUBCHEM = 5


class CompoundSearchResultModel(BaseModel):
    name: str = None
    inchi: str = None
    databaseId: str = None
    formula: str = None
    smiles: str = None
    search_resource: SearchResource = None

    def is_complete(self):
        if not self.name and not self.inchi and not self.formula and not self.smiles and not self.databaseId:
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


class CompoundSearchResonseModel(BaseModel):
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
