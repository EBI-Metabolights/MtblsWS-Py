from typing import List, Any, Optional

from pydantic import BaseModel, Field


class CommentDataItem(BaseModel):
    text: str = None
    date: str = None


class DataItem(BaseModel):
    data: str = None
    type: str = None
    source: str = None
    comments: List[CommentDataItem] = []


class StructureDataItem(BaseModel):
    structure: str = None
    type: str = None
    dimension: str = None
    defaultStructure: bool = True
    comments: List[CommentDataItem] = []


class OntologyDataItem(BaseModel):
    chebiName: str = None
    chebiId: str = None
    type: str = None
    status: str = None
    cyclicRelationship: bool = False
    comments: List[CommentDataItem] = []
    ontologyElement: List[Any] = []


class CompoundOriginDataItem(BaseModel):
    speciesText: str = None
    speciesAccession: str = None
    componentText: str = None
    componentAccession: str = None
    strainText: str = None
    strainAccession: str = None
    sourceType: str = None
    sourceAccession: str = None


class LiteEntity(BaseModel):
    chebiId: str = None
    chebiAsciiName: str = None
    entityStar: int = 0
    searchScore: Optional[float] = 0


class Entity(BaseModel):
    chebiId: str = None
    chebiAsciiName: str = None
    definition: str = None
    status: str = None
    smiles: str = None
    inchi: str = None
    inchiKey: str = None
    charge: str = None
    mass: str = None
    monoisotopicMass: str = None
    entityStar: int = 0

    secondaryChEBIIds: List[str] = Field([], alias="SecondaryChEBIIds")
    synonyms: List[DataItem] = Field([], alias="Synonyms")
    iupacNames: List[DataItem] = Field([], alias="IupacNames")
    formulae: List[DataItem] = Field([], alias="Formulae")
    registryNumbers: List[DataItem] = Field([], alias="RegistryNumbers")
    citations: List[DataItem] = Field([], alias="Citations")
    chemicalStructures: List[StructureDataItem] = Field([], alias="ChemicalStructures")
    databaseLinks: List[DataItem] = Field([], alias="DatabaseLinks")
    ontologyParents: List[OntologyDataItem] = Field([], alias="OntologyParents")
    ontologyChildren: List[OntologyDataItem] = Field([], alias="OntologyChildren")
    generalComments: List[CommentDataItem] = Field([], alias="GeneralComments")
    compoundOrigins: List[CompoundOriginDataItem] = Field([], alias="CompoundOrigins")

    class Config:
        allow_population_by_field_name = True