from typing import Any, List, Union

from pydantic import BaseModel, ConfigDict, Field


class CommentDataItem(BaseModel):
    text: Union[None, str] = None
    date: Union[None, str] = None


class DataItem(BaseModel):
    data: Union[None, str] = None
    type: Union[None, str] = None
    source: Union[None, str] = None
    comments: List[CommentDataItem] = []


class StructureDataItem(BaseModel):
    structure: Union[None, str] = None
    type: Union[None, str] = None
    dimension: Union[None, str] = None
    defaultStructure: bool = True
    comments: List[CommentDataItem] = []


class OntologyDataItem(BaseModel):
    chebiName: Union[None, str] = None
    chebiId: Union[None, str] = None
    type: Union[None, str] = None
    status: Union[None, str] = None
    cyclicRelationship: bool = False
    comments: List[CommentDataItem] = []
    ontologyElement: List[Any] = []


class CompoundOriginDataItem(BaseModel):
    speciesText: Union[None, str] = None
    speciesAccession: Union[None, str] = None
    componentText: Union[None, str] = None
    componentAccession: Union[None, str] = None
    strainText: Union[None, str] = None
    strainAccession: Union[None, str] = None
    sourceType: Union[None, str] = None
    sourceAccession: Union[None, str] = None


class LiteEntity(BaseModel):
    chebiId: Union[None, str] = None
    chebiAsciiName: Union[None, str] = None
    entityStar: int = 0
    searchScore: Union[None, float] = 0.0


class Entity(BaseModel):
    chebiId: Union[None, str] = None
    chebiAsciiName: Union[None, str] = None
    definition: Union[None, str] = None
    status: Union[None, str] = None
    smiles: Union[None, str] = None
    inchi: Union[None, str] = None
    inchiKey: Union[None, str] = None
    charge: Union[None, str] = None
    mass: Union[None, str] = None
    monoisotopicMass: Union[None, str] = None
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

    model_config = ConfigDict(populate_by_name=True)
