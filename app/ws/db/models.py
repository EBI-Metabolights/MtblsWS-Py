import datetime
from typing import Optional, List, Dict, Set, Union

from pydantic import BaseModel, Field, validator

from app.ws.db.utils import datetime_to_int


class IndexedUserModel(BaseModel):
    firstName: str = Field(None)
    fullName: str = Field(None)  # assigned as not_analyzed in es
    lastName: str = Field(None)
    orcid: str = Field(None)
    userName: str = Field(None)  # assigned as not_analyzed in es
    address: str = Field(None)

    class Config:
        orm_mode = True


class StudyTaskModel(BaseModel):
    id: int = Field(0)
    study_acc: str = Field(None)
    task_name: str = Field(None)
    last_request_time: datetime.datetime = Field(None)
    last_request_executed: datetime.datetime = Field(None)
    last_execution_time: datetime.datetime = Field(None)
    last_execution_status: str = Field(None)
    last_execution_message: str = Field(None)

    class Config:
        orm_mode = True
        json_encoders = {
            datetime.datetime: lambda v: v.timestamp(),
        }

class UserModel(BaseModel):
    address: str = Field(None, alias="address")  # excluded from es
    affiliation: str = Field(None, alias="affiliation")  # excluded from es
    affiliationUrl: str = Field(None, alias="affiliationurl")  # excluded from es
    curator: bool = False  # excluded from es
    dbPassword: str = Field(..., alias="password")  # excluded from es
    email: str = Field(..., alias="email")  # excluded from es
    firstName: str = Field(None, alias="firstname")
    fullName: str = Field(None)  # assigned as not_analyzed in es
    joinDate: datetime.datetime = Field(None, alias="joindate")  # excluded from es
    lastName: str = Field(None, alias="lastname")
    orcid: str = Field(None, alias="orcid")
    role: Union[int, str] = Field(..., alias="role")  # excluded from es
    status: Union[int, str] = Field(..., alias="status")  # excluded from es
    userId: int = Field(..., alias="id")  # excluded from es
    userName: str = Field(..., alias="username")  # assigned as not_analyzed in es
    userVerifyDbPassword: str = None  # not in es index mapping
    mobilePhoneNumber: str = None  # not in es index mapping
    officePhoneNumber: str = None  # not in es index mapping
    apiToken: str = None  # excluded from es

    class Config:
        orm_mode = True


class SimplifiedUserModel(BaseModel):
    address: str = Field(None, alias="address")  # excluded from es
    affiliation: str = Field(None, alias="affiliation")  # excluded from es
    affiliationUrl: str = Field(None, alias="affiliationurl")  # excluded from es
    email: str = Field(..., alias="email")  # excluded from es
    firstName: str = Field(None, alias="firstname")
    fullName: str = Field(None)  # assigned as not_analyzed in es
    joinDate: datetime.datetime = Field(None, alias="joindate")  # excluded from es
    lastName: str = Field(None, alias="lastname")
    orcid: str = Field(None, alias="orcid")
    role: Union[int, str] = Field(..., alias="role")  # excluded from es
    status: Union[int, str] = Field(..., alias="status")  # excluded from es
    userName: str = Field(..., alias="username")  # assigned as not_analyzed in es
    apiToken: str = Field(..., alias="apitoken")  # excluded from es
    class Config:
        orm_mode = True


class OrganismModel(BaseModel):
    organismName: str = None  # assigned as not_analyzed in es
    organismPart: str = None  # assigned as not_analyzed in es


class EntityModel(BaseModel):
    id: int
    organism: List[OrganismModel] = []


class ValidationEntryModel(BaseModel):
    id: int = 0
    description: str = ''
    status: str = 'RED'
    statusExt: str = ''
    group: str = 'STUDY'
    passedRequirement: bool = True
    type: str = 'MANDATORY'
    message: str = "OK"
    overriden: bool = False


class ValidationEntriesModel(BaseModel):
    entries: List[ValidationEntryModel] = []
    status: str = 'GREEN'
    passedMinimumRequirement: bool = False
    overriden: bool = False

    class Config:
        orm_mode = True


class StudyFactorModel(BaseModel):
    name: str = None  # assigned as not_analyzed in es


class FieldModel(BaseModel):
    index: int = -1
    header: str = None
    fieldType: str = None
    description: str = None
    cleanHeader: str = None


class TableModel(BaseModel):
    data: List[List[str]] = []
    fields: Dict[str, FieldModel] = {}


class BackupModel(BaseModel):
    backupTimeStamp: int = None
    backupId: str = None
    folderPath: str = None


class StudyDesignDescriptor(BaseModel):
    description: str = None  # assigned as not_analyzed in es


class PublicationModel(BaseModel):
    abstractText: str = None  # not in es index mapping
    title: str = None
    doi: str = None
    pubmedId: str = None
    authorList: str = None


class ProtocolModel(BaseModel):
    name: str = None
    description: str = None


class SampleMeasurementModel(BaseModel):
    sampleName: str = None
    value: str = None


class MetaboliteAssignmentLine(BaseModel):
    identifier: str = None
    databaseIdentifier: str = None  # in es mapping
    unitId: str = None  # mzTab internal identificator
    chemicalFormula: str = None  # in es mapping
    smiles: str = None  # V2 field only                     # in es mapping
    inchi: str = None  # V2 field only                      # in es mapping
    metaboliteIdentification: str = None  # V2 field        # in es mapping
    chemicalShift: str = None
    multiplicity: str = None
    massToCharge: str = None  # in es mapping
    fragmentation: str = None
    modifications: str = None  # V2 field only
    charge: str = None
    retentionTime: str = None  # in es mapping
    taxid: str = None  # in es mapping
    species: str = None  # in es mapping
    database: str = None
    databaseVersion: str = None
    reliability: str = None  # in es mapping
    uri: str = None
    searchEngine: str = None
    searchEngineScore: str = None
    smallmoleculeAbundanceSub: str = None
    smallmoleculeAbundanceStdevSub: str = None
    smallmoleculeAbundanceStdErrorSub: str = None
    sampleMeasurements: List[SampleMeasurementModel] = []  # in es mapping
    assayName: str = None  # This is the name of the Assay record this MAF is assigned to


class MetaboliteAssignmentModel(BaseModel):
    metaboliteAssignmentFileName: str = None
    metaboliteAssignmentLines: List[MetaboliteAssignmentLine] = []


class IndexedAssayModel(BaseModel):
    measurement: str = None  # assigned as not_analyzed
    technology: str = None  # assigned as not_analyzed
    platform: str = None

    class Config:
        orm_mode = True


class AssayModel(BaseModel):
    measurement: str = None  # assigned as not_analyzed
    technology: str = None  # assigned as not_analyzed
    platform: str = None
    fileName: str = None  # excluded from es
    assayNumber: int = -1  # excluded from es
    metaboliteAssignment: MetaboliteAssignmentModel = None  # excluded from es
    assayTable: TableModel = None  # excluded from es


class ContactModel(BaseModel):
    lastName: str = None
    firstName: str = None
    email: str = None
    role: str = None
    midInitial: str = None
    phone: str = None
    fax: str = None
    affiliation: str = None
    address: str = None


class StudySummaryModel(BaseModel):
    id: str = Field(None, description="Study accession number")
    title: str = Field(None, description="Title of study")
    description: str = Field(None, description="Description of study")
    
    
class StudyDerivedData(BaseModel):
    submissionMonth: str = ""
    releaseMonth: str = ""
    submissionYear: str = 1970
    releaseYear: str = 1970
    country: str = ""
    organismNames: str = ""
    organismParts: str = ""
    
    
class LiteStudyModel(EntityModel):
    id: int = Field(...)
    studyIdentifier: str = Field(...)  # assigned as not_analyzed in es
    title: Optional[str] = None

    studyDescription: str = None
    studyStatus: str = None  # assigned as not_analyzed in es
    studyPublicReleaseDate: int = Field(0)
    updateDate: int = Field(0)
    studySubmissionDate: int = Field(0)
    obfuscationCode: str = Field(...)  # assigned as not_analyzed in es
    studySize: int = Field(0)
    validations: ValidationEntriesModel = None
    factors: List[StudyFactorModel] = []
    isatabErrorMessages: List[str] = []
    studyHumanReadable: str = None
    users: Union[List[UserModel], List[IndexedUserModel]] = []
    publicStudy: bool = False
    derivedData: StudyDerivedData = None
    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class StudyModel(LiteStudyModel):
    indexTimestamp: int = 0
    ObjectType: str = "study"
    description: Optional[str]
    studyLocation: Optional[str]  # excluded from es
    descriptors: List[StudyDesignDescriptor] = []
    publications: List[PublicationModel] = []
    protocols: List[ProtocolModel] = []
    assays: Union[List[AssayModel], List[IndexedAssayModel]] = []
    contacts: List[ContactModel] = []  # excluded from es
    backups: List[BackupModel] = []
    sampleTable: TableModel = None  # excluded from es

    @validator('updateDate', 'studySubmissionDate', 'studyPublicReleaseDate')
    def datetime_validation(cls, value):
        if not value:
            return None
        if isinstance(value, datetime.datetime):
            timestamp_value = datetime_to_int(value)
            return timestamp_value
        return value

    class Config:
        orm_mode = True
        allow_population_by_field_name = True

        json_encoders = {
            datetime.datetime: lambda v: v.timestamp()
        }
class SpeciesGroupModel(BaseModel):
    id: int = None
    name: str = None

    class Config:
        orm_mode = True

class SpeciesMembersModel(BaseModel):
    id: int = None
    taxon: str = None
    taxonDesc: Optional[str] = Field("", alias="taxon_desc")
    parentMemberId: int = Field(None, alias="parent_id")
    speciesGroup: SpeciesGroupModel = Field(None, alias="group")

    class Config:
        orm_mode = True
class MetSpeciesModel(BaseModel):
    id: int = None
    description: str = None
    species: str = None
    taxon: str = None
    speciesMember: SpeciesMembersModel = Field(None, alias="ref_species_member")

    class Config:
        orm_mode = True

class MetaboLightsModel(BaseModel):
    id: int
    accession: str = Field(None, alias="acc")
    name: str = None
    description: str = None
    inchi: str = None
    inchikey: str = None
    chebiId: str = Field(None, alias="temp_id")
    formula: str = None
    iupacNames: str = Field(None, alias="iupac_names")
    studyStatus: str = 'PUBLIC'
    hasLiterature: bool = Field(None, alias="has_literature")
    hasReactions: bool = Field(None, alias="has_reactions")
    hasSpecies: bool = Field(None, alias="has_species")
    hasPathways: bool = Field(None, alias="has_pathways")
    hasNMR: bool = Field(None, alias="has_nmr")
    hasMS: bool = Field(None, alias="has_ms")
    updatedDate: str = None
    metSpecies: List[MetSpeciesModel] = []
    class Config:
        orm_mode = True