import datetime
from typing import Optional, List, Dict

from pydantic import BaseModel, Field, validator

from app.ws.db.utils import datetime_to_int


class UserModel(BaseModel):
    userId: int = Field(..., alias="id")
    email: str = Field(..., alias="email")
    userName: str = Field(..., alias="username")
    dbPassword: str = Field(..., alias="password")
    userVerifyDbPassword: str = None
    joinDate: datetime.datetime = Field(None, alias="joindate")
    firstName: str = Field(None, alias="firstname")
    lastName: str = Field(None, alias="lastname")
    address: str = Field(None, alias="address")
    affiliation: str = Field(None, alias="affiliation")
    affiliationUrl: str = Field(None, alias="affiliationurl")
    status: str = Field(..., alias="status")
    role: str = Field(..., alias="role")
    orcid: str = Field(None, alias="orcid")
    mobilePhoneNumber: str = None
    officePhoneNumber: str = None
    curator: bool = False
    fullName: str = None

    class Config:
        orm_mode = True


class OrganismModel(BaseModel):
    organismName: str = None
    organismPart: str = None


class EntityModel(BaseModel):
    id: int
    organism: List[OrganismModel] = []


class ValidationEntryModel(BaseModel):
    id: int
    description: str = None
    status: str
    statusExt: str = None
    group: str = None
    passedRequirement: bool
    type: str = None
    message: str = None
    overriden: bool


class ValidationEntriesModel(BaseModel):
    entries: List[ValidationEntryModel] = []
    status: str
    passedMinimumRequirement: bool
    overriden: bool

    class Config:
        orm_mode = True


class StudyFactorModel(BaseModel):
    name: str = None


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
    backupTimeStamp: datetime.datetime
    backupId: str
    folderPath: str


class StudyDesignDescriptor(BaseModel):
    description: str = None


class PublicationModel(BaseModel):
    abstractText: str = None
    title: str = None
    doi: str = None
    pubmedId: str = None
    authorList: str = None


class ProtocolModel(BaseModel):
    name: str = None
    Description: str = None


class SampleMeasurementModel(BaseModel):
    sampleName: str = None
    value: str = None


class MetaboliteAssignmentLine(BaseModel):
    identifier: str = None
    databaseIdentifier: str = None
    unitId: str = None # mzTab internal identificator
    chemicalFormula: str = None
    smiles: str  = None # V2 field only
    inchi: str  = None # V2 field only
    metaboliteIdentification: str  = None # V2 field
    chemicalShift: str = None
    multiplicity: str = None
    massToCharge: str = None
    fragmentation: str = None
    modifications: str = None  # V2 field only
    charge: str = None
    retentionTime: str = None
    taxid: str = None
    species: str = None
    database: str = None
    databaseVersion: str = None
    reliability: str = None
    uri: str = None
    searchEngine: str = None
    searchEngineScore: str = None
    smallmoleculeAbundanceSub: str = None
    smallmoleculeAbundanceStdevSub: str = None
    smallmoleculeAbundanceStdErrorSub: str = None
    sampleMeasurements: List[SampleMeasurementModel] = []
    assayName: str = None# This is the name of the Assay record this MAF is assigned to


class MetaboliteAssignmentModel(BaseModel):
    metaboliteAssignmentFileName: str = None
    metaboliteAssignmentLines: List[MetaboliteAssignmentLine] = []


class AssayModel(BaseModel):
    measurement: str = None
    technology: str = None
    platform: str = None
    fileName: str = None
    assayNumber: int = -1
    metaboliteAssignment: MetaboliteAssignmentModel = None
    assayTable: TableModel = None


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


class LiteStudyModel(EntityModel):
    id: int = Field(...)
    studyIdentifier: str = Field(...)
    title: Optional[str] = None

    studyDescription: str = None
    studyStatus: str = None
    studyPublicReleaseDate: int = Field(0)
    updateDate: int = Field(0)
    studySubmissionDate: int = Field(0)
    obfuscationCode: str = Field(...)
    studySize: int = Field(0)
    validations: ValidationEntriesModel = None
    factors: List[StudyFactorModel] = []
    isatabErrorMessages: List[str] = []
    studyHumanReadable: str = None
    users: List[UserModel] = []
    publicStudy: bool = False

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class StudyModel(LiteStudyModel):
    description: Optional[str]
    studyLocation: Optional[str]
    descriptors: List[StudyDesignDescriptor] = []
    publications: List[PublicationModel] = []
    protocols: List[ProtocolModel] = []
    assays: List[AssayModel] = []
    contacts: List[ContactModel] = []
    backups: List[BackupModel] = []
    sampleTable: TableModel = None

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
