import datetime
from typing import List, Dict, Union

from pydantic import field_validator, BaseModel, Field, ConfigDict

from app.ws.db.utils import datetime_to_int


class StudyAccessPermission(BaseModel):
    userName: str = ""
    userRole: str = ""
    partner: bool = False
    submitterOfStudy: bool = False
    obfuscationCode: str = ""
    studyId: str = ""
    studyStatus: str = ""
    view: bool = False
    edit: bool = False
    delete: bool = False
    
    
class IndexedUserModel(BaseModel):
    firstName: Union[None, str] = Field(None)
    fullName: Union[None, str] = Field(None)  # assigned as not_analyzed in es
    lastName: Union[None, str] = Field(None)
    orcid: Union[None, str] = Field(None)
    userName: Union[None, str] = Field(None)  # assigned as not_analyzed in es
    address: Union[None, str] = Field(None)
    model_config = ConfigDict(from_attributes=True)


class StudyTaskModel(BaseModel):
    id: int = Field(0)
    study_acc: Union[None, str] = Field(None)
    task_name: Union[None, str] = Field(None)
    last_request_time: Union[None, datetime.datetime] = Field(None)
    last_request_executed: Union[None, datetime.datetime] = Field(None)
    last_execution_time: Union[None, datetime.datetime] = Field(None)
    last_execution_status: Union[None, str] = Field(None)
    last_execution_message: Union[None, str] = Field(None)
    # TODO[pydantic]: The following keys were removed: `json_encoders`.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-config for more information.
    model_config = ConfigDict(from_attributes=True, json_encoders={
        datetime.datetime: lambda v: v.timestamp(),
    })

class UserModel(BaseModel):
    address: Union[None, str] = Field(None, alias="address")  # excluded from es
    affiliation: Union[None, str] = Field(None, alias="affiliation")  # excluded from es
    affiliationUrl: Union[None, str] = Field(None, alias="affiliationurl")  # excluded from es
    curator: bool = False  # excluded from es
    dbPassword: str = Field(..., alias="password")  # excluded from es
    email: str = Field(..., alias="email")  # excluded from es
    firstName: Union[None, str] = Field(None, alias="firstname")
    fullName: Union[None, str] = Field(None)  # assigned as not_analyzed in es
    joinDate: Union[None, str, int, datetime.datetime] = Field(None, alias="joindate")  # excluded from es
    lastName: Union[None, str] = Field(None, alias="lastname")
    orcid: Union[None, str] = Field(None, alias="orcid")
    role: Union[int, str] = Field(..., alias="role")  # excluded from es
    status: Union[int, str] = Field(..., alias="status")  # excluded from es
    partner: Union[bool, int] = Field(..., alias="partner")  # excluded from es
    userId: int = Field(..., alias="id")  # excluded from es
    userName: str = Field(..., alias="username")  # assigned as not_analyzed in es
    userVerifyDbPassword: Union[None, str] = None  # not in es index mapping
    mobilePhoneNumber: Union[None, str] = None  # not in es index mapping
    officePhoneNumber: Union[None, str] = None  # not in es index mapping
    apiToken: Union[None, str] = Field(None, alias="apitoken")
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)
    @field_validator('joinDate', check_fields=False)
    @classmethod
    def datetime_validation(cls, value):
        if not value:
            return None 
        if isinstance(value, datetime.datetime):
            return value.isoformat()
        return value

    @field_validator('partner', check_fields=False)
    @classmethod
    def partner_validation(cls, value):
        try:
            return True if int(value) > 0 else False
        except Exception:
            return False
        
class UserLiteModel(BaseModel):
    firstName: str = ""
    lastName: str = ""
    affiliation: str = ""
    address: str = ""
    
class NewUserModel(BaseModel):
    
    userId: Union[None, int] = Field(None, alias="id")  # excluded from es
    email: str = Field(..., alias="email")  # excluded from es
    userName: str = Field(..., alias="username")  # assigned as not_analyzed in es
    dbPassword: str = Field(..., alias="password")  # excluded from es
    joinDate: Union[None, str, int, datetime.datetime] = Field(..., alias="joindate")  # excluded from es
    firstName: str = Field(..., alias="firstname")
    fullName: Union[None, str] = Field(None)  # assigned as not_analyzed in es
    lastName: str = Field(..., alias="lastname")
    address: str = Field(..., alias="address")  # excluded from es
    affiliation: str = Field(..., alias="affiliation")  # excluded from es
    affiliationUrl: str = Field(..., alias="affiliationurl")  # excluded from es
    status: Union[int, str] = Field(..., alias="status")  # excluded from es
    role: Union[int, str] = Field(..., alias="role")  # excluded from es
    apiToken: str = Field(..., alias="apitoken")
    orcid: Union[None, str] = Field(None, alias="orcid")
    userVerifyDbPassword: Union[None, str] = None  # not in es index mapping
    curator: bool = False  # excluded from es
    mobilePhoneNumber: Union[None, str] = None  # not in es index mapping
    officePhoneNumber: Union[None, str] = None  # not in es index mapping
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)
    
    @field_validator('joinDate', check_fields=False)
    @classmethod
    def datetime_validation(cls, value):
        if not value:
            return None 
        if isinstance(value, int):
            return datetime.datetime.fromtimestamp(value)
        return value


class MetabolightsParameterModel(BaseModel):
    name:   Union[None, str] = Field(..., alias="name")
    value:  Union[None, str] = Field(..., alias="value")
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

class MetabolightsStatisticsModel(BaseModel):
    id:   Union[None, int] = Field(..., alias="id")
    page_section:   Union[None, str] = Field(..., alias="page_section")
    str_name:   Union[None, str] = Field("", alias="str_name")
    str_value:   Union[None, str] = Field("", alias="str_value")
    sort_order:   Union[None, int] = Field("", alias="sort_order")
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

class SimplifiedUserModel(BaseModel):
    address: Union[None, str] = Field(None, alias="address")  # excluded from es
    affiliation: Union[None, str] = Field(None, alias="affiliation")  # excluded from es
    affiliationUrl: Union[None, str] = Field(None, alias="affiliationurl")  # excluded from es
    email: str = Field(..., alias="email")  # excluded from es
    firstName: Union[None, str] = Field(None, alias="firstname")
    fullName: Union[None, str] = Field(None)  # assigned as not_analyzed in es
    joinDate: Union[None, str, int, datetime.datetime] = Field(None, alias="joindate")  # excluded from es
    lastName: Union[None, str] = Field(None, alias="lastname")
    orcid: Union[None, str] = Field(None, alias="orcid")
    role: Union[int, str] = Field(..., alias="role")  # excluded from es
    status: Union[int, str] = Field(..., alias="status")  # excluded from es
    partner: Union[bool, int] = Field(..., alias="partner")  # excluded from es
    userName: str = Field(..., alias="username")  # assigned as not_analyzed in es
    apiToken: str = Field(..., alias="apitoken")  # excluded from es
    model_config = ConfigDict(from_attributes=True)

    @field_validator('joinDate', check_fields=False)
    @classmethod
    def partner_validation(cls, value):
        try:
            return True if int(value) > 0 else False
        except Exception:
            return False
    
    @field_validator('joinDate', check_fields=False)
    @classmethod
    def datetime_validation(cls, value):
        if not value:
            return None 
        if isinstance(value, int):
            return datetime.datetime.fromtimestamp(value)
        return value

class OrganismModel(BaseModel):
    organismName: Union[None, str] = None  # assigned as not_analyzed in es
    organismPart: Union[None, str] = None  # assigned as not_analyzed in es


class EntityModel(BaseModel):
    id: int
    organism: Union[None, List[OrganismModel]] = []


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
    model_config = ConfigDict(from_attributes=True)


class StudyFactorModel(BaseModel):
    name: Union[None, str] = None  # assigned as not_analyzed in es


class FieldModel(BaseModel):
    index: int = -1
    header: Union[None, str] = None
    fieldType: Union[None, str] = None
    description: Union[None, str] = None
    cleanHeader: Union[None, str] = None


class TableModel(BaseModel):
    data: List[List[str]] = []
    fields: Dict[str, FieldModel] = {}


class BackupModel(BaseModel):
    backupTimeStamp: Union[None, int] = None
    backupId: Union[None, str] = None
    folderPath: Union[None, str] = None


class StudyDesignDescriptor(BaseModel):
    description: Union[None, str] = None  # assigned as not_analyzed in es


class PublicationModel(BaseModel):
    abstractText: Union[None, str] = None  # not in es index mapping
    title: Union[None, str] = None
    doi: Union[None, str] = None
    pubmedId: Union[None, str] = None
    authorList: Union[None, str] = None


class ProtocolModel(BaseModel):
    name: Union[None, str] = None
    description: Union[None, str] = None


class SampleMeasurementModel(BaseModel):
    sampleName: Union[None, str] = None
    value: Union[None, str] = None


class MetaboliteAssignmentLine(BaseModel):
    identifier: Union[None, str] = None
    databaseIdentifier: Union[None, str] = None  # in es mapping
    unitId: Union[None, str] = None  # mzTab internal identificator
    chemicalFormula: Union[None, str] = None  # in es mapping
    smiles: Union[None, str] = None  # V2 field only                     # in es mapping
    inchi: Union[None, str] = None  # V2 field only                      # in es mapping
    metaboliteIdentification: Union[None, str] = None  # V2 field        # in es mapping
    chemicalShift: Union[None, str] = None
    multiplicity: Union[None, str] = None
    massToCharge: Union[None, str] = None  # in es mapping
    fragmentation: Union[None, str] = None
    modifications: Union[None, str] = None  # V2 field only
    charge: Union[None, str] = None
    retentionTime: Union[None, str] = None  # in es mapping
    taxid: Union[None, str] = None  # in es mapping
    species: Union[None, str] = None  # in es mapping
    database: Union[None, str] = None
    databaseVersion: Union[None, str] = None
    reliability: Union[None, str] = None  # in es mapping
    uri: Union[None, str] = None
    searchEngine: Union[None, str] = None
    searchEngineScore: Union[None, str] = None
    smallmoleculeAbundanceSub: Union[None, str] = None
    smallmoleculeAbundanceStdevSub: Union[None, str] = None
    smallmoleculeAbundanceStdErrorSub: Union[None, str] = None
    sampleMeasurements: Union[None, List[SampleMeasurementModel]] = []  # in es mapping
    assayName: Union[None, str] = None  # This is the name of the Assay record this MAF is assigned to


class MetaboliteAssignmentModel(BaseModel):
    metaboliteAssignmentFileName: Union[None, str] = None
    metaboliteAssignmentLines: List[MetaboliteAssignmentLine] = []


class IndexedAssayModel(BaseModel):
    measurement: Union[None, str] = None  # assigned as not_analyzed
    technology: Union[None, str] = None  # assigned as not_analyzed
    platform: Union[None, str] = None
    model_config = ConfigDict(from_attributes=True)


class AssayModel(BaseModel):
    measurement: Union[None, str] = None  # assigned as not_analyzed
    technology: Union[None, str] = None  # assigned as not_analyzed
    platform: Union[None, str] = None
    fileName: Union[None, str] = None  # excluded from es
    assayNumber: int = -1  # excluded from es
    metaboliteAssignment: Union[None, MetaboliteAssignmentModel] = None  # excluded from es
    assayTable: Union[None, TableModel] = None  # excluded from es


class ContactModel(BaseModel):
    lastName: Union[None, str] = None
    firstName: Union[None, str] = None
    email: Union[None, str] = None
    role: Union[None, str] = None
    midInitial: Union[None, str] = None
    phone: Union[None, str] = None
    fax: Union[None, str] = None
    affiliation: Union[None, str] = None
    address: Union[None, str] = None


class StudySummaryModel(BaseModel):
    id: Union[None, str] = Field(None, description="Study accession number")
    title: Union[None, str] = Field(None, description="Title of study")
    description: Union[None, str] = Field(None, description="Description of study")
    
    
class StudyDerivedData(BaseModel):
    submissionMonth: str = ""
    releaseMonth: str = ""
    submissionYear: int = 1970
    releaseYear: int = 1970
    country: str = ""
    organismNames: str = ""
    organismParts: str = ""
    
    
class LiteStudyModel(EntityModel):
    ObjectType: str = "Study"
    id: int = Field(...)
    studyIdentifier: str = Field(...)  # assigned as not_analyzed in es
    title: Union[None, str] = None

    studyDescription: Union[None, str] = None
    studyStatus: Union[None, str] = None  # assigned as not_analyzed in es
    studyPublicReleaseDate: Union[str, int] = Field(0)
    updateDate: Union[str, int] = Field(0)
    studySubmissionDate: Union[str, int] = Field(0)
    obfuscationCode: str = Field('')  # assigned as not_analyzed in es
    studySize: int = Field(0)
    validations: Union[None, ValidationEntriesModel] = None
    factors: List[StudyFactorModel] = []
    isatabErrorMessages: List[str] = []
    studyHumanReadable: Union[None, str] = None
    users: Union[List[UserModel], List[IndexedUserModel]] = []
    publicStudy: bool = False
    derivedData: Union[None, StudyDerivedData] = None
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class StudyModel(LiteStudyModel):
    indexTimestamp: int = 0
    description: Union[None, str] = None
    studyLocation: Union[None, str] = None  # excluded from es
    descriptors: List[StudyDesignDescriptor] = []
    publications: List[PublicationModel] = []
    protocols: List[ProtocolModel] = []
    assays: Union[List[AssayModel], List[IndexedAssayModel], None] = []
    contacts: Union[None, List[ContactModel]] = []  # excluded from es
    backups: Union[None, List[BackupModel]] = []
    sampleTable: Union[None, TableModel] = None  # excluded from es

    @field_validator('updateDate', 'studySubmissionDate', 'studyPublicReleaseDate')
    @classmethod
    def datetime_validation(cls, value):
        if not value:
            return None
        if isinstance(value, datetime.datetime):
            timestamp_value = datetime_to_int(value)
            return timestamp_value
        return value
    # TODO[pydantic]: The following keys were removed: `json_encoders`.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-config for more information.
    model_config = ConfigDict(from_attributes=True, populate_by_name=True, json_encoders={
        datetime.datetime: lambda v: v.timestamp()
    })
class SpeciesGroupModel(BaseModel):
    ObjectType: str = "SpeciesGroup"
    id: Union[None, int] = None
    name: Union[None, str] = None
    model_config = ConfigDict(from_attributes=True)

class SpeciesMembersModel(BaseModel):
    ObjectType: str = "SpeciesMembers"
    id: Union[None, int] = None
    taxon: Union[None, str] = None
    taxonDesc: Union[None, str] = Field("", alias="taxon_desc")
    parentMemberId: Union[None, int] = Field(None, alias="parent_id")
    speciesGroup: Union[None, SpeciesGroupModel] = Field(None, alias="group")
    model_config = ConfigDict(from_attributes=True)
class MetSpeciesModel(BaseModel):
    ObjectType: str = "Species"
    id: Union[None, int] = None
    description: Union[None, str] = None
    species: Union[None, str] = None
    taxon: Union[None, str] = None
    speciesMember: Union[None, SpeciesMembersModel] = Field(None, alias="ref_species_member")
    model_config = ConfigDict(from_attributes=True)

class MetDbModel(BaseModel):
    ObjectType: str = "Database"
    id: Union[None, int] = None
    name: Union[None, str] = Field(None, alias="db_name")
    model_config = ConfigDict(from_attributes=True)


class MetCrossReferenceModel(BaseModel):
    ObjectType: str = "CrossReference"
    id: Union[None, int] = None
    accession: Union[None, str] = Field(None, alias="acc")
    db: Union[None, MetDbModel] = None
    model_config = ConfigDict(from_attributes=True)


class MetSpeciesIndexModel(EntityModel):
    ObjectType: str = "MetSpecies"
    species: Union[None, MetSpeciesModel] = Field(None, alias="species")
    crossReference: Union[None, MetCrossReferenceModel] = Field(None, alias="cross_reference")
    model_config = ConfigDict(from_attributes=True)

class MetaboLightsCompoundModel(EntityModel):
    ObjectType: str = "compound"
    accession: Union[None, str] = Field(None, alias="acc")
    name: Union[None, str] = None
    description: Union[None, str] = None
    inchi: Union[None, str] = None
    inchikey: Union[None, str] = None
    chebiId: Union[None, str] = Field(None, alias="temp_id")
    formula: Union[None, str] = None
    iupacNames: Union[None, str] = Field(None, alias="iupac_names")
    studyStatus: str = 'PUBLIC'
    hasLiterature: bool = Field(False, alias="has_literature")
    hasReactions: bool = Field(False, alias="has_reactions")
    hasSpecies: bool = Field(False, alias="has_species")
    hasPathways: bool = Field(False, alias="has_pathways")
    hasNMR: bool = Field(False, alias="has_nmr")
    hasMS: bool = Field(False, alias="has_ms")
    updatedDate: Union[None, datetime.datetime] = Field(None, alias="updated_date")
    metSpecies: Union[None, List[MetSpeciesModel]] = Field([], alias="met_species")
    crossReference: Union[None, List[MetCrossReferenceModel]] = Field([], alias="ref_xref")

    @field_validator('updatedDate')
    @classmethod
    def datetime_validation(cls, value):
        if not value:
            return None
        if isinstance(value, datetime.datetime):
            return value.strftime("%d-%b-%Y %H:%M:%S")
        return value
    model_config = ConfigDict(from_attributes=True)
class MetAttributeDefinitionModel(EntityModel):
    ObjectType: str = "AttributeDefinition"
    value: Union[None, str] = None
    description: Union[None, str] = None
    model_config = ConfigDict(from_attributes=True)
class MetAttributeModel(EntityModel):
    ObjectType: str = "Attribute"
    value: Union[None, str] = None
    attributeDefinition: Union[None, MetAttributeDefinitionModel] = Field(None, alias="attribute_definition")
    model_config = ConfigDict(from_attributes=True)
class MetSpectraModel(EntityModel):
    ObjectType: str = "Spectra"
    name: Union[None, str] = None
    pathToJsonSpectra: Union[None, str] = Field(None, alias="path_to_json")
    spectraType: Union[None, str] = Field(None, alias="spectra_type")
    attributes: Union[None, List[MetAttributeModel]] = Field([], alias="attributes")
    model_config = ConfigDict(from_attributes=True)

class MetDb(EntityModel):
    ObjectType: str = "Database"
    db_name: str 
    model_config = ConfigDict(from_attributes=True)
class MetPathwayModel(EntityModel):
    ObjectType: str = "Pathway"
    name: Union[None, str] = None
    pathToPathwayFile: Union[None, str] = Field(None, alias="path_to_pathway_file")
    attributes: Union[None, List[MetAttributeModel]] = Field([], alias="attributes")
    database: Union[None, MetDb] = Field(None, alias="database")
    speciesAssociated: Union[None,  MetSpeciesModel] = Field(None, alias="species")
    model_config = ConfigDict(from_attributes=True)

  
    
class MetaboLightsCompoundIndexModel(EntityModel):
    ObjectType: str = "compound"
    accession: Union[None, str] = Field(None, alias="acc")
    name: Union[None, str] = None
    description: Union[None, str] = None
    inchi: Union[None, str] = None
    inchikey: Union[None, str] = None
    chebiId: Union[None, str] = Field(None, alias="temp_id")
    formula: Union[None, str] = None
    iupacNames: Union[None, str] = Field(None, alias="iupac_names")
    studyStatus: str = 'PUBLIC'
    hasLiterature: bool = Field(None, alias="has_literature")
    hasReactions: bool = Field(None, alias="has_reactions")
    hasSpecies: bool = Field(None, alias="has_species")
    hasPathways: bool = Field(None, alias="has_pathways")
    hasNMR: bool = Field(None, alias="has_nmr")
    hasMS: bool = Field(None, alias="has_ms")
    updatedDate: Union[None, datetime.datetime, str] = Field(None, alias="updated_date")
    metSpecies: Union[List[MetSpeciesIndexModel], None] = Field([], alias="met_species_index")
    crossReference: Union[List[MetCrossReferenceModel], None] = Field([], alias="ref_xref")
    
    metSpectras: Union[None, List[MetSpectraModel]] = Field([], alias="met_spectras")
    metPathways: Union[None, List[MetPathwayModel]] = Field([], alias="met_pathways")
    
    @field_validator('updatedDate', check_fields=False)
    @classmethod
    def datetime_validation(cls, value):
        if not value:
            return None 
        if isinstance(value, datetime.datetime):
            return value.strftime("%Y-%m-%d")
        return value
    model_config = ConfigDict(from_attributes=True)

class ESMetaboLightsCompound(MetaboLightsCompoundIndexModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)