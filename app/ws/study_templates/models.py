import datetime
import enum
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.alias_generators import to_camel, to_pascal


class MetadataFileType(enum.StrEnum):
    ASSAY = "assay"
    SAMPLE = "sample"
    INVESTIGATION = "investigation"
    ASSIGNMENT = "assignment"


class OntologyValidationType(enum.StrEnum):
    ANY_ONTOLOGY_TERM = "any-ontology-term"
    CHILD_ONTOLOGY_TERM = "child-ontology-term"
    SELECTED_ONTOLOGY = "ontology-term-in-selected-ontologies"
    SELECTED_ONTOLOGY_TERM = "selected-ontology-term"
    ONLY_CHECK_CONSTRAINTS = "check-only-constraints"


class ConstraintType(enum.StrEnum):
    PATTERN = "pattern"
    MINIMUM = "minimum"
    MAXIMUM = "maximum"
    REQUIRED = "required"


class EnforcementLevel(enum.StrEnum):
    REQUIRED = "required"
    RECOMMENDED = "recommended"
    OPTIONAL = "optional"
    NOT_APPLICABLE = "not-applicable"


class StudyCategoryStr(enum.StrEnum):
    OTHER = "other"
    MS_MHD_ENABLED = "ms-mhd-enabled"
    MS_IMAGING = "ms-imaging"
    MS_OTHER = "ms-other"
    NMR = "nmr"
    MS_MHD_LEGACY = "ms-mhd-legacy"


class StudyBaseModel(BaseModel):
    """Base model class to convert python attributes to camel case"""

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        json_schema_serialization_defaults_required=True,
        field_title_generator=lambda field, field_info: to_pascal(
            field.replace("_", " ").strip()
        ),
    )


class OntologyTerm(StudyBaseModel):
    term: Annotated[str, Field(description="Ontology term")]

    term_accession_number: Annotated[
        str,
        Field(
            description="The accession number from the Term Source "
            "associated with the term.",
        ),
    ]
    term_source_ref: Annotated[
        str,
        Field(description="Source reference name of ontology term. e.g., EFO, OBO."),
    ]

    def __str__(self):
        return f"[{self.term}, {self.term_source_ref}, {self.term_accession_number}]"


class OntologyTermPlaceholder(StudyBaseModel):
    term_accession_number: Annotated[
        str,
        Field(description="The accession number of placeholder."),
    ]
    term_source_ref: Annotated[
        str,
        Field(description="Source reference name of placeholder. e.g., MTBLS."),
    ]

    def __str__(self):
        return f"[{self.term_source_ref}, {self.term_accession_number}]"


class FieldSelector(StudyBaseModel):
    name: Annotated[
        str,
        Field(description="Node name. e.g., Sample Name, Source Name, Protocol REF"),
    ]
    value: Annotated[
        None | str,
        Field(
            description="Node value to match."
            " e.g. Protocol REF with value 'Sample collection'"
        ),
    ]


class SelectionCriteria(StudyBaseModel):
    isa_file_type: Annotated[
        None | MetadataFileType,
        Field(description="ISA-TAB file type."),
    ] = None
    study_created_at_or_after: Annotated[
        None | datetime.datetime,
        Field(description="Filter to select studies created after the defined date."),
    ] = None
    study_created_before: Annotated[
        None | datetime.datetime,
        Field(description="Filter to select studies created before the defined date."),
    ] = None
    study_category_filter: Annotated[
        None | list[StudyCategoryStr],
        Field(description="Filter to select studies with the defined category"),
    ] = None
    template_version_filter: Annotated[
        None | list[str],
        Field(description="Filter to select studies with the defined template version"),
    ] = None
    isa_file_template_name_filter: Annotated[
        None | list[str],
        Field(
            description="Filter to select ISA-TAB file template. "
            "LC-MS, GC-MS, etc. for assay, minimum, clinical, etc. for sample"
        ),
    ] = None
    linked_field_and_value_filter: Annotated[
        None | list[FieldSelector],
        Field(
            description="Filter by linked field and its value. "
            "Current rule will be selected if the field "
            "is an attribute of ISA-TAB node (Sample Name, Source Name, Protocol REF)."
            "Characteristics can be linked to Sample Name or Source Name. "
            "Parameter Value can be linked to Protocol REF with value. "
            "e.g., {name: 'Protocol REF', 'value': 'Mass spectrometry'}, "
            "Units can be linked to Parameter Value, Factor Value "
            "and Characteristic fields."
            "Comments can be linked to ISA-TAB nodes "
            "(Sample Name, Source Name, Protocol REF, etc.)"
        ),
    ] = None


class AdditionalSource(StudyBaseModel):
    source_label: Annotated[
        str, Field(description="Source label. e.g., ILX, wikidata, etc.")
    ]
    accession_prefix: Annotated[
        str,
        Field(
            description="Prefix for the values from source. "
            "e.g., https://www.wikidata.org/wiki/, http://uri.interlex.org/base/ilx_"
        ),
    ]

    def __str__(self):
        return f"[{self.source_label}, {self.accession_prefix}]"


class FieldConstraint(StudyBaseModel):
    constraint: Annotated[
        None | str | int | float | bool, Field(description="Constraint value")
    ] = None
    error_message: Annotated[
        str,
        Field(description="Error message if value does not satisfy the constraint."),
    ] = ""
    enforcement_level: Annotated[
        EnforcementLevel,
        Field(description="Rule enforcement level for the constraint."),
    ] = EnforcementLevel.REQUIRED


class ParentOntologyTerms(StudyBaseModel):
    exclude_by_label_pattern: Annotated[
        None | list[str],
        Field(description="Label match regex patterns to filter ontology terms."),
    ] = []
    exclude_by_accession: Annotated[
        None | list[str],
        Field(description="Accession numbers of the excluded ontology terms."),
    ] = []
    parents: Annotated[
        list[OntologyTerm],
        Field(description="List of parent ontology terms"),
    ] = []


class BaseOntologyValidation(StudyBaseModel):
    rule_name: Annotated[
        str,
        Field(
            description="Unique name id for the the field. "
            "Rule naming convention is <Field name>-<incremental number>. "
            "e.g., Parameter Value[Instrument]-01, Parameter Value[Instrument]-02"
        ),
    ]
    field_name: Annotated[
        str,
        Field(
            description="Name of the column header or investigation field name. "
            "e.g., Parameter Value[Instrument], Study Assay Measurement Type."
        ),
    ]

    ontology_validation_type: Annotated[
        None | OntologyValidationType, Field(description="Validation rule type")
    ] = OntologyValidationType.ANY_ONTOLOGY_TERM

    ontologies: Annotated[
        None | list[str],
        Field(
            description="Ordered ontology source references. "
            "If validation type is ontology-term-in-selected-ontologies, "
            "it defines ontology sources, "
            "otherwise it lists recommended ontology sources."
        ),
    ] = None

    allowed_parent_ontology_terms: Annotated[
        None | ParentOntologyTerms,
        Field(
            description="Parent ontology terms "
            "to find the allowed child ontology terms. "
            "Applicable only for validation type child-ontology-term"
        ),
    ] = None


class FieldValueValidation(BaseOntologyValidation):
    description: Annotated[
        str,
        Field(description="Definition of rule and summary of selection criteria."),
    ] = ""
    selection_criteria: Annotated[
        SelectionCriteria, Field(description="Field selection criteria")
    ]
    term_enforcement_level: Annotated[
        EnforcementLevel, Field(description="Rule enforcement level for ontology terms")
    ] = EnforcementLevel.REQUIRED
    unexpected_term_enforcement_level: Annotated[
        EnforcementLevel,
        Field(description="Rule enforcement level for unexpected terms"),
    ] = EnforcementLevel.REQUIRED

    validation_type: Annotated[
        OntologyValidationType, Field(description="Validation rule type")
    ] = OntologyValidationType.ANY_ONTOLOGY_TERM
    constraints: Annotated[
        None | dict[ConstraintType, FieldConstraint],
        Field(description="Field constraints"),
    ] = {}
    default_value: Annotated[
        None | OntologyTerm, Field(description="Default ontology term")
    ] = None
    allowed_missing_ontology_terms: Annotated[
        None | list[OntologyTerm], Field(description="Allowed missing ontology terms")
    ] = []
    allowed_other_sources: Annotated[
        None | list[AdditionalSource],
        Field(description="Allowed values from other non ontology sources."),
    ] = []
    allowed_placeholders: Annotated[
        None | list[OntologyTermPlaceholder],
        Field(description="Allowed placeholders for term source and accession"),
    ] = []
    terms: Annotated[
        None | list[OntologyTerm],
        Field(
            description="Selected ontology terms. "
            "If validation type is selected-ontology-term, "
            "it defines ordered allowed ontology terms, "
            "otherwise it lists ordered and recommended ontology terms."
        ),
    ] = []
    ontologies: Annotated[
        None | list[str],
        Field(
            description="Ordered ontology source references. "
            "If validation type is ontology-term-in-selected-ontologies, "
            "it defines ontology sources, "
            "otherwise it lists recommended ontology sources."
        ),
    ] = []

    allowed_parent_ontology_terms: Annotated[
        None | ParentOntologyTerms,
        Field(
            description="Parent ontology terms to "
            "find the allowed child ontology terms. "
            "Applicable only for validation type child-ontology-term"
        ),
    ] = None

    unexpected_terms: Annotated[
        None | list[str],
        Field(description="unexpected terms."),
    ] = []

    @model_validator(mode="wrap")
    @classmethod
    def validate_model(cls, v: Any, handler) -> Self:
        if isinstance(v, dict):
            enforcement = v.get("enforcementLevel", None)
            if enforcement:
                v["termEnforcementLevel"] = enforcement
            constraints = v.get("constraints", None)
            if isinstance(constraints, list):
                new_constraints = {}
                for item in constraints:
                    new_constraints[item.get("type")] = item
                v["constraints"] = new_constraints
        validation_type = v.get("validationType", None)
        if validation_type == "check-only-constraints":
            v["termEnforcementLevel"] = EnforcementLevel.NOT_APPLICABLE

        return handler(v)


class ColumnDescription(StudyBaseModel):
    column_structure: Annotated[
        Literal["SINGLE_COLUMN", "ONTOLOGY_COLUMN", "SINGLE_COLUMN_AND_UNIT_ONTOLOGY"],
        Field(description="column structure"),
    ]
    column_category: Annotated[
        None
        | Literal[
            "", "Basic", "Protocol", "Parameter", "Characteristics", "File", "Label"
        ],
        Field(description="column category"),
    ] = None
    column_header: Annotated[str, Field(description="column header")]
    column_prefix: Annotated[None | str, Field(description="column prefix")] = None
    default_value: Annotated[None | str, Field(description="column prefix")] = None
    default_column_index: Annotated[int, Field(description="default column index")]
    min_length: Annotated[
        None | int, Field(description="column value minimum length")
    ] = None
    max_length: Annotated[
        None | int, Field(description="column value maximum length")
    ] = None
    required: Annotated[bool, Field(description="column value is required")] = False
    description: Annotated[None | str, Field(description="column description")] = None
    examples: Annotated[
        None | list[str], Field(description="column value examples")
    ] = None

    @field_validator("min_length", "max_length", mode="before")
    def min_max_validator(val):
        if isinstance(val, int) and val > 0:
            return val
        return None


class InvestigationFileSection(StudyBaseModel):
    name: Annotated[str, Field(description="Section name")]
    fields: Annotated[list[str], Field(description="Section row prefixes")] = []
    default_comments: Annotated[
        list[str], Field(description="Default comments for the section")
    ] = []
    default_field_values: Annotated[
        dict[str, str | list[str] | list[list[str]]],
        Field(description="Default field values"),
    ] = {}
    default_comment_values: Annotated[
        dict[str, str | list[str] | list[list[str]]],
        Field(description="Default comment values"),
    ] = {}


class InvestigationFileTemplate(StudyBaseModel):
    version: Annotated[str, Field(description="Template version")]
    description: Annotated[str, Field(description="Template name")]
    sections: Annotated[
        list[InvestigationFileSection], Field(description="Investigation file sections")
    ]


class IsaTableFileTemplate(StudyBaseModel):
    fixed_column_count: Annotated[int, Field(description="Fixed column count.")] = 0
    description: Annotated[str, Field(description="Template name")]
    version: Annotated[str, Field(description="Template version")]
    headers: Annotated[
        list[ColumnDescription], Field(description="ISA-TAB table column definitions")
    ]


class ProtocolParameterDefinition(StudyBaseModel):
    definition: Annotated[str, Field(description="Definition of protocol parameter.")]
    type: Annotated[
        OntologyTerm, Field(description="Ontology term of protocol parameter type")
    ]
    type_curie: Annotated[
        str,
        Field(
            description="Compact URI presentation (obo_id) of protocol parameter type. "
            "e.g. MS:1000831, OBI:0001139"
        ),
    ] = ""
    format: Annotated[
        Literal["Text", "Ontology", "Numeric"],
        Field(description="value representation format"),
    ]
    examples: Annotated[
        list[str], Field(description="Example protocol parameter values.")
    ] = []


class ProtocolDefinition(StudyBaseModel):
    name: Annotated[str, Field(description="Name of protocol")]
    description: Annotated[str, Field(description="Description of protocol")]
    type: Annotated[OntologyTerm, Field(description="Ontology term of protocol type")]
    type_curie: Annotated[
        str, Field(description="Compact URI presentation (obo_id) of protocol type")
    ] = ""
    parameters: Annotated[list[str], Field(description="Parameters of protocol")] = []
    parameter_definitions: Annotated[
        dict[str, ProtocolParameterDefinition],
        Field(
            description="Definition of protocol parameter "
            "listed in the `parameters` field",
        ),
    ] = {}


class StudyProtocolTemplate(StudyBaseModel):
    version: Annotated[str, Field(description="Template version")]
    description: Annotated[str, Field(description="Template description")] = ""
    protocols: Annotated[list[str], Field(description="Ordered protocol names")] = []
    protocol_definitions: Annotated[
        dict[str, ProtocolDefinition],
        Field(description="Definition of protocol listed in the `protocols` field"),
    ] = {}


class OntologySourceReferenceTemplate(StudyBaseModel):
    source_name: Annotated[str, Field(description="Source name")]
    source_file: Annotated[str, Field(description="Source file")]
    source_version: Annotated[str, Field(description="Source version")]
    source_description: Annotated[
        str, Field(description="Source description and full name")
    ]


class DefaultControl(StudyBaseModel):
    key_pattern: Annotated[str, Field(description="pattern of column or field")]
    default_key: Annotated[str, Field(description="default key name")]


class ActiveMhdProfile(StudyBaseModel):
    profile_name: Annotated[str, Field(description="profile name")]
    default_version: Annotated[str, Field(description="default profile version")]
    active_versions: Annotated[list[str], Field(description="active profile versions")]


class TemplateConfiguration(StudyBaseModel):
    active_investigation_file_templates: Annotated[
        list[str], Field(description="active investigation file templates")
    ]
    active_assignment_file_templates: Annotated[
        list[str], Field(description="active assignment file templates")
    ]
    active_sample_file_templates: Annotated[
        list[str], Field(description="active sample file templates")
    ]
    active_assay_file_templates: Annotated[
        list[str], Field(description="active assay file templates")
    ]
    active_study_categories: Annotated[
        list[str], Field(description="active study categories")
    ]
    active_dataset_licenses: Annotated[
        list[str], Field(description="active dataset licenses")
    ]
    active_mhd_profiles: Annotated[
        dict[StudyCategoryStr, ActiveMhdProfile],
        Field(description="active dataset licenses"),
    ]
    active_study_design_descriptor_categories: Annotated[
        list[str],
        Field(description="active study design descriptor categories"),
    ]
    active_assay_design_descriptor_categories: Annotated[
        list[str],
        Field(description="active assay design descriptor categories"),
    ]
    default_sample_file_template: Annotated[
        str, Field(description="default sample file name")
    ]
    default_investigation_file_template: Annotated[
        str, Field(description="default study file name")
    ]
    default_study_category: Annotated[str, Field(description="default study category")]
    default_dataset_license: Annotated[
        str, Field(description="default dataset license name")
    ]
    investigation_file_name: Annotated[
        str, Field(description="investigation file name")
    ]
    derived_file_extensions: Annotated[
        list[str], Field(description="derived file extensions")
    ]
    raw_file_extensions: Annotated[list[str], Field(description="raw file extensions")]

    assay_file_type_mappings: Annotated[
        dict[StudyCategoryStr, list[str]],
        Field(description="Study category assay file type mappings"),
    ]


class LicenseInfo(StudyBaseModel):
    name: Annotated[str, Field(description="license name")]
    version: Annotated[str, Field(description="license version")] = ""
    url: Annotated[str, Field(description="license URL")] = ""


class MhdProfileInfo(StudyBaseModel):
    file_schema: Annotated[str, Field(description="File schema URL")]
    mhd_file_profile: Annotated[str, Field(description="MHD file profile URL")] = ""
    announcement_file_profile: Annotated[
        str, Field(description="announcement file profile URL")
    ] = ""


class StudyCategoryDefinition(StudyBaseModel):
    index: Annotated[int, Field(description="study category index")]
    name: Annotated[str, Field(description="study category name")]
    label: Annotated[str, Field(description="study category label")]
    description: Annotated[str, Field(description="study category description")]


class CommentDescription(StudyBaseModel):
    name: Annotated[str, Field(description="Comment name")]
    label: Annotated[str, Field(description="Comment label")]
    is_ontology: Annotated[
        bool, Field(description="Is the comment an ontology term")
    ] = False
    control_list_key: Annotated[
        None | str, Field(description="Comment control list key")
    ] = None


class CommentGroupDefinition(StudyBaseModel):
    allow_multiple: Annotated[
        bool, Field(description="comment group can be defined multiple")
    ] = False
    join_operator: Annotated[
        None | str,
        Field(description="join operator if group has multiple values"),
    ] = None
    comments: Annotated[
        list[CommentDescription], Field(description="comments in group")
    ] = False


class SectionDefaultComments(StudyBaseModel):
    groups: Annotated[list[str], Field(description="comment groups in section")] = []

    groupDefinitions: Annotated[
        dict[str, CommentGroupDefinition],
        Field(description="section comment group definitions"),
    ] = {}


class DefaultCommentConfiguration(StudyBaseModel):
    study_comments: Annotated[
        SectionDefaultComments, Field(description="study section comments")
    ]
    assay_comments: Annotated[
        SectionDefaultComments, Field(description="study assay section comments")
    ]
    study_design_descriptor_comments: Annotated[
        SectionDefaultComments,
        Field(description="study design descriptors section comments"),
    ]
    study_factor_comments: Annotated[
        SectionDefaultComments, Field(description="study factors section comments")
    ]
    study_protocol_comments: Annotated[
        SectionDefaultComments, Field(description="study protocol section comments")
    ]
    study_publication_comments: Annotated[
        SectionDefaultComments, Field(description="study publications section comments")
    ]
    study_contact_comments: Annotated[
        SectionDefaultComments, Field(description="study contacts section comments")
    ]


class DescriptorCategoryDefinition(StudyBaseModel):
    name: Annotated[str, Field(description="study category name")]
    label: Annotated[str, Field(description="study category label")]
    control_list_key: Annotated[
        None | str, Field(description="study category description")
    ]


class DescriptorConfiguration(StudyBaseModel):
    default_descriptor_category: Annotated[
        str, Field(description="default descriptor category")
    ] = "default"
    default_submitter_source: Annotated[
        str, Field(description="default submitter source")
    ] = ""
    default_data_curation_source: Annotated[
        str, Field(description="default data curation source")
    ] = ""
    default_workflow_source: Annotated[
        str, Field(description="default workflow source")
    ] = ""
    default_descriptor_categories: Annotated[
        dict[str, DescriptorCategoryDefinition],
        Field(description="default descriptor sources"),
    ] = {}
    default_descriptor_sources: Annotated[
        dict[str, OntologyTerm], Field(description="default descriptor sources")
    ] = {}


class TemplateSettings(StudyBaseModel):
    active_template_versions: Annotated[
        list[str], Field(description="active template versions")
    ]
    default_template_version: Annotated[
        str, Field(description="default study template version")
    ]
    dataset_licenses: Annotated[
        dict[str, LicenseInfo],
        Field(description="MetaboLights template versions"),
    ] = {}
    descriptor_configuration: Annotated[
        DescriptorConfiguration, Field(description="default comment configuration")
    ] = DescriptorConfiguration()
    result_file_formats: Annotated[
        dict[str, OntologyTerm], Field(description="result file formats")
    ] = {}
    default_file_controls: Annotated[
        dict[MetadataFileType, list[DefaultControl]],
        Field(description="default control lists"),
    ]
    default_comments: Annotated[
        DefaultCommentConfiguration, Field(description="default comment configuration")
    ]
    study_categories: Annotated[
        dict[str, StudyCategoryDefinition], Field(description="study categories")
    ] = {}
    mhd_profiles: Annotated[
        dict[str, dict[str, MhdProfileInfo]],
        Field(description="MHD profiles and versions"),
    ] = {}
    versions: Annotated[
        dict[str, TemplateConfiguration],
        Field(description="MetaboLights template versions"),
    ] = {}


class FileTemplates(StudyBaseModel):
    assay_file_header_templates: Annotated[
        dict[str, list[IsaTableFileTemplate]],
        Field(description="ISA-TAB assay file templates"),
    ] = {}
    sample_file_header_templates: Annotated[
        dict[str, list[IsaTableFileTemplate]],
        Field(description="ISA-TAB assay file templates"),
    ] = {}
    assignment_file_header_templates: Annotated[
        dict[str, list[IsaTableFileTemplate]],
        Field(description="maf file templates"),
    ] = {}
    investigation_file_templates: Annotated[
        dict[str, list[InvestigationFileTemplate]],
        Field(description="investigation file templates"),
    ] = {}
    protocol_templates: Annotated[
        dict[str, list[StudyProtocolTemplate]],
        Field(description="Study protocol templates"),
    ] = {}
    ontology_source_reference_templates: Annotated[
        dict[str, OntologySourceReferenceTemplate],
        Field(description="Ontology source reference templates"),
    ] = {}
    configuration: Annotated[
        TemplateSettings,
        Field(description="Validation template settings"),
    ] = {}


class ValidationControls(StudyBaseModel):
    assay_file_controls: Annotated[
        dict[str, list[FieldValueValidation]],
        Field(
            description="Controls for assay file columns. "
            "Field value validations are ordered by precedence. "
            "If there are more than one matches for the field. "
            "Select the first one."
        ),
    ] = {}
    sample_file_controls: Annotated[
        dict[str, list[FieldValueValidation]],
        Field(
            description="Controls for sample file columns. "
            "Field value validations are ordered by precedence. "
            "If there are more than one matches for the field."
            "Select the first one."
        ),
    ] = {}
    assignment_file_controls: Annotated[
        dict[str, list[FieldValueValidation]],
        Field(
            description="Controls for MAF file columns. "
            "Field value validations are ordered by precedence. "
            "If there are more than one matches for the field."
            "Select the first one."
        ),
    ] = {}
    investigation_file_controls: Annotated[
        dict[str, list[FieldValueValidation]],
        Field(
            description="Controls for investigation file fields. "
            "Field value validations are ordered by precedence. "
            "If there are more than one matches for the field."
            "Select the first one."
        ),
    ] = {}


class ValidationConfiguration(StudyBaseModel):
    controls: Annotated[
        ValidationControls,
        Field(description="Investigation, sample, assay validation controls"),
    ] = ValidationControls()
    templates: Annotated[
        FileTemplates, Field(description="Investigation, sample, assay file templates")
    ] = FileTemplates()
