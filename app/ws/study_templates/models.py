import datetime
import enum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic.alias_generators import to_camel, to_pascal


class IsaTabFileType(enum.StrEnum):
    ASSAY = "assay"
    SAMPLE = "sample"
    INVESTIGATION = "investigation"


class ValidationType(enum.StrEnum):
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
            description="The accession number from the Term Source associated with the term.",
        ),
    ]
    term_source_ref: Annotated[
        str,
        Field(description="Source reference name of ontology term. e.g., EFO, OBO."),
    ]


class OntologyTermPlaceholder(StudyBaseModel):
    term_accession_number: Annotated[
        str,
        Field(description="The accession number of placeholder."),
    ]
    term_source_ref: Annotated[
        str,
        Field(description="Source reference name of placeholder. e.g., MTBLS."),
    ]


class FieldSelector(StudyBaseModel):
    name: Annotated[
        str,
        Field(description="Node name. e.g., Sample Name, Source Name, Protocol REF"),
    ]
    value: Annotated[
        None | str,
        Field(
            description="Node value to find . e.g. Protocol REF with value 'Sample collection'"
        ),
    ]


class SelectionCriteria(StudyBaseModel):
    isa_file_type: Annotated[
        IsaTabFileType,
        Field(description="ISA-TAB file type."),
    ]
    study_created_at_or_after: Annotated[
        None | datetime.datetime,
        Field(description="Filter to select studies created after the defined date."),
    ]
    study_created_before: Annotated[
        None | datetime.datetime,
        Field(description="Filter to select studies created before the defined date."),
    ]
    study_category_filter: Annotated[
        None | list[StudyCategoryStr],
        Field(description="Filter to select studies with the defined category"),
    ]
    template_version_filter: Annotated[
        None | list[str],
        Field(description="Filter to select studies with the defined template version"),
    ]
    isa_file_template_name_filter: Annotated[
        None | list[str],
        Field(
            description="Filter to select ISA-TAB file template. "
            "LC-MS, GC-MS, etc. for assay, minimum, clinical, etc. for sample"
        ),
    ]
    linked_field_and_value_filter: Annotated[
        None | list[FieldSelector],
        Field(
            description="Filter by linked field and its value. "
            "Current rule will be selected if the field "
            "is an attribute of ISA-TAB node (Sample Name, Source Name, Protocol REF)."
            "Characteristics can be linked to Sample Name or Source Name. "
            "Parameter Value can be linked to Protocol REF with value. "
            "e.g., {name: 'Protocol REF', 'value': 'Mass spectrometry'}, "
            "Units can be linked to Parameter Value, Factor Value and Characteristic fields."
            "Comments can be linked to ISA-TAB nodes (Sample Name, Source Name, Protocol REF, etc.)"
        ),
    ]


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


class FieldConstraint(StudyBaseModel):
    type_: Annotated[
        ConstraintType, Field(alias="type", description="Constraint type.")
    ]
    constraint: Annotated[
        None | str | int | bool, Field(description="Constraint value")
    ] = None
    error_message: Annotated[
        str, Field(description="Error message if value does not satisfy the constraint")
    ] = ""


class ParentOntologyTerms(StudyBaseModel):
    exclude_by_label_pattern: Annotated[
        None | list[str],
        Field(description="Label match regex patterns to filter ontology terms."),
    ]
    exclude_by_accession: Annotated[
        None | list[str],
        Field(description="Accession numbers of the excluded ontology terms."),
    ]
    parents: Annotated[
        list[OntologyTerm],
        Field(description="List of parent ontology terms"),
    ]


class FieldValueValidation(StudyBaseModel):
    rule_name: Annotated[
        str,
        Field(
            description="Unique name id for the the field. "
            "Rule naming convention is <Field name>-<incremental number>. "
            "e.g., Parameter Value[Instrument]-01, Parameter Value[Instrument]-02"
        ),
    ]
    description: Annotated[
        str,
        Field(description="Definition of rule and summary of selection criteria."),
    ]
    field_name: Annotated[
        str,
        Field(
            description="Name of the column header or investigation field name. "
            "e.g., Parameter Value[Instrument], Study Assay Measurement Type."
        ),
    ]
    selection_criteria: Annotated[
        SelectionCriteria, Field(description="Field selection criteria")
    ]
    validation_type: Annotated[
        ValidationType, Field(description="Validation rule type")
    ] = ValidationType.ANY_ONTOLOGY_TERM
    constraints: Annotated[
        None | list[FieldConstraint], Field(description="Field constraints")
    ] = None
    default_value: Annotated[
        None | OntologyTerm, Field(description="Default ontology term")
    ]
    allowed_missing_ontology_terms: Annotated[
        None | list[OntologyTerm], Field(description="Allowed missing ontology terms")
    ]
    allowed_other_sources: Annotated[
        None | list[AdditionalSource],
        Field(description="Allowed values from other non ontology sources."),
    ]
    allowed_placeholders: Annotated[
        None | list[OntologyTermPlaceholder],
        Field(description="Allowed placeholders for term source and accession"),
    ]
    terms: Annotated[
        None | list[OntologyTerm],
        Field(
            description="Selected ontology terms. "
            "If validation type is selected-ontology-term, "
            "it defines ordered allowed ontology terms, "
            "otherwise it lists ordered and recommended ontology terms."
        ),
    ]
    ontologies: Annotated[
        None | list[str],
        Field(
            description="Ordered ontology source references. "
            "If validation type is ontology-term-in-selected-ontologies, "
            "it defines ontology sources, otherwise it lists recommended ontology sources."
        ),
    ]

    allowed_parent_ontology_terms: Annotated[
        None | ParentOntologyTerms,
        Field(
            description="Parent ontology terms to find the allowed child ontology terms. "
            "Applicable only for validation type child-ontology-term"
        ),
    ]
    unexpected_terms: Annotated[
        None | list[str],
        Field(description="unexpected terms."),
    ] = None


class ColumnDescription(StudyBaseModel):
    column_structure: Annotated[
        Literal["SINGLE_COLUMN", "ONTOLOGY_COLUMN", "SINGLE_COLUMN_AND_UNIT_ONTOLOGY"],
        Field(description="column structure"),
    ]
    column_category: Annotated[
        None
        | Literal["", "Basic", "Protocol", "Parameter", "Characteristics", "File", "Label"],
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


class IsaTableFileTemplate(StudyBaseModel):
    fixed_column_count: Annotated[int, Field(description="Fixed column count.")] = 0
    description: Annotated[str, Field(description="Template name")]
    version: Annotated[str, Field(description="Template version")]
    headers: Annotated[
        list[ColumnDescription], Field(description="ISA-TAB table column definitions")
    ]


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
    controls: Annotated[ValidationControls, Field(description="File templates")]
    templates: Annotated[FileTemplates, Field(description="File templates")]
