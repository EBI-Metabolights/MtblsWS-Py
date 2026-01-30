from typing import Annotated, Any, Dict, List, Optional, Self

from pydantic import Field, ValidationError, model_validator

from app.ws.db.models import CamelCaseBaseModel


class CommentKV(CamelCaseBaseModel):
    name: str
    value: Optional[str] = None


class TermSource(CamelCaseBaseModel):
    comments: List[CommentKV] = []
    name: Optional[str] = None
    file: Optional[str] = None
    version: Optional[str] = None
    description: Optional[str] = None


class OntologyTerm(CamelCaseBaseModel):
    comments: List[CommentKV] = []
    term_accession: Optional[str] = None
    annotation_value: Optional[str] = None
    term_source: TermSource


class RelatedDataset(CamelCaseBaseModel):
    repository: str
    accession: str


class Funding(CamelCaseBaseModel):
    funding_organization: Optional[OntologyTerm] = None
    grant_identifier: Optional[str] = None

    @model_validator(mode="wrap")
    @classmethod
    def validate_model(cls, v: Any, handler) -> Self:
        converted_value = handler(v)
        if (
            not converted_value.grant_identifier
            and not converted_value.funding_organization
        ):
            raise ValidationError(
                "At least one of 'grant_identifier' or "
                "'funding_organization' must be provided in Funding."
            )
        return converted_value


class Contact(CamelCaseBaseModel):
    comments: List[CommentKV] = []
    first_name: str
    last_name: str
    email: str
    affiliation: Optional[str] = None
    address: Optional[str] = None
    fax: Optional[str] = None
    mid_initials: Optional[str] = None
    phone: Optional[str] = None
    roles: Annotated[List[OntologyTerm], Field(min_length=1)] = []


class Factor(CamelCaseBaseModel):
    comments: List[CommentKV] = []
    factor_name: str
    factor_type: OntologyTerm


class Publication(CamelCaseBaseModel):
    title: Optional[str] = None
    author_list: Optional[str] = None
    doi: Optional[str] = None
    pubmed_id: Optional[str] = None
    status: OntologyTerm = OntologyTerm(
        annotation_value="in preparation",
        term_source=TermSource(name="EFO"),
        term_accession="http://www.ebi.ac.uk/efo/EFO_0001795",
    )


class StudyCreationRequest(CamelCaseBaseModel):
    selected_template_version: Optional[str] = None
    selected_study_categories: Dict[str, List[str]]
    selected_investigation_file_template: Optional[str] = None
    selected_sample_file_template: Optional[str] = None
    dataset_license_agreement: Optional[bool] = False
    dataset_policy_agreement: Optional[bool] = False
    publications: list[Publication] = []
    title: str = ""
    description: str = ""
    related_datasets: List[RelatedDataset] = []
    funding: List[Funding] = []
    contacts: Annotated[List[Contact], Field()] = []
    design_descriptors: List[OntologyTerm] = []
    factors: Annotated[List[Factor], Field()] = []


class DefaultValue(CamelCaseBaseModel):
    term_accession: Optional[str] = None
    annotation_value: Optional[str] = None
    term_source: Optional[TermSource] = None
    unit: Optional[OntologyTerm] = None


class AssayFieldDefaultValue(CamelCaseBaseModel):
    field_name: Annotated[str, Field()]
    field_format: Annotated[str, Field()]
    default_value: Annotated[DefaultValue, Field()]


class AssayCreationRequest(CamelCaseBaseModel):
    selected_assay_file_template: str
    assay_identifier: Optional[str] = None
    assay_result_file_type: Optional[str] = None
    assay_result_file_name: Optional[str] = None
    selected_measurement_type: Optional[str] = None
    selected_omics_type: Optional[str] = None
    design_descriptors: list[OntologyTerm] = []
    assay_file_default_values: list[AssayFieldDefaultValue] = []
