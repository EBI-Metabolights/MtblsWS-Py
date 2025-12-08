import logging
from typing import Annotated, Union

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel, to_pascal

logger = logging.getLogger(__name__)


class IsaBaseModel(BaseModel):
    """Base model class to convert python attributes to camel case"""

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        json_schema_serialization_defaults_required=True,
        field_title_generator=lambda field_name, field_info: to_pascal(
            field_name.replace("_", " ").strip()
        ),
    )


class Comment(IsaBaseModel):
    name: Annotated[str, Field(description="Comment name")] = ""
    value: Annotated[str, Field(description="Comment value")] = ""


class Commented(IsaBaseModel):
    comments: Annotated[list[Comment], Field(description="Comments")] = []


class Identifiable(Commented):
    id_: Annotated[str, Field(description="Unique id of the object")] = ""


class OntologySource(Commented):
    name: Annotated[
        str,
        Field(
            description="The name of the source of a term; i.e. the source controlled vocabulary or ontology."
            " These names will be used in all corresponding Term Source REF fields that occur elsewhere."
        ),
    ] = ""

    file: Annotated[
        str,
        Field(description="A file name or a URI of an official resource."),
    ] = ""
    version: Annotated[
        Union[str, int],
        Field(
            description="The version number of the Term Source to support terms tracking."
        ),
    ] = ""
    description: Annotated[
        str,
        Field(
            description="Description of source. "
            "Use for disambiguating resources when homologous prefixes have been used.",
        ),
    ] = ""


class OntologyTerm(Identifiable):
    term: Annotated[
        str,
        Field(description="Ontology term"),
    ] = ""

    term_accession: Annotated[
        str,
        Field(
            description="The accession number from the Term Source associated with the term.",
        ),
    ] = ""
    term_source: Annotated[
        str,
        Field(
            description="Source reference name of ontology term. e.g., EFO, OBO, NCIT. "
            "Ontology source reference names should be defined "
            "in ontology source references section in i_Investigation.txt file",
        ),
    ] = ""


class DesignDescriptor(OntologyTerm): ...


class ProtocolType(OntologyTerm): ...


class ProtocolComponent(Identifiable):
    name: Annotated[
        str,
        Field(description="Name of the protocol component"),
    ] = ""
    component_type: Annotated[
        OntologyTerm, Field(alias="type", description="type of protocol component")
    ]


class ProtocolParameter(OntologyTerm): ...


class Factor(IsaBaseModel):
    name: Annotated[
        str,
        Field(
            description="The name of one factor used in the study files. "
            "A factor corresponds to an independent variable manipulated by the experimentalist "
            "with the intention to affect biological systems in a way that can be measured by an assay.",
        ),
    ] = ""
    type_: Annotated[
        OntologyTerm,
        Field(
            alias="type",
            description="A ,term allowing the classification of the factor into categories. "
            "The term is a controlled vocabulary or an ontology",
        ),
    ] = OntologyTerm()


class Assay(IsaBaseModel):
    filename: Annotated[
        str,
        Field(
            description="Assay file name. Expected filename pattern is a_*.txt",
        ),
    ] = ""
    measurement_type: Annotated[
        OntologyTerm,
        Field(
            description="A term to qualify what is being measured (e.g. metabolite identification).",
        ),
    ] = OntologyTerm()
    technology_type: Annotated[
        OntologyTerm,
        Field(
            description="Term to identify the technology used to perform the measurement, "
            "e.g. NMR spectrometry assay, mass spectrometry assay",
        ),
    ] = OntologyTerm()
    technology_platform: Annotated[
        str,
        Field(
            description="Platform information "
            "such as assay technique name, polarity, column model, manufacturer, platform name.",
        ),
    ] = ""


class CommentedAssay(Commented, Assay): ...


class Person(IsaBaseModel):
    last_name: Annotated[
        str,
        Field(
            description="Last name of a person associated with the investigation or study."
        ),
    ] = ""
    first_name: Annotated[
        str,
        Field(
            description="First name of person associated with the investigation or study.",
        ),
    ] = ""
    mid_initials: Annotated[
        str,
        Field(
            description="Middle name initials (if exists) of person associated with the investigation or study",
        ),
    ] = ""
    email: Annotated[
        str,
        Field(
            description="Email address of person associated with the investigation or study",
        ),
    ] = ""
    phone: Annotated[
        str,
        Field(
            description="Phone number of person associated with the investigation or study",
        ),
    ] = ""
    fax: Annotated[
        str,
        Field(
            description="Fax number of person associated with the investigation or study",
        ),
    ] = ""
    address: Annotated[
        str,
        Field(
            description="Address of person associated with the investigation or study",
        ),
    ] = ""
    affiliation: Annotated[
        str,
        Field(
            description="Affiliation of person associated with the investigation or study",
        ),
    ] = ""
    roles: Annotated[
        list[OntologyTerm],
        Field(
            description="Roles of person associated with the investigation or study. "
            "Multiple role can be defined for each person. Role is defined as an ontology term. "
            "e.g., NCIT:Investigator, NCIT:Author",
        ),
    ] = []


class CommentedPerson(Commented, Person): ...


class Protocol(Identifiable):
    name: Annotated[
        str,
        Field(description="Protocol name."),
    ] = ""
    protocol_type: Annotated[
        ProtocolType,
        Field(description="Term to classify the protocol."),
    ] = ProtocolType()
    description: Annotated[
        str,
        Field(description="Protocol description."),
    ] = ""
    uri: Annotated[
        str,
        Field(
            description="Pointer to external protocol resources "
            "that can be accessed by their Uniform Resource Identifier (URI).",
        ),
    ] = ""
    version: Annotated[
        str,
        Field(
            description="An identifier for the version to ensure protocol tracking.."
        ),
    ] = ""
    parameters: Annotated[
        list[ProtocolParameter],
        Field(description="Protocol's parameters."),
    ] = []
    components: Annotated[
        list[ProtocolComponent],
        Field(
            description="Protocol's components; "
            "e.g. instrument names, software names, and reagents names.",
        ),
    ] = []


class Publication(IsaBaseModel):
    pubmed_id: Annotated[
        str,
        Field(title="PubMed Id", description="The PubMed IDs of the publication."),
    ] = ""
    doi: Annotated[
        str,
        Field(description="A Digital Object Identifier (DOI) for the publication."),
    ] = ""
    author_list: Annotated[
        str,
        Field(
            description="The list of authors associated with the publication. "
            "Comma (,) is recommended to define multiple authors."
        ),
    ] = ""
    title: Annotated[
        str,
        Field(
            description="The title of publication associated with the investigation."
        ),
    ] = ""
    status: Annotated[
        OntologyTerm,
        Field(
            description="A term describing the status of that publication "
            "(i.e. EFO:submitted, EFO:in preparation, EFO:published).",
        ),
    ] = OntologyTerm()


class Study(IsaBaseModel):
    identifier: Annotated[
        str,
        Field(
            description="A unique identifier, "
            "either a temporary identifier generated by MetaboLights repository.",
        ),
    ] = ""
    filename: Annotated[
        str,
        Field(
            description="Name of the Sample Table file corresponding the definition of that Study.",
        ),
    ] = ""

    title: Annotated[
        str,
        Field(
            description="A concise phrase used to encapsulate the purpose and goal of the study.",
        ),
    ] = ""
    description: Annotated[
        str,
        Field(
            description="A textual description of the study, with components such as objective or goals.",
        ),
    ] = ""
    submission_date: Annotated[
        str,
        Field(
            description="The date on which the study is submitted to an archive. "
            "String formatted as ISO8601 date YYYY-MM-DD",
        ),
    ] = ""
    public_release_date: Annotated[
        str,
        Field(
            description="The date on which the study SHOULD be released publicly. "
            "String formatted as ISO8601 date YYYY-MM-DD",
        ),
    ] = ""
    publications: Annotated[
        list[Publication],
        Field(description="Content of study publications section."),
    ] = []
    contacts: Annotated[
        list[Person],
        Field(description="Content of study contacts section."),
    ] = []
    design_descriptors: Annotated[
        list[DesignDescriptor],
        Field(description="Content of study design descriptors section."),
    ] = []

    protocols: Annotated[
        list[Protocol],
        Field(description="Content of study protocols section."),
    ] = []
    assays: Annotated[
        list[Assay],
        Field(
            description="Study assay section of i_Investigation.txt file. "
            "This section contains study assays and comments.",
        ),
    ] = []
    factors: Annotated[
        list[Factor],
        Field(description="Content of study factors section."),
    ] = []


class Investigation(Identifiable):
    identifier: Annotated[
        str,
        Field(description="Investigation identifier."),
    ] = ""
    title: Annotated[
        str,
        Field(description="A concise name given to the investigation."),
    ] = ""
    description: Annotated[
        str,
        Field(description="A textual description of the investigation."),
    ] = ""
    submission_date: Annotated[
        str,
        Field(
            description="The date on which the investigation was reported to the repository. "
            "String formatted as ISO8601 date YYYY-MM-DD"
        ),
    ] = ""
    public_release_date: Annotated[
        str,
        Field(
            description="The date on which the investigation was released publicly. "
            "String formatted as ISO8601 date YYYY-MM-DD"
        ),
    ] = ""

    publications: Annotated[
        list[Publication],
        Field(
            description="All publications prepared to report results of the investigation."
        ),
    ] = []

    contacts: Annotated[
        list[Person],
        Field(description="People details of the investigation."),
    ] = []

    ontology_source_references: Annotated[
        list[OntologySource],
        Field(description="Ontology sources used in the investigation"),
    ] = []

    studies: Annotated[
        list[Study],
        Field(description="Studies carried out in the investigation."),
    ] = []
