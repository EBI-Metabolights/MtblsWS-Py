import logging
import os
from pathlib import Path
from typing import Any, Literal, OrderedDict
from isatools import model
import httpx
from isatools import model
from app.config import get_settings
from app.ws.isaApiClient import IsaApiClient
from app.ws.settings.utils import get_study_settings
from app.ws.study.isa_table_models import NumericValue, OntologyValue
from app.ws.utils import get_maf_name_from_assay_name, read_tsv

logger = logging.getLogger(__name__)
iac = IsaApiClient()


MEASURMENT_TYPE_ONTOLOGY_TERMS = {
    "metabolite profiling": ("OBI", "http://purl.obolibrary.org/obo/OBI_0000366"),
    "targeted metabolite profiling": (
        "MSIO",
        "http://purl.obolibrary.org/obo/MSIO_0000100",
    ),
    "untargeted metabolite profiling": (
        "MSIO",
        "http://purl.obolibrary.org/obo/MSIO_0000101",
    ),
}
DEFAULT_MEASUREMENT_TYPE = "metabolite profiling"

TECHNOLOGY_TYPE_ONTOLOGY_TERMS = {
    "MS": (
        "mass spectrometry assay",
        "OBI",
        "http://purl.obolibrary.org/obo/OBI_0000470",
    ),
    "NMR": (
        "NMR spectroscopy assay",
        "OBI",
        "http://purl.obolibrary.org/obo/OBI_0000623",
    ),
}


def update_study_protocols(
    isa_study: model.Study,
    protocols: list[dict[str, Any]],
):
    # Add new protocol
    assay_protocols = OrderedDict(
        [(x.get("name"), x.get("parameters")) for x in protocols]
    )
    protocols = isa_study.protocols
    study_protocols_map = OrderedDict([(x.name, x) for x in protocols])
    for name, params in assay_protocols.items():
        assay_params = {x for x in params if x and x.strip()}
        if name in study_protocols_map:
            if assay_params:
                study_protocol = study_protocols_map.get(name)
                new_params = {
                    x.parameter_name.term
                    for x in study_protocol.parameters
                    if x.parameter_name.term not in params
                }
                for assay_param in new_params:
                    protocol_parameter = model.ProtocolParameter(
                        parameter_name=model.OntologyAnnotation(term=assay_param)
                    )
                    study_protocol.parameters.append(protocol_parameter)

        else:
            # Create a new protocol
            protocol = model.Protocol(
                name=name,
                protocol_type=model.OntologyAnnotation(term=name),
                description="Please update this protocol description",
            )

            for param in assay_params:
                if param:
                    protocol_parameter = model.ProtocolParameter(
                        parameter_name=model.OntologyAnnotation(term=param)
                    )
                    protocol.parameters.append(protocol_parameter)

            # Add the new protocol to the protocols list
            protocols.append(protocol)


def create_sample_sheet(
    study_id: str,
    study_path: None | str = None,
    sample_type: None | str = None,
    template_version: None | str = None,
    override_current: bool = False,
):
    settings = get_settings()

    if not study_path:
        studies_path = settings.study.mounted_paths.study_metadata_files_root_path
        study_path = os.path.join(studies_path, study_id)

    sample_file_name = "s_" + study_id.upper() + ".txt"
    assay_file_path = os.path.join(study_path, sample_file_name)
    override_file = True
    if not override_current:
        override_file = is_empty_isa_table_sheet(assay_file_path)
    if override_file:
        if not sample_type:
            sample_type = settings.study.default_metadata_sample_template_name

        if not template_version:
            template_version = settings.study.default_metadata_template_version

        template = get_sample_template(
            template_name=sample_type, template_version=template_version
        )
        Path(assay_file_path).parent.mkdir(parents=True, exist_ok=True)
        success = create_file_from_template(assay_file_path, template)
        logger.info("%s file is created.", assay_file_path)
        return success, sample_file_name
    return False, None


def add_new_assay_sheet(
    study_id: str,
    assay_type: str,
    polarity,
    column_type,
    default_column_values: None | dict[str, str | OntologyValue | NumericValue] = None,
    assay_file_name: None | str = None,
    maf_file_name: None | str = None,
    template_version: None | str = None,
    measurment_type_name: Literal[
        "metabolite profiling",
        "untargeted metabolite profiling",
        "targeted metabolite profiling",
    ] = "metabolite profiling",
):
    study_settings = get_study_settings()
    study_metadata_location = os.path.join(
        study_settings.mounted_paths.study_metadata_files_root_path, study_id
    )
    measurement_type_label = (
        measurment_type_name.lower().replace("assay", "").strip().replace(" ", "_")
    )
    file_name = "_".join(
        [
            "a",
            study_id.upper(),
            assay_type,
            polarity,
            column_type or "",
            measurement_type_label or "",
        ]
    )
    if not assay_file_name:
        assay_file_name = get_valid_assay_file_name(file_name, study_metadata_location)

    isa_study, isa_inv, _ = iac.get_isa_study(
        study_id=study_id,
        skip_load_tables=True,
        study_location=study_metadata_location,
    )

    success, technology_platform = create_assay_sheet(
        study_path=study_metadata_location,
        assay_file_name=assay_file_name,
        assay_type=assay_type,
        polarity=polarity or "",
        column_type=column_type or "",
        default_column_values=default_column_values or {},
        template_version=template_version,
    )
    if not success:
        return False, None, None
    if not maf_file_name:
        maf_file_name = get_maf_name_from_assay_name(assay_file_name)
    main_technology_type = "NMR" if assay_type in ["NMR", "MRImaging"] else "MS"
    create_maf_sheet(
        study_path=study_metadata_location,
        maf_file_name=maf_file_name,
        main_technology_type=main_technology_type,
        template_version=template_version,
    )

    ontology_source_references = get_ontology_source_references()

    term, term_source, term_accession = TECHNOLOGY_TYPE_ONTOLOGY_TERMS.get(
        main_technology_type
    )
    technology_type = model.OntologyAnnotation(
        term=term,
        term_source=update_ontology_sources(
            isa_inv, ontology_source_references, term_source
        ),
        term_accession=term_accession,
    )

    if not measurment_type_name:
        measurment_type_name = DEFAULT_MEASUREMENT_TYPE
    term_source, term_accession = MEASURMENT_TYPE_ONTOLOGY_TERMS.get(
        measurment_type_name
    )
    measurement_type = model.OntologyAnnotation(
        term=measurment_type_name,
        term_source=update_ontology_sources(
            isa_inv, ontology_source_references, term_source
        ),
        term_accession=term_accession,
    )
    assay = model.Assay(
        filename=file_name,
        technology_platform=technology_platform,
        technology_type=technology_type,
        measurement_type=measurement_type,
    )

    assays: list[model.Assay] = isa_study.assays
    assays.append(assay)
    protocol_descriptions = get_protocol_descriptions(assay_type=assay_type)
    protocols = protocol_descriptions.get("protocols", [])
    update_study_protocols(isa_study, protocols)

    logger.info("A copy of the previous files will be saved")
    assay.technology_platform = technology_platform
    iac.write_isa_study(
        isa_inv, None, study_metadata_location, save_investigation_copy=True
    )
    return True, assay_file_name, maf_file_name


def get_valid_assay_file_name(file_name, study_path):
    # Has the filename has already been used in another assay?
    file_counter = 0
    assay_file = os.path.join(study_path, file_name + ".txt")
    file_exists = os.path.isfile(assay_file)
    while file_exists:
        file_counter += 1
        new_file = file_name + "-" + str(file_counter)
        if not os.path.isfile(os.path.join(study_path, new_file + ".txt")):
            file_name = new_file
            break

    return file_name + ".txt"


def update_ontology_sources(isa_inv, ontology_source_references, ontology_source):
    obi_ontology_reference = ontology_source_references.get(ontology_source)
    item: model.OntologySource = isa_inv.get_ontology_source_reference(ontology_source)
    obi_ontology = obi_ontology_reference
    if item is None:  # Add the ontology to the investigation
        ontologies = isa_inv.get_ontology_source_references()
        ontologies.append(obi_ontology_reference)
    else:
        obi_ontology = item
        item.name = obi_ontology_reference.name
        item.version = obi_ontology_reference.version
        item.description = obi_ontology_reference.description
        item.file = obi_ontology_reference.file
    return obi_ontology


def create_assay_sheet(
    study_path: str,
    assay_file_name: str,
    assay_type: str,
    polarity: str = "",
    column_type: str = "",
    default_column_values: None | dict[str, str | OntologyValue | NumericValue] = None,
    template_version: None | str = None,
    override_current: bool = False,
) -> str:
    assay_file_path = os.path.join(study_path, assay_file_name)
    override_file = True
    if not override_current:
        override_file = is_empty_isa_table_sheet(assay_file_path)

    if override_file:
        assay_template = get_assay_template(
            assay_type=assay_type, template_version=template_version
        )
        assay_desc = assay_template.get("description") or ""
        technology_platform = assay_desc + " - " + polarity
        if column_type:
            technology_platform += " - " + column_type

        assay_template_headers = assay_template.get("headers", [])
        for header in assay_template_headers:
            if header.get("columnHeader") in default_column_values:
                header["defaultValue"] = default_column_values[
                    header.get("columnHeader")
                ]
        create_file_from_template(assay_file_path, assay_template)

        return True, technology_platform

    return False, ""


def create_maf_sheet(
    study_path: str,
    maf_file_name: str,
    main_technology_type: str,
    template_version: None | str = None,
    override_current: bool = False,
):
    maf_file_path = os.path.join(study_path, maf_file_name)
    maf_template = get_maf_template(
        maf_type=main_technology_type, template_version=template_version
    )
    override_maf = True
    if not override_current:
        override_maf = is_empty_isa_table_sheet(maf_file_path)

    if override_maf:
        create_file_from_template(maf_file_path, maf_template)
        logger.info("%s maf file is created.", maf_file_name)
        return True
    else:
        logger.warning("%s maf file exists. skipping...", maf_file_name)
    return False


def is_empty_isa_table_sheet(isa_table_file_path: str):
    empty = True
    if os.path.exists(isa_table_file_path):
        maf_df = read_tsv(isa_table_file_path, header=0, encoding="ISO-8859-1")
        if maf_df.empty or len(maf_df.columns) == 0 or len(maf_df) <= 1:
            logger.info("%s is not valid.", isa_table_file_path)
        else:
            empty = False
    return empty


def get_ontology_source_references() -> dict[str, model.OntologySource]:
    settings = get_settings()
    service_url = settings.external_dependencies.api.policy_engine_url
    endpoint = (
        "/v1/data/metabolights/validation/v2/templates/ontologySourceReferenceTemplates"
    )
    url = f"{service_url}{endpoint}"
    ontology_sources = {}
    try:
        response = httpx.get(url)
        response.raise_for_status()
        response_json = response.json()
        if response_json:
            ontology_sources = {
                x.get("sourceName", ""): model.OntologySource(
                    name=x.get("sourceName", ""),
                    file=x.get("sourceFile", ""),
                    version=x.get("sourceVersion", ""),
                    description=x.get("sourceDescription", ""),
                )
                for x in response_json.get("result", [])
            }
            return ontology_sources
    except Exception as ex:
        logger.error("%s", ex)
    return {}


def get_sample_template(
    template_name: str, template_version: None | str = None
) -> dict[str, Any]:
    templates_base_path = "/v1/data/metabolights/validation/v2/templates"
    context_path = f"{templates_base_path}/sampleFileHeaderTemplates/{template_name}"
    return get_template_from_policy_service(
        context_path=context_path, template_version=template_version
    )


def get_assay_template(
    assay_type: str, template_version: None | str = None
) -> dict[str, Any]:
    templates_base_path = "/v1/data/metabolights/validation/v2/templates"
    context_path = f"{templates_base_path}/assayFileHeaderTemplates/{assay_type}"
    return get_template_from_policy_service(
        context_path=context_path, template_version=template_version
    )


def get_maf_template(
    maf_type: str, template_version: None | str = None
) -> dict[str, Any]:
    templates_base_path = "/v1/data/metabolights/validation/v2/templates"
    context_path = (
        f"{templates_base_path}/assignmentFileHeaderTemplates/{maf_type.upper()}"
    )
    return get_template_from_policy_service(
        context_path=context_path, template_version=template_version
    )


def get_protocol_descriptions(
    assay_type: str, template_version: None | str = None
) -> dict[str, Any]:
    templates_base_path = "/v1/data/metabolights/validation/v2/templates"
    context_path = f"{templates_base_path}/studyProtocolTemplates/{assay_type}"
    return get_json_from_policy_service(context_path=context_path).get("result", {})


def get_template_from_policy_service(
    context_path: str, template_version: None | str = None
) -> dict[str, Any]:
    settings = get_settings()
    if not template_version:
        template_version = settings.study.default_metadata_template_version

    response_json = get_json_from_policy_service(context_path=context_path)
    templates = response_json.get("result", [])
    for template in templates:
        if template.get("version", "") == template_version:
            return template
    return {}


def get_json_from_policy_service(context_path: str) -> dict[str, Any]:
    settings = get_settings()
    service_url = settings.external_dependencies.api.policy_engine_url
    url = f"{service_url}{context_path}"
    try:
        response = httpx.get(url)
        response.raise_for_status()
        return response.json()

    except Exception as ex:
        logger.error("%s", ex)
    return {}


def create_file_from_template(sample_file_fullpath: str, template: dict[str, Any]):
    if not template:
        return False
    header_row: list[str] = []
    initial_row: list[str] = []
    try:
        for header in template.get("headers", []):
            default_value = header.get("defaultValue", None) or ""
            column_structure = header.get("columnStructure", "")
            header_name = header.get("columnHeader", "") or ""
            add_new_columns(
                header_row, initial_row, header_name, column_structure, default_value
            )

        with Path(sample_file_fullpath).open("w") as f:
            f.write("\t".join(header_row) + "\n")
            f.write("\t".join(initial_row) + "\n")

        return True
    except Exception as ex:
        logger.exception("%s", ex)
        return False


def add_new_columns(
    header_row: list[str],
    initial_row: list[str],
    header_name: str,
    column_structure: str,
    default_value: str | OntologyValue | NumericValue,
):
    header_row.append(header_name or "")
    if isinstance(default_value, NumericValue):
        term = default_value.unit.term
        term_source = default_value.unit.term_source_ref
        term_accession = default_value.unit.term_accession_number
        value = default_value.value
        if column_structure == "SINGLE_COLUMN_AND_UNIT_ONTOLOGY":
            header_row.append("Unit")
            header_row.append("Term Source REF")
            header_row.append("Term Accession Number")
            initial_row.append(value)
            initial_row.append(term)
            initial_row.append(term_source)
            initial_row.append(term_accession)
        elif column_structure == "ONTOLOGY_COLUMN":
            header_row.append("Term Source REF")
            header_row.append("Term Accession Number")
            initial_row.append(value)
            initial_row.append("")
            initial_row.append("")
        elif column_structure == "SINGLE_COLUMN":
            initial_row.append(value)
    elif isinstance(default_value, OntologyValue):
        term = default_value.term
        term_source = default_value.term_source_ref
        term_accession = default_value.term_accession_number
        if column_structure == "SINGLE_COLUMN_AND_UNIT_ONTOLOGY":
            header_row.append("Unit")
            header_row.append("Term Source REF")
            header_row.append("Term Accession Number")
            initial_row.append(term)
            initial_row.append("")
            initial_row.append("")
            initial_row.append("")
        elif column_structure == "ONTOLOGY_COLUMN":
            header_row.append("Term Source REF")
            header_row.append("Term Accession Number")
            initial_row.append(term)
            initial_row.append(term_source)
            initial_row.append(term_accession)
        elif column_structure == "SINGLE_COLUMN":
            initial_row.append(term)
    else:
        text = str(default_value)
        if column_structure == "SINGLE_COLUMN_AND_UNIT_ONTOLOGY":
            header_row.append("Unit")
            header_row.append("Term Source REF")
            header_row.append("Term Accession Number")
            initial_row.append(text)
            initial_row.append("")
            initial_row.append("")
            initial_row.append("")
        elif column_structure == "ONTOLOGY_COLUMN":
            header_row.append("Term Source REF")
            header_row.append("Term Accession Number")
            initial_row.append(text)
            initial_row.append("")
            initial_row.append("")
        elif column_structure == "SINGLE_COLUMN":
            initial_row.append(text)
