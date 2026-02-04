import datetime
import logging
import os
import re
from typing import OrderedDict

from isatools import model
from pydantic import BaseModel

from app.ws.db.types import StudyCategory
from app.ws.isaApiClient import IsaApiClient
from app.ws.study.isa_table_models import OntologyValue
from app.ws.study.utils import get_study_metadata_path
from app.ws.utils import read_tsv

logger = logging.getLogger(__name__)


def update_revision_comments(
    isa_study: model.Study,
    revision_number: int = 0,
    revision_datetime: None | datetime.datetime = None,
    revision_comment: None | str = None,
):
    if revision_number > 0:
        revision_comments = [
            c
            for c in isa_study.comments
            if c.name.strip().lower() in {"revision", "dataset revision"}
        ]
        revision_datetimes = [
            c
            for c in isa_study.comments
            if c.name.strip().lower() in {"revision date", "dataset revision date"}
        ]
        revision_logs = [
            c
            for c in isa_study.comments
            if c.name.strip().lower() in {"revision log", "dataset revision log"}
        ]

        revision_comments.extend(revision_datetimes)
        revision_comments.extend(revision_logs)
        comment = model.Comment(name="Revision", value=str(revision_number))
        isa_study.comments.append(comment)
        revision_datetime = ""
        if revision_datetime and isinstance(revision_datetime, datetime.datetime):
            revision_datetime = revision_datetime.strftime("%Y-%m-%d")
        comment = model.Comment(name="Revision Date", value=revision_datetime)
        isa_study.comments.append(comment)
        log = revision_comment or ""
        log = log.strip().replace("\t", " ").replace("\n", " ")
        comment = model.Comment(name="Revision Log", value=log)
        isa_study.comments.append(comment)

        for comment in revision_comments:
            isa_study.comments.remove(comment)


class CharacteristicDescription(BaseModel):
    name: str
    type: OntologyValue
    format: str


def consolidate_keywords(
    isa_study: model.Study, study_path: str, source: str = "workflows"
):
    ontology_terms: OrderedDict[str, model.OntologyAnnotation] = OrderedDict()
    study_keywords: OrderedDict[str, model.OntologyAnnotation] = OrderedDict()
    instruments = get_instruments(isa_study, study_path, source)
    sample_descriptors = get_sample_descriptors(isa_study, study_path, source)
    ontology_terms.update(instruments)
    ontology_terms.update(sample_descriptors)

    desc_comments = OrderedDict(
        [
            ("Assay Descriptor", []),
            ("Assay Descriptor Term Accession Number", []),
            ("Assay Descriptor Term Source REF", []),
            ("Assay Descriptor Category", []),
            ("Assay Descriptor Source", []),
        ]
    )

    omics_type_comments = OrderedDict(
        [
            ("Omics Type", []),
            ("Omics Type Term Accession Number", []),
            ("Omics Type Term Source REF", []),
        ]
    )
    assay_type_comments = OrderedDict(
        [
            ("Assay Type", []),
            ("Assay Type Term Accession Number", []),
            ("Assay Type Term Source REF", []),
        ]
    )

    for item in isa_study.assays:
        assay: model.Assay = item
        for comment in assay.comments:
            if comment.name in desc_comments:
                desc_comments[comment.name].extend(comment.value.split(";"))
            if comment.name in omics_type_comments:
                omics_type_comments[comment.name].extend(comment.value.split(";"))

            if comment.name in assay_type_comments:
                assay_type_comments[comment.name].extend(comment.value.split(";"))
            if assay.measurement_type and assay.measurement_type.term:
                key = assay.measurement_type.term.lower()
                if key not in ontology_terms:
                    ontology_terms[key] = model.OntologyAnnotation(
                        term=assay.measurement_type.term,
                        term_accession=assay.measurement_type.term_accession,
                        term_source=assay.measurement_type.term_source,
                        comments=[
                            model.Comment(
                                name="Study Design Type Category",
                                value="measurement-type",
                            ),
                            model.Comment(
                                name="Study Design Type Source", value=source
                            ),
                        ],
                    )

        for desc_comment in desc_comments.get("Assay Descriptor", []):
            for idx, part in enumerate(desc_comment):
                if part.lower() in ontology_terms:
                    continue
                ontology_terms[part.lower()] = model.OntologyAnnotation()
                ontology = ontology_terms[part.lower()]
                ontology.term = part
                accessions = desc_comments.get(
                    "Assay Descriptor Term Accession Number", []
                )
                if len(accessions) > idx:
                    ontology.term_accession = accessions[idx]
                sources = desc_comments.get("Assay Descriptor Term Source REF", [])
                if len(sources) > idx:
                    ontology.term_source = model.OntologySource(name=sources[idx])
                categories = desc_comments.get("Assay Descriptor Category", [])
                if not ontology.comments:
                    ontology.comments = []
                if len(categories) > idx:
                    ontology.comments.append(
                        model.Comment(
                            name="Study Design Type Category", value=categories[idx]
                        )
                    )
                sources = desc_comments.get("Assay Descriptor Source", [])
                if len(sources) > idx:
                    ontology.comments.append(
                        model.Comment(name="Study Design Type Source", value=source)
                    )

        part = omics_type_comments.get("Omics Type", [""])[0]
        if part and part.lower() not in ontology_terms:
            ontology_terms[part.lower()] = model.OntologyAnnotation(
                comments=[
                    model.Comment(
                        name="Study Design Type Category",
                        value="omics-type",
                    ),
                    model.Comment(name="Study Design Type Source", value=source),
                ],
            )
            ontology = ontology_terms[part.lower()]
            ontology.term = part
            ontology.term_accession = omics_type_comments.get(
                "Omics Type Term Accession Number", [""]
            )[0]
            ontology.term_source = model.OntologySource(
                name=omics_type_comments.get("Omics Type Term Source REF", [""])[0]
            )
        part = omics_type_comments.get("Assay Type", [""])[0]
        if part and part.lower() not in ontology_terms:
            ontology_terms[part.lower()] = model.OntologyAnnotation(
                comments=[
                    model.Comment(
                        name="Study Design Type Category",
                        value="assay-type",
                    ),
                    model.Comment(name="Study Design Type Source", value=source),
                ],
            )
            ontology = ontology_terms[part.lower()]
            ontology.term = part
            ontology.term_accession = assay_type_comments.get(
                "Assay Type Term Accession Number", [""]
            )[0]
            ontology.term_source = model.OntologySource(
                name=assay_type_comments.get("Assay Type Term Source REF", [""])[0]
            )

    for item in isa_study.design_descriptors:
        descriptor: model.OntologyAnnotation = item
        study_keywords[descriptor.term.lower()] = item
    category_name = "Study Design Type Category"
    for item in ontology_terms:
        if item not in study_keywords:
            isa_study.design_descriptors.append(ontology_terms[item])
        else:
            descriptor_source = ontology_terms[item].get_comment(category_name)
            category = study_keywords[item].get_comment(category_name)
            if descriptor_source:
                if not category:
                    study_keywords[item].comments.append(descriptor_source)
                elif category.value != descriptor_source.value:
                    category.value = descriptor_source.value

    remove_list = []
    for keyword, onto in study_keywords.items():
        if keyword not in ontology_terms:
            comment = onto.get_comment(category_name)
            if comment and comment.value == source:
                remove_list.append(onto)
    for onto in remove_list:
        isa_study.design_descriptors.remove(onto)


def get_instruments(
    isa_study: model.Study, study_path: str, source: str
) -> dict[str, model.OntologyAnnotation]:
    instruments: dict[str, model.OntologyAnnotation] = {}
    for item in isa_study.assays:
        assay: model.Assay = item
        assay_file_path = os.path.join(study_path, assay.filename)
        if not os.path.exists(assay_file_path):
            continue

        df = read_tsv(assay_file_path)
        for idx, column_name in enumerate(df.columns):
            if "instrument" not in column_name.lower():
                continue
            result = re.match(r".+\[(.+)\].*", column_name)
            category = result.groups()[0] if result else "instrument"
            unique_vals = set()
            for row_idx, x in enumerate(df[column_name].tolist()):
                if not x or not x.strip() or x.strip().lower() in unique_vals:
                    continue
                key = x.strip().lower()
                unique_vals.add(key)
                source_ref = None
                accession = None
                if len(df.columns) > idx + 2 and df.columns[idx + 2].startswith(
                    "Term Accession Number"
                ):
                    source_ref = df[df.columns[idx + 1]][row_idx]
                    accession = df[df.columns[idx + 2]][row_idx]
                instruments[key] = model.OntologyAnnotation(
                    term=x,
                    term_accession=accession,
                    term_source=model.OntologySource(name=source_ref),
                    comments=[
                        model.Comment(
                            name="Study Design Type Category",
                            value=category,
                        ),
                        model.Comment(name="Study Design Type Source", value=source),
                    ],
                )
    return instruments


def get_sample_descriptors(
    isa_study: model.Study, study_path: str, source: str
) -> dict[str, model.OntologyAnnotation]:
    descriptors: dict[str, model.OntologyAnnotation] = {}
    sample_file_path = os.path.join(study_path, isa_study.filename)
    if not os.path.exists(sample_file_path):
        return {}

    df = read_tsv(sample_file_path)
    fields = ["Organism", "Organism part", "Disease", "Cell type", "Sample type"]
    for idx, column_name in enumerate(df.columns):
        match = False
        category = ""
        for field in fields:
            if field.lower() in column_name.lower():
                match = True
                category = field
                break
        if not match:
            continue
        unique_vals = set()
        for row_idx, x in enumerate(df[column_name].tolist()):
            if not x or not x.strip() or x.strip().lower() in unique_vals:
                continue
            key = x.strip().lower()
            unique_vals.add(key)
            source_ref = None
            accession = None
            if len(df.columns) > idx + 2 and df.columns[idx + 2].startswith(
                "Term Accession Number"
            ):
                source_ref = df[df.columns[idx + 1]][row_idx]
                accession = df[df.columns[idx + 2]][row_idx]
            descriptors[key] = model.OntologyAnnotation(
                term=x,
                term_accession=accession,
                term_source=model.OntologySource(name=source_ref),
                comments=[
                    model.Comment(
                        name="Study Design Type Category",
                        value=category,
                    ),
                    model.Comment(name="Study Design Type Source", value=source),
                ],
            )
    return descriptors


@staticmethod
def update_mhd_comments(
    isa_study: model.Study,
    study_category: None | int | StudyCategory = None,
    mhd_model_version: None | str = None,
    mhd_accession: None | str = None,
    sample_template: None | str = None,
    template_version: None | str = None,
    study_template: None | str = None,
    created_at: None | datetime.datetime = None,
    characteristic_definitions: None | list[CharacteristicDescription] = None,
) -> list[str]:
    old_comments = {}
    if isa_study.comments:
        old_comments = {x.name: x for x in isa_study.comments}

    new_comments = []

    study_category_name = study_category
    if study_category is not None:
        if isinstance(study_category, StudyCategory):
            study_category_name = study_category.get_label()
        if isinstance(study_category, int):
            try:
                category = StudyCategory(study_category)
            except Exception:
                logger.warning(
                    "'%s' can not be converted to study category for %s",
                    study_category,
                    isa_study.identifier,
                )
                category = StudyCategory.OTHER
            study_category_name = category.get_label()

    mhd_comments_map: OrderedDict[str, model.Comment] = OrderedDict()
    created = created_at.strftime("%Y-%m-%d") if created_at else ""
    mhd_comments_map["created at"] = model.Comment(name="Created At", value=created)
    mhd_comments_map["study category"] = model.Comment(
        name="Study Category", value=str(study_category_name or "")
    )
    mhd_comments_map["template version"] = model.Comment(
        name="Template Version", value=str(template_version or "")
    )
    mhd_comments_map["sample template"] = model.Comment(
        name="Sample Template", value=str(sample_template or "")
    )
    mhd_comments_map["study template"] = model.Comment(
        name="Study Template", value=str(study_template or "")
    )

    if mhd_model_version:
        mhd_comments_map["mhd model version"] = model.Comment(
            name="MHD Model Version", value=str(mhd_model_version)
        )

    if mhd_accession:
        mhd_comments_map["mhd accession"] = model.Comment(
            name="MHD accession", value=mhd_accession
        )
    # if characteristic_definitions:
    #     mhd_comments_map = OrderedDict()
    #     mhd_comments_map["study characteristics name"] = []
    #     mhd_comments_map["study characteristics type"] = []
    #     mhd_comments_map["study characteristics type term accession number"] = []
    #     mhd_comments_map["study characteristics type term source ref"] = []
    #     mhd_comments_map["study characteristics format"] = []
    #     mhd_comments_map["mhd accession"] = model.Comment(
    #         name="MHD accession", value=mhd_accession
    #     )
    #     [x.name or "" for x in characteristic_definitions]

    updated_comments: OrderedDict[str, model.Comment] = OrderedDict()
    for comment_name, comment in old_comments.items():
        if comment_name.lower() not in {
            "created at",
            "study category",
            "sample template",
            "study template",
            "mhd accession",
            "mhd model version",
            "template version",
        }:
            new_comments.append(comment)
        else:
            lower = comment.name.lower()
            new_comment = mhd_comments_map.get(lower, None)
            if (
                not new_comment
                or comment.name != new_comment.name
                or comment.value != new_comment.value
            ):
                updated_comments[comment.name] = "update"

    for v in mhd_comments_map.values():
        if v.name not in updated_comments:
            updated_comments[v.name] = "new"
        new_comments.append(v)
    isa_study.comments = new_comments

    return list(updated_comments.keys())


@staticmethod
def update_characteristics(
    isa_study: model.Study,
    study_category: None | int | StudyCategory = None,
    mhd_model_version: None | str = None,
    mhd_accession: None | str = None,
    sample_template: None | str = None,
    template_version: None | str = None,
) -> list[str]:
    old_comments = {}
    if isa_study.comments:
        old_comments = {x.name: x for x in isa_study.comments}

    new_comments = []

    study_category_name = study_category
    if study_category is not None:
        if isinstance(study_category, StudyCategory):
            study_category_name = study_category.get_label()
        if isinstance(study_category, int):
            try:
                category = StudyCategory(study_category)
            except Exception:
                logger.warning(
                    "'%s' can not be converted to study category for %s",
                    study_category,
                    isa_study.identifier,
                )
                category = StudyCategory.OTHER
            study_category_name = category.get_label()

    mhd_comments_map: OrderedDict[str, model.Comment] = OrderedDict()

    mhd_comments_map["study category"] = model.Comment(
        name="Study Category", value=str(study_category_name or "")
    )
    mhd_comments_map["template version"] = model.Comment(
        name="Template Version", value=str(template_version or "")
    )
    mhd_comments_map["sample template"] = model.Comment(
        name="Sample Template", value=str(sample_template or "")
    )

    if mhd_model_version:
        mhd_comments_map["sample template"] = model.Comment(
            name="mhd model version", value=str(mhd_model_version)
        )

    if mhd_accession:
        mhd_comments_map["mhd accession"] = model.Comment(
            name="mhd accession", value=mhd_accession
        )

    updated_comments: OrderedDict[str, model.Comment] = OrderedDict()
    for comment_name, comment in old_comments.items():
        if comment_name.lower() not in {
            "study category",
            "sample template",
            "mhd accession",
            "mhd model version",
            "template version",
        }:
            new_comments.append(comment)
        else:
            lower = comment.name.lower()
            new_comment = mhd_comments_map.get(lower, None)
            if (
                not new_comment
                or comment.name != new_comment.name
                or comment.value != new_comment.value
            ):
                updated_comments[comment.name] = "update"

    for v in mhd_comments_map.values():
        if v.name not in updated_comments:
            updated_comments[v.name] = "new"
        new_comments.append(v)
    isa_study.comments = new_comments

    return list(updated_comments.keys())


@staticmethod
def update_license(isa_study: model.Study, dataset_license: None | str = None) -> bool:
    license_name = dataset_license or ""
    data_updated = False
    updated_comments = []
    license_comment_updated = False
    for comment in isa_study.comments:
        if comment.name.lower() != "license":
            updated_comments.append(comment)
        elif not license_comment_updated:
            if comment.name != "License" or comment.value != license_name:
                data_updated = True
            comment.name = "License"
            comment.value = license_name
            updated_comments.append(comment)
            license_comment_updated = True
    if not license_comment_updated:
        data_updated = True
        updated_comments.append(model.Comment(name="License", value=license_name))
    isa_study.comments = updated_comments
    return data_updated


if __name__ == "__main__":
    study_id = "MTBLS30008974"
    study_location = get_study_metadata_path(study_id)
    iac = IsaApiClient()
    isa_study, isa_inv, std_path = iac.get_isa_study(
        study_id, None, skip_load_tables=True, study_location=study_location
    )
    consolidate_keywords(isa_study, study_location, source="workflows-status-update")
    print(isa_study.design_descriptors)
