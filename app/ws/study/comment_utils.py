import datetime
import logging
from typing import OrderedDict

from isatools import model
from pydantic import BaseModel

from app.ws.db.types import StudyCategory
from app.ws.study.isa_table_models import OntologyValue

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


def consolidate_keywords(isa_study: model.Study):
    study_keywords: OrderedDict[str, model.OntologyAnnotation] = OrderedDict()
    desc_comments = OrderedDict(
        [
            ("Assay Descriptor", []),
            ("Assay Descriptor Term Accession Number", []),
            ("Assay Descriptor Term Source REF", []),
            ("Assay Descriptor Category", []),
            ("Assay Descriptor Source", []),
        ]
    )
    for item in isa_study.assays:
        assay: model.Assay = item
        for comment in assay.comments:
            if comment.name in desc_comments:
                desc_comments[comment.name].extend(comment.value.split(";"))
    ontology_terms: OrderedDict[str, model.OntologyAnnotation] = OrderedDict()

    for item in desc_comments.get("Assay Descriptor", []):
        for idx, part in enumerate(item):
            if part.lower() in ontology_terms:
                continue
            ontology_terms[part.lower()] = model.OntologyAnnotation()
            ontology = ontology_terms[part.lower()]
            ontology.term = part
            accessions = desc_comments.get("Assay Descriptor Term Accession Number", [])
            if len(accessions) > idx:
                ontology.term_accession = accessions[idx]
            sources = desc_comments.get("Assay Descriptor Term Source REF", [])
            if len(sources) > idx:
                ontology.term_source = sources[idx]
            categories = desc_comments.get("Assay Descriptor Category", [])
            if not ontology.comments:
                ontology.comments = []
            if len(categories) > idx:
                ontology.comments.append(
                    model.Comment(
                        name="Assay Descriptor Category", value=categories[idx]
                    )
                )
            sources = desc_comments.get("Assay Descriptor Source", [])
            if len(sources) > idx:
                ontology.comments.append(
                    model.Comment(name="Assay Descriptor Source", value=categories[idx])
                )

    for item in isa_study.design_descriptors:
        descriptor: model.OntologyAnnotation = item
        study_keywords[descriptor.term.lower()] = descriptor
    for item in ontology_terms:
        if item not in study_keywords:
            isa_study.design_descriptors.append(ontology_terms[item])


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
