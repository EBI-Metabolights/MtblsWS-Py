import datetime
import logging
from typing import OrderedDict
from isatools import model

from app.ws.db.types import StudyCategory

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


@staticmethod
def update_mhd_comments(
    isa_study: model.Study,
    study_category: None | int | StudyCategory = None,
    mhd_model_version: None | str = None,
    mhd_accession: None | str = None,
    sample_template: None | str = None,
    template_version: None | str = None
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
