from datetime import datetime
import pathlib
from pydantic import BaseModel, EmailStr


class StudyContact(BaseModel):
    full_name: str = ""
    emails: list[EmailStr] = []


class StudyPublication(BaseModel):
    title: str = ""
    authors_list: str = ""
    doi: str = ""
    pub_med_id: str = ""


class StudyOverview(BaseModel):
    study_id: str = ""
    title: str = ""
    status: str = ""
    submitters: list[StudyContact] = []
    publications: list[StudyPublication] = []
    contacts: list[StudyContact] = []
    created_at: None | datetime = None
    first_private_date: None | datetime = None
    first_public_date: None | datetime = None
    revision_number: int = 0
    revision_datetime: None | datetime = None
    reserved_accession: None | str = ""
    reserved_submission_id: None | str = ""


class MetaboLightsStudyReport(BaseModel):
    report_datetime: datetime
    studies: list[StudyOverview] = []

    def filter_study_report(
        self,
        status: str,
        exclude_submitter_emails: None | set[str] = None,
        min_created_at: None | datetime = None,
        max_created_at: None | datetime = None,
    ) -> list[StudyOverview]:
        filtered_studies: list[StudyOverview] = []
        for item in self.studies:
            if min_created_at and item.created_at < min_created_at:
                continue
            if max_created_at and item.created_at > max_created_at:
                continue
            if exclude_submitter_emails:
                for submitter in item.submitters:
                    for email in submitter.emails:
                        if email in exclude_submitter_emails:
                            continue
            if item.status.lower() == status.lower():
                filtered_studies.append(item)
        filtered_studies.sort(key=lambda x: x.created_at, reverse=True)
        return filtered_studies


def load_study_report(
    study_report_path: str = "study_report.json",
) -> MetaboLightsStudyReport:
    target_path = pathlib.Path(study_report_path)
    if not study_report_path:
        raise Exception("Report path is not defined.")
    if not target_path.exists():
        raise Exception(f"Report path '{study_report_path}' does not exist.")

    json_str = target_path.open().read()
    report = MetaboLightsStudyReport.model_validate_json(json_str)
    return report


def save_study_report(
    report: MetaboLightsStudyReport,
    study_report_path: str = "study_report.json",
) -> None:
    target_path = pathlib.Path(study_report_path)
    target_path.open("w").write(report.model_dump_json(indent=2))
