from datetime import datetime
from pydantic import BaseModel

class StudyContact(BaseModel):
    full_name: str = ""
    emails: list[str] = []


class StudyOverview(BaseModel):
    study_id: str = ""
    title: str = ""
    status: str = ""
    submitters: list[StudyContact] = []
    contacts: list[StudyContact] = []
    created_at: None | datetime = None
    first_private_date: None | datetime = None
    first_public_date: None | datetime = None
    revision_number: int = 0
    revision_datetime: None | datetime = None
    reserved_accession: None | str = None
    reserved_submission_id: None | str = None
    

class MetaboLightsStudyReport(BaseModel):
    report_datetime: datetime
    studies: list[StudyOverview] = []