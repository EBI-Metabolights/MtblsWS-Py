from pydantic import BaseModel


class ReportSettings(BaseModel):
    reporting_root_path: str
    mariana_path: str
    reporting_path: str
