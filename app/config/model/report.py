from pydantic import BaseModel


class ReportSettings(BaseModel):
    mariana_report_folder_name: str
    report_base_folder_name: str
    report_global_folder_name: str = "global"
