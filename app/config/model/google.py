from typing import List
from pydantic import BaseModel


class GoogleCalenderConnection(BaseModel):
    project_id: str
    client_id: str
    auth_uri: str
    token_uri: str
    auth_provider_x509_cert_url: str
    client_secret: str
    redirect_uris: List[str] = []


class GoogleCalenderInstalledConnection(BaseModel):
    installed: GoogleCalenderConnection


class GoogleSheetConnection(BaseModel):
    type: str
    project_id: str
    private_key_id: str
    private_key: str
    client_email: str
    client_id: str
    auth_uri: str
    token_uri: str
    auth_provider_x509_cert_url: str
    client_x509_cert_url: str


class GoogleServices(BaseModel):
    google_calendar_id: str
    google_analytics_tracking_id: str
    google_mariana_drive_id: str


class GoogleConnection(BaseModel):
    google_sheet_api: GoogleSheetConnection
    google_calender_api: GoogleCalenderInstalledConnection


class GoogleSheets(BaseModel):
    zooma_sheet: str
    europe_pmc_report: str
    mtbls_statistics: str
    lc_ms_statistics: str
    mtbls_curation_log: str


class GoogleSettings(BaseModel):
    connection: GoogleConnection
    services: GoogleServices
    sheets: GoogleSheets
