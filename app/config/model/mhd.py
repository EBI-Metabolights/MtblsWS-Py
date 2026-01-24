from pydantic import BaseModel


class MhdSettings(BaseModel):
    api_key: str
    mhd_webservice_base_url: str = "https://www.metabolomicshub.org/api/submission"
