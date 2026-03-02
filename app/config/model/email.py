from pydantic import BaseModel


class EmailServiceConnection(BaseModel):
    host: str
    port: int = 25
    username: str
    password: str
    use_tls: bool = False
    use_ssl: bool = False


class EmailServiceConfiguration(BaseModel):
    no_reply_email_address: str
    curation_email_address: str
    technical_issue_recipient_email_address: str
    hpc_cluster_job_track_email_address: str


class EmailServiceSettings(BaseModel):
    connection: EmailServiceConnection
    configuration: EmailServiceConfiguration


class EmailSettings(BaseModel):
    email_service: EmailServiceSettings
