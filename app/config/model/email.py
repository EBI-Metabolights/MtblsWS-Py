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


class EmailTemplateConfiguration(BaseModel):
    metabolights_host_url_in_email: str
    ftp_upload_help_doc_url: str


class EmailServiceSettings(BaseModel):
    connection: EmailServiceConnection
    configuration: EmailServiceConfiguration


class EmailSettings(BaseModel):
    email_service: EmailServiceSettings
    template_email_configuration: EmailTemplateConfiguration
