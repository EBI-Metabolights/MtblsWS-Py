from pydantic import BaseModel


class FtpServerConnection(BaseModel):
    host: str = "ftp-private.ebi.ac.uk"
    username: str
    password: str


class MountedFtpStorageConfiguration(BaseModel):
    ftp_folders_root_path: str = ""


class PrivateFtpServerConfiguration(BaseModel):
    mount_type: str = "remote_worker"


class PrivateFtpServerSettings(BaseModel):
    connection: FtpServerConnection
    configuration: PrivateFtpServerConfiguration


class PublicFtpServerConfiguration(BaseModel):
    mount_type: str = "remote_worker"
    mounted_public_ftp_folders_root_path: str = ""
    public_studies_http_base_url: str = (
        "http://ftp.ebi.ac.uk/pub/databases/metabolights/studies/public"
    )
    public_studies_ftp_base_url: str = (
        "ftp://ftp.ebi.ac.uk/pub/databases/metabolights/studies/public"
    )
    public_studies_globus_base_url: str = "https://app.globus.org/file-manager?origin_id=47772002-3e5b-4fd3-b97c-18cee38d6df2&origin_path=/pub/databases/metabolights/studies/public"
    public_studies_aspera_base_path: str = "/studies/public"


class PublicFtpServerSettings(BaseModel):
    configuration: PublicFtpServerConfiguration


class FtpServerSettings(BaseModel):
    private: PrivateFtpServerSettings
    public: PublicFtpServerSettings
