from pydantic import BaseModel


class FtpServerConnection(BaseModel):
    host: str = "ftp-private.ebi.ac.uk"
    username: str
    password: str


class PrivateFtpServerConfiguration(BaseModel):
    mount_type: str
    private_ftp_user_home_path: str
    studies_folder_absolute_path: str
    private_ftp_folders_relative_path: str


class PrivateFtpServerSettings(BaseModel):
    connection: FtpServerConnection
    configuration: PrivateFtpServerConfiguration


class PublicFtpServerConfiguration(BaseModel):
    mount_type: str
    studies_folder_absolute_path: str


class PublicFtpServerSettings(BaseModel):
    configuration: PublicFtpServerConfiguration


class FtpServerSettings(BaseModel):
    private: PrivateFtpServerSettings
    public: PublicFtpServerSettings
