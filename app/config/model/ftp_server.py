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


class PublicFtpServerSettings(BaseModel):
    configuration: PublicFtpServerConfiguration


class FtpServerSettings(BaseModel):
    private: PrivateFtpServerSettings
    public: PublicFtpServerSettings
