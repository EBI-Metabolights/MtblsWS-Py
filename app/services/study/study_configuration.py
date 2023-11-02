import re
from typing import Union


from app.config import ApplicationSettings
from app.services.study.lite_study_configuration import LiteStudyConfiguration
from app.utils import MetabolightsException
from app.ws.db.types import StudyStatus
from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService

class StudyConfiguration(LiteStudyConfiguration):
    
    def __init__(
        self,
        study_id: str,
        obfuscation_code: Union[None, str] = None,
        cluster_mode: bool = False,
        study_folder_relative_path: Union[None, str] = None,
        study_ftp_folder_relative_path: Union[None, str] = None,
        settings: Union[None, ApplicationSettings] = None,
        study_service: Union[None, StudyService] = None,
        user_service: Union[None, UserService] = None,
    ) -> None:
        self.study_status = StudyStatus.DORMANT
        self._db_metadata = None
        super().__init__(study_id, obfuscation_code, cluster_mode, study_folder_relative_path, study_ftp_folder_relative_path, settings)

        self.study_service = study_service
        if not self.study_service:
            self.study_service = StudyService.get_instance()

        self.user_service = user_service
        if not self.user_service:
            self.user_service = UserService.get_instance()
            
            
    
    def check_db_consistency(self):
        if self.db_metadata.acc != self.study_id:
            raise MetabolightsException(message="Invalid object. Study id is not matched.")
        
    @property
    def db_metadata(self):
        if not self._db_metadata:
            self._db_metadata = self.study_service.get_study_by_acc(self.study_id)
            self.obfuscation_code = self._db_metadata.obfuscationcode
            self.study_status = StudyStatus(self._db_metadata.status)
            self.check_db_consistency()
            self.recalculate_private_ftp_paths()
        return self._db_metadata



if __name__ == "__main__":
    study_configuration = StudyConfiguration(study_id="MTBLS1", cluster_mode=True)
    db_metadata = study_configuration.db_metadata

    print(db_metadata)
