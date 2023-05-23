import os
from app.services.storage_service.unmounted.readonly_volume_manager import ReadOnlyFileVolumeManager
from app.tasks.worker import MetabolightsTask, celery, get_flask_app
from app.ws.settings.utils import get_cluster_settings
from app.ws.study.study_service import StudyService

@celery.task(
    base=MetabolightsTask,
    bind=True,
    name="app.tasks.common.remote_folder_operations.create_readonly_study_folders",
    soft_time_limit=110,
    time_limit=120,
    autoretry_for={Exception},
    default_retry_delay=10,
    max_retries=2,
)
def create_readonly_study_folders(self, study_id=None):

    flask_app = get_flask_app()
    with flask_app.app_context():
        study = StudyService.get_instance(flask_app).get_study_by_acc(study_id)
        root_folders = []
        cluster_setttings = get_cluster_settings()
        root_folders.append(cluster_setttings.cluster_study_readonly_audit_files_root_path)
        root_folders.append(cluster_setttings.cluster_study_readonly_files_root_path)
        root_folders.append(cluster_setttings.cluster_study_readonly_metadata_files_root_path)
        root_folders.append(cluster_setttings.cluster_study_readonly_public_metadata_versions_root_path)
        root_folders.append(cluster_setttings.cluster_study_readonly_integrity_check_files_root_path)
        folders = []
        for path in root_folders:
            folders.append(os.path.join(path, study_id))
        manager = ReadOnlyFileVolumeManager()
        
        manager.create_folder(study.acc, folders)
        