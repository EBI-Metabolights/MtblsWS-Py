from app.services.storage_service.storage_service import StorageService
from app.tasks.system_monitor_tasks.integrity_tests.utils import check_result

@check_result(category="paths")
def check_private_ftp():
    private_ftp_sm = StorageService.get_ftp_private_storage()
    return private_ftp_sm.remote.create_folder("mtbls-test-9999999999-folder")
