import json
from pydantic import BaseModel
from typing_extensions import OrderedDict
from app.study_folder_utils import FileDescriptor
from app.tasks.common_tasks.basic_tasks.email import send_email_for_new_provisional_study
from app.tasks.worker import MetabolightsTask, celery
from app.utils import current_time
from app.ws.db.schemes import Study
from app.ws.study.commons import create_ftp_folder
from app.ws.study.study_service import StudyService
import os
import pathlib
from typing import Dict, List, Union

from app.config import get_settings
from app.tasks.worker import MetabolightsTask, report_internal_technical_issue, send_email, celery

from app.ws.db.schemes import Study

@celery.task(
    base=MetabolightsTask,
    bind=True,
    name="app.tasks.common_tasks.basic_tasks.ftp_operations.create_ftp_folder",
    soft_time_limit=110,
    time_limit=120,
    autoretry_for={Exception},
    default_retry_delay=10,
    max_retries=2,
)
def create_private_ftp_folder(self, user_token=None, study_id=None, send_email=False):
    study: Study = StudyService.get_instance().get_study_by_acc(study_id)
    result = create_ftp_folder(
        study.acc,
        study.obfuscationcode,
        user_token,
        email_service=None,
        send_email=False,
    )

    if send_email:
        if result and "upload_location" in result and result["upload_location"]:
            upload_location = result["upload_location"]

            send_email_for_new_provisional_study(user_token, study_id, upload_location)
    return result

@celery.task(
    base=MetabolightsTask,
    bind=True,
    name="app.tasks.common_tasks.basic_tasks.ftp_operations.index_study_data_files",
    autoretry_for={Exception},
    default_retry_delay=1,
    max_retries=1,
)
def index_study_data_files(self, study_id, obfuscation_code, recursive: bool=True) -> Dict[str, FileDescriptor]:
        # folder_name = f"{study_id.lower()}-{obfuscation_code}"
        # mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
        # data_files_path = os.path.join(mounted_paths.cluster_private_ftp_root_path, folder_name)
        folder_name = study_id
        mounted_paths = get_settings().study.mounted_paths
        data_files_path = os.path.join(mounted_paths.study_metadata_files_root_path, study_id, "FILES")
        file_descriptors: OrderedDict[str, FileDescriptor] = OrderedDict()

        study_source_path_item = pathlib.Path(data_files_path)
        if study_source_path_item.exists():
            file_descriptors = get_private_ftp_data_files(
                root_path=data_files_path, file_descriptors=file_descriptors, root=study_source_path_item, recursive=recursive)
        
            data_files = {}
            for item, descriptor in file_descriptors.items():
                if descriptor.name.endswith(".tsv") and descriptor.name.startswith("m_"):
                    continue
                if descriptor.name.endswith(".txt") and len(descriptor.name) > 2 and  descriptor.name[:2] in {"i_", "s_", "a_"}:
                    continue
                descriptor.relative_path = f"FILES/{descriptor.relative_path}"
                descriptor.parent_relative_path = f"FILES/{descriptor.parent_relative_path}" if descriptor.parent_relative_path else "FILES"
                data_files[f"FILES/{item}"] = descriptor.model_dump()
            target_root_path = os.path.join(mounted_paths.study_internal_files_root_path, study_id, "DATA_FILES")
            os.makedirs(target_root_path, exist_ok=True)
            target_path = os.path.join(target_root_path, "data_file_index.json")
            result = {"data_files": data_files}
            result["index_datetime"] = current_time().isoformat()
            result["study_id"] = study_id
            
            with open(target_path, "w") as f:
                json.dump(result, f, indent=2)
        else:
            raise Exception(f"There is no folder on private ftp {folder_name}")
            

def get_private_ftp_data_files(root_path: str, file_descriptors: Dict[str, FileDescriptor], root: pathlib.Path, recursive=True):
    
    iterator = get_study_folder_files(root_path=root_path, file_descriptors=file_descriptors, root=root, recursive=recursive)
    
    list(iterator)
    return file_descriptors


def get_study_folder_files(root_path: str, file_descriptors: Dict[str, FileDescriptor], root: pathlib.Path, recursive: bool = True):

    relative_root_path = str(root).replace(f"{root_path}", "").lstrip("/")
    if str(root_path) != str(root):
        if not relative_root_path:
            yield root
    for item in root.iterdir():
        relative_path = str(item).replace(f"{root_path}", "").lstrip("/")
        if item.name.startswith(".nfs"):
            continue
        m_time = os.path.getmtime(item)

        parent_relative_path = ""
        if "/" in relative_path:
            parent_relative_path = str(pathlib.Path(relative_path).parent)
            
        if item.is_dir():
            file_descriptors[relative_path] = FileDescriptor(name=item.name, relative_path=relative_path, is_dir=True, modified_time=m_time, extension=item.suffix, parent_relative_path=parent_relative_path)
            if recursive:
                yield from get_study_folder_files(root_path, file_descriptors, item)
            else:
                yield item
        else:
            if item.is_symlink() and not item.resolve().exists():
                continue
            if not item.exists():
                continue
            file_size = 0
            is_empty = True
            if not item.is_symlink() or (item.is_symlink() and item.resolve().exists()):
                file_size = os.path.getsize(item)
                m_time = os.path.getmtime(item)
                is_empty = True if file_size == 0 else False
            file_descriptors[relative_path] = FileDescriptor(name=item.name, relative_path=relative_path, 
                                                             is_dir=False, modified_time=m_time, 
                                                             extension=item.suffix, file_size=file_size, 
                                                             is_empty=is_empty, parent_relative_path=parent_relative_path)
                                
            yield item


if __name__ == "__main__":
    index_study_data_files(study_id="MTBLS8236", obfuscation_code="")