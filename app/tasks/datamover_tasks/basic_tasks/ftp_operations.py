import datetime
import json
import logging
import os
import pathlib
from typing import Dict, List, OrderedDict, Union

import boto3
from celery.result import AsyncResult

from app.config import get_settings
from app.services.storage_service.acl import Acl
from app.services.storage_service.models import SyncTaskResult, SyncTaskStatus
from app.study_folder_utils import FileDescriptor
from app.tasks.worker import MetabolightsTask, celery
from app.utils import current_time
from app.ws.redis.redis import get_redis_server

logger = logging.getLogger("wslog")


@celery.task(
    base=MetabolightsTask,
    bind=True,
    name="app.tasks.datamover_tasks.basic_tasks.ftp_operations.index_study_data_files",
    autoretry_for={Exception},
    default_retry_delay=1,
    max_retries=1,
)
def index_study_data_files(
    self, study_id: str, obfuscation_code: str, recursive: bool = True, *args, **kwargs,
) -> Dict[str, FileDescriptor]:
    folder_name = f"{study_id.lower()}-{obfuscation_code}"
    settings = get_settings()
    mounted_paths = settings.hpc_cluster.datamover.mounted_paths
    private_data_files_path = os.path.join(
        mounted_paths.cluster_private_ftp_root_path, folder_name
    )
    metadata_revisions_path = os.path.join(
        mounted_paths.cluster_public_ftp_root_path, study_id, "METADATA_REVISIONS"
    )

    file_descriptors: OrderedDict[str, FileDescriptor] = OrderedDict()
    ignore_files = [
        settings.study.audit_files_symbolic_link_name,
        settings.study.internal_files_symbolic_link_name,
    ]
    study_private_ftp_path_item = pathlib.Path(private_data_files_path)
    ordered_private_data_files = OrderedDict()
    if study_private_ftp_path_item.exists():
        current = os.stat(private_data_files_path).st_mode & 0o777
        try:
            if current != Acl.READ_ONLY.value:
                os.chmod(private_data_files_path, mode=Acl.READ_ONLY.value)
            file_descriptors = get_ftp_data_files(
                root_path=private_data_files_path,
                file_descriptors=file_descriptors,
                root=study_private_ftp_path_item,
                recursive=recursive,
                ignore_files=ignore_files,
            )
        finally:
            if current != Acl.READ_ONLY.value:
                os.chmod(private_data_files_path, mode=current)

        data_files = {}
        for item, descriptor in file_descriptors.items():
            if descriptor.name.endswith(".tsv") and descriptor.name.startswith("m_"):
                continue
            if (
                descriptor.name.endswith(".txt")
                and len(descriptor.name) > 2
                and descriptor.name[:2] in {"i_", "s_", "a_"}
            ):
                continue
            descriptor.relative_path = f"FILES/{descriptor.relative_path}"
            descriptor.parent_relative_path = (
                f"FILES/{descriptor.parent_relative_path}"
                if descriptor.parent_relative_path
                else "FILES"
            )
            data_files[f"FILES/{item}"] = descriptor.model_dump()
        keys = list(data_files.keys())
        keys.sort()
        ordered_private_data_files = OrderedDict([(x, data_files[x]) for x in keys])
    else:
        raise Exception(f"There is no folder on private ftp {folder_name}")

    revision_number_on_public_ftp = 0
    metadata_revisions_path_item = pathlib.Path(metadata_revisions_path)
    if metadata_revisions_path_item.exists():
        revision_files = [x.name for x in metadata_revisions_path_item.iterdir()]
        revision_files.sort()
        try:
            if (
                revision_files
                and revision_files[-1]
                and revision_files[-1].startswith(study_id)
                and "_" in revision_files[-1]
            ):
                revision_number_on_public_ftp = int(revision_files[-1].split("_")[1])
        except Exception as ex:
            pass
    if settings.study.public_study_storage_type == "nfs":
        public_data_files, error_message = get_file_index_from_nfs_storage(
            study_id=study_id, recursive=recursive, ignore_files=ignore_files
        )
    elif settings.study.public_study_storage_type == "object-storage":
        public_data_files, error_message = get_file_index_from_object_storage(
            study_id=study_id, recursive=recursive, ignore_files=ignore_files
        )
    else:
        raise Exception(
            f"Invalid public study storage type: {settings.study.public_study_storage_type}"
        )
    keys = list(public_data_files.keys())
    keys.sort()
    ordered_public_data_files = OrderedDict([(x, public_data_files[x]) for x in keys])

    target_root_path = os.path.join(
        mounted_paths.cluster_study_internal_files_root_path, study_id, "DATA_FILES"
    )
    os.makedirs(target_root_path, exist_ok=True)
    target_path = os.path.join(target_root_path, "data_file_index.json")
    result = {
        "private_data_files": ordered_private_data_files,
        "public_data_files": ordered_public_data_files,
    }
    result["index_datetime"] = current_time().isoformat()
    result["study_id"] = study_id
    result["revision_number_on_public_ftp"] = revision_number_on_public_ftp

    with open(target_path, "w") as f:
        json.dump(result, f, indent=2)
    return {
        "study_id": study_id,
        "index_datetime": result["index_datetime"],
        "target_path": target_path,
    }


def get_file_index_from_object_storage(
    study_id: str, recursive: bool = True, ignore_files: Union[None, List[str]] = None
) -> Dict[str, FileDescriptor]:
    settings = get_settings()
    error_message = None
    s3_api_url = settings.study.public_study_object_storage_url
    storage_bucket_name = settings.study.public_study_object_storage_bucket_name
    access_key_id = settings.study.public_study_object_storage_access_key_id
    secress_access_key = settings.study.public_study_object_storage_secret_access_key
    region = settings.study.public_study_object_storage_region
    root_subfolder = settings.study.public_study_object_storage_subfolder
    subfolder = f"{root_subfolder}/{study_id}" if root_subfolder else study_id
    public_data_files, error_message = search_files_on_object_storage(
        s3_api_url,
        storage_bucket_name,
        access_key_id,
        secress_access_key,
        region,
        subfolder,
        recursive,
        ignore_files,
    )

    result: Dict[str, FileDescriptor] = {}
    parent_folders = set()
    if public_data_files:
        # convert FileDescriptor models to dicts with expected key format
        for item, descriptor in public_data_files.items():
            if (
                descriptor.parent_relative_path
                and descriptor.parent_relative_path != "FILES"
            ):
                parent_folders.add(descriptor.parent_relative_path)
            key = f"{item}" if item.startswith("FILES/") else f"FILES/{item}"
            # descriptor may already be a dict or a FileDescriptor
            if hasattr(descriptor, "model_dump"):
                result[key] = descriptor.model_dump()
            else:
                result[key] = descriptor
    for parent_folder in parent_folders:
        if parent_folder not in result:
            result[parent_folder] = FileDescriptor(
                name=pathlib.Path(parent_folder).name,
                relative_path=parent_folder,
                is_dir=True,
                modified_time=0,
                extension="",
                file_size=0,
                is_empty=False,
                parent_relative_path=str(pathlib.Path(parent_folder).parent)
                if "/" in parent_folder
                else "",
            ).model_dump()
    return result, error_message


def search_files_on_object_storage(
    s3_api_url: str,
    storage_bucket_name: str,
    access_key_id: str,
    secret_access_key: str,
    region: str,
    subfolder: str,
    recursive: bool = True,
    ignore_files: Union[None, List[str]] = None,
):
    error_message = None
    public_data_files: Dict[str, FileDescriptor] = {}
    if not storage_bucket_name:
        return (
            public_data_files,
            "Public study object storage bucket name is not defined.",
        )
    try:
        if access_key_id and secret_access_key:
            s3 = boto3.client(
                "s3",
                endpoint_url=s3_api_url,
                region_name=region,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
            )
        else:
            raise Exception(
                "Object storage access key ID and secret access key must be provided."
            )

        paginator = s3.get_paginator("list_objects_v2")
        prefix = f"{subfolder}/FILES"
        for page in paginator.paginate(Bucket=storage_bucket_name, Prefix=prefix):
            if "Contents" not in page:
                continue
            for obj in page["Contents"]:
                key: str = obj["Key"]
                # drop the leading '<subfolder>/' to match NFS relative paths
                relative_path = key.replace(f"{subfolder}/", "", 1).strip("/")

                if ignore_files:
                    if any(relative_path.startswith(x) for x in ignore_files):
                        continue

                name = pathlib.Path(relative_path).name
                parent_relative_path = ""
                if "/" in relative_path:
                    parent_relative_path = str(pathlib.Path(relative_path).parent)

                is_dir = key.endswith("/")
                try:
                    modified_time = obj["LastModified"].timestamp()
                except Exception:
                    modified_time = 0

                file_size = obj["Size"] if not is_dir else 0
                extension = pathlib.Path(name).suffix if not is_dir else ""
                is_empty = True if (not is_dir and file_size == 0) else False

                fd = FileDescriptor(
                    name=name,
                    relative_path=relative_path,
                    is_dir=is_dir,
                    modified_time=modified_time,
                    extension=extension,
                    file_size=file_size,
                    is_empty=is_empty,
                    parent_relative_path=parent_relative_path,
                )
                public_data_files[relative_path] = fd

    except Exception as ex:
        error_message = f"Failed to list objects from object storage: {ex}"
        logger.error(error_message)

    return public_data_files, error_message


def get_file_index_from_nfs_storage(
    study_id: str, recursive: bool = True, ignore_files: Union[None, List[str]] = None
) -> Dict[str, FileDescriptor]:
    settings = get_settings()
    mounted_paths = settings.hpc_cluster.datamover.mounted_paths
    public_data_files_path = os.path.join(
        mounted_paths.cluster_public_ftp_root_path, study_id, "FILES"
    )
    public_data_files_path_item = pathlib.Path(public_data_files_path)
    public_data_files = {}
    public_data_file_descriptors: OrderedDict[str, FileDescriptor] = OrderedDict()
    error_message = None
    try:
        if public_data_files_path_item.exists():
            public_data_file_descriptors = get_ftp_data_files(
                root_path=public_data_files_path,
                file_descriptors=public_data_file_descriptors,
                root=public_data_files_path_item,
                recursive=recursive,
                ignore_files=ignore_files,
            )

            for item, descriptor in public_data_file_descriptors.items():
                descriptor.relative_path = f"FILES/{descriptor.relative_path}"
                descriptor.parent_relative_path = (
                    f"FILES/{descriptor.parent_relative_path}"
                    if descriptor.parent_relative_path
                    else "FILES"
                )
                public_data_files[f"FILES/{item}"] = descriptor.model_dump()
    except Exception as ex:
        error_message = f"Failed to list files from NFS storage: {ex}"
        logger.error(error_message)
    return public_data_files, error_message


def get_ftp_data_files(
    root_path: str,
    file_descriptors: Dict[str, FileDescriptor],
    root: pathlib.Path,
    recursive=True,
    ignore_files: Union[None, List[str]] = None,
):
    iterator = get_study_folder_files(
        root_path=root_path,
        file_descriptors=file_descriptors,
        root=root,
        recursive=recursive,
        ignore_files=ignore_files,
    )

    list(iterator)
    return file_descriptors


def get_study_folder_files(
    root_path: str,
    file_descriptors: Dict[str, FileDescriptor],
    root: pathlib.Path,
    recursive: bool = True,
    ignore_files: Union[None, List[str]] = None,
):
    relative_root_path = str(root).replace(f"{root_path}", "").lstrip("/")
    if str(root_path) != str(root):
        if not relative_root_path:
            yield root
    for item in root.iterdir():
        relative_path = str(item).replace(f"{root_path}", "").lstrip("/")

        if ignore_files:
            ignore = [x for x in ignore_files if relative_path.startswith(x)]
            if ignore:
                continue

        if item.name.startswith(".nfs"):
            continue
        m_time = os.path.getmtime(item)

        parent_relative_path = ""
        if "/" in relative_path:
            parent_relative_path = str(pathlib.Path(relative_path).parent)

        if item.is_dir():
            file_descriptors[relative_path] = FileDescriptor(
                name=item.name,
                relative_path=relative_path,
                is_dir=True,
                modified_time=m_time,
                extension=item.suffix,
                parent_relative_path=parent_relative_path,
            )
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
            file_descriptors[relative_path] = FileDescriptor(
                name=item.name,
                relative_path=relative_path,
                is_dir=False,
                modified_time=m_time,
                extension=item.suffix,
                file_size=file_size,
                is_empty=is_empty,
                parent_relative_path=parent_relative_path,
            )

            yield item


def sync_private_ftp_data_files(study_id: str, obfuscation_code: str) -> SyncTaskResult:
    redis = get_redis_server()
    now = current_time()
    now_str = now.isoformat()
    now_time = now.timestamp()
    mounted_paths = get_settings().study.mounted_paths
    target_root_path = os.path.join(
        mounted_paths.study_internal_files_root_path, study_id, "DATA_FILES"
    )
    target_path = os.path.join(target_root_path, "data_file_index.json")
    current_datetime_result = redis.get_value(
        f"{study_id}:index_private_ftp_storage:current_datetime"
    )
    if current_datetime_result and os.path.exists(target_path):
        current_datetime = current_datetime_result.decode()
        if os.path.exists(target_path):
            with open(target_path) as f:
                all_directory_files = json.load(f)
            index_datetime = all_directory_files["index_datetime"]
            last_index_time = datetime.datetime.fromisoformat(
                index_datetime
            ).timestamp()
            current_index_time = datetime.datetime.fromisoformat(
                current_datetime
            ).timestamp()
            status = None
            task_id_result = redis.get_value(
                f"{study_id}:index_private_ftp_storage:task_id"
            )
            task_id = task_id_result.decode() if task_id_result else None
            if last_index_time > current_index_time:
                result: AsyncResult = celery.AsyncResult(task_id)
                if result.ready:
                    redis.delete_value(f"{study_id}:index_private_ftp_storage:task_id")
                    redis.delete_value(
                        f"{study_id}:index_private_ftp_storage:current_datetime"
                    )
                if result.successful:
                    data_result = result.get()
                    logger.debug(data_result)
                    status = SyncTaskResult(
                        task_id=task_id,
                        dry_run=False,
                        description="Private FTP Sync completed",
                        status=SyncTaskStatus.COMPLETED_SUCCESS,
                        task_done_time_str=index_datetime,
                        task_done_timestamp=last_index_time,
                        last_update_time=now_str,
                        last_update_timestamp=now_time,
                    )
            else:
                status = SyncTaskResult(
                    task_id=task_id,
                    dry_run=False,
                    description="Private FTP Sync running...",
                    status=SyncTaskStatus.RUNNING,
                    last_update_time=now_str,
                    last_update_timestamp=now_time,
                )
            if status:
                if status.description and len(status.description) > 100:
                    status.description = f"{status.description[:100]} ..."
                return status

    redis.set_value(
        f"{study_id}:index_private_ftp_storage:current_datetime", now_str, ex=30 * 60
    )
    inputs = {
        "study_id": study_id,
        "obfuscation_code": obfuscation_code,
        "recursive": True,
    }
    task = index_study_data_files.apply_async(kwargs=inputs)
    task_id = task.id
    redis.set_value(
        f"{study_id}:index_private_ftp_storage:task_id", task.id, ex=30 * 60
    )
    return SyncTaskResult(
        task_id=task_id,
        dry_run=False,
        description="Private FTP Data File Index task started",
        status=SyncTaskStatus.PENDING,
        last_update_time=now_str,
        last_update_timestamp=now_time,
    )


if __name__ == "__main__":
    study_id = "MTBLS1"
    result = get_file_index_from_object_storage(study_id=study_id, recursive=True)
    print(result)
