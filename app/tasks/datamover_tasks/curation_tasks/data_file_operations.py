import logging
import os
import shutil
import time
import zipfile
from typing import Any, Dict, Union

from app.config import get_settings
from app.tasks.worker import MetabolightsTask, celery
from app.utils import MetabolightsException
from app.ws.folder_maintenance import MaintenanceException
from app.ws.settings.utils import get_study_settings

logger = logging.getLogger("datamover_worker")


@celery.task(
    bind=True,
    base=MetabolightsTask,
    name="app.tasks.datamover_tasks.curation_tasks.data_file_operations.delete_aspera_files",
)
def delete_aspera_files_from_data_files(self, study_id: str):
    mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
    study_data_path = os.path.join(
        mounted_paths.cluster_study_readonly_files_actual_root_path, study_id
    )

    delete_asper_files(study_data_path)


def scandir_get_aspera(dir):
    subfolders, files = [], []

    for f in os.scandir(dir):
        if f.is_dir():
            subfolders.append(f.path)
        if f.is_file():
            if os.path.splitext(f.name)[1].lower() in (
                ".partial",
                ".aspera-ckpt",
                ".aspx",
            ):
                files.append(f.path)

    for dir in list(subfolders):
        sf, f = scandir_get_aspera(dir)
        subfolders.extend(sf)
        files.extend(f)
    return subfolders, files


def delete_asper_files(directory):
    subs, files = scandir_get_aspera(directory)
    for file_to_delete in files:
        print("File to delete  : " + file_to_delete)
        if os.path.exists(file_to_delete):  # First, does the file/folder exist?
            if os.path.isfile(file_to_delete):  # is it a file?
                os.remove(file_to_delete)


@celery.task(
    bind=True,
    base=MetabolightsTask,
    name="app.tasks.datamover_tasks.curation_tasks.data_file_operations.unzip_folders",
)
def unzip_folders(
    self,
    study_metadata_path: str,
    files: Dict[str, Any],
    remove_zip_files: bool = True,
    override: bool = False,
):
    settings = get_study_settings()
    for file in files:
        if "name" not in file or not file["name"].startswith(
            f"{settings.readonly_files_symbolic_link_name}/"
        ):
            raise MetabolightsException(
                http_code=400,
                message=f"files should start with {settings.internal_files_symbolic_link_name}/",
            )

    messages = {}

    for file in files:
        f_name = file["name"]
        file_path = os.path.join(study_metadata_path, f_name)
        messages[f_name] = {"unzip": False, "delete_zip_file": False, "detail": []}
        if not os.path.exists(file_path) or not os.path.isfile(file_path):
            messages[f_name]["detail"].append(
                f"{file_path} does not exist or not a valid zip file."
            )
            continue
        error = False
        target_relative_path = file_path.replace(".zip", "")
        target_path = os.path.join(study_metadata_path, target_relative_path)
        if os.path.exists(target_path) and not override:
            messages[f_name]["detail"].append(
                f"{target_relative_path} is already exist"
            )
            continue
        try:
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                zip_ref.extractall(path=target_path)
                messages[f_name]["unzip"] = True
        except Exception as e:
            msg = "Could not extract zip file " + f_name
            messages[f_name]["detail"].append(msg)
            logger.error(msg + ":" + str(e))
            error = True
        if not error:
            try:
                if remove_zip_files:
                    if os.path.exists(file_path):
                        if os.path.islink(file_path):
                            os.unlink(file_path)
                        elif os.path.isfile(file_path):  # is it a file?
                            os.remove(file_path)
                        elif os.path.isdir(file_path):  # is it a folder
                            shutil.rmtree(file_path)

                        messages[f_name]["delete_zip_file"] = True
            except:
                msg = "Could not remove zip file " + f_name
                logger.error(msg)
                messages[f_name]["detail"].append(msg)

    return messages


@celery.task(
    bind=True,
    base=MetabolightsTask,
    name="app.tasks.datamover_tasks.curation_tasks.data_file_operations.move_data_files",
)
def move_data_files(
    self,
    study_id: Union[None, str] = None,
    obfuscation_code: str = None,
    files: Dict[str, Any] = None,
    target_location: str = "RECYCLE_BIN",
    override: bool = False,
    task_name=None,
):
    if not study_id or not files:
        raise MaintenanceException(message="Invalid input")

    settings = get_study_settings()
    mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
    for file in files:
        if (
            "name" not in file
            or not file["name"]
            or not file["name"].startswith("FILES/")
        ):
            raise MaintenanceException(message="File names should start with FILES/")

    files_path = os.path.join(
        mounted_paths.cluster_private_ftp_root_path,
        f"{study_id.lower()}-{obfuscation_code}",
    )

    date_format = "%Y-%m-%d_%H-%M-%S"
    timestamp_str = time.strftime(date_format)
    if not task_name:
        task_name = f"{study_id}_MOVE_DATA_FILES_{timestamp_str}"
    recycle_bin_dir = os.path.join(
        mounted_paths.cluster_private_ftp_recycle_bin_root_path, study_id, task_name
    )

    warnings = []
    successes = []
    errors = []
    for file in files:
        if "name" in file and file["name"]:
            f_name = file["name"].replace("FILES/", "", 1)
            try:
                file_path = os.path.join(files_path, f_name)
                if not os.path.exists(file_path):
                    warnings.append(
                        {
                            "file": f_name,
                            "message": "Operation is ignored. File does not exist.",
                        }
                    )
                    continue

                if target_location == "RAW_FILES":
                    new_relative_path = f"RAW_FILES/{f_name}"
                    target_path = os.path.join(files_path, new_relative_path)
                elif target_location == "DERIVED_FILES":
                    new_relative_path = f"DERIVED_FILES/{f_name}"
                    target_path = os.path.join(files_path, new_relative_path)
                elif target_location == "SUPPLEMENTARY_FILES":
                    new_relative_path = f"SUPPLEMENTARY_FILES/{f_name}"
                    target_path = os.path.join(files_path, new_relative_path)
                else:
                    target_path = os.path.join(recycle_bin_dir, f_name)
                parent_directory = os.path.dirname(target_path)
                if not os.path.exists(parent_directory):
                    os.makedirs(parent_directory, exist_ok=True)
                if file_path == target_path:
                    warnings.append(
                        {
                            "file": f_name,
                            "message": "Operation is ignored. Target is same directory.",
                        }
                    )
                    continue

                if not override and os.path.exists(target_path):
                    warnings.append(
                        {
                            "file": f_name,
                            "message": "Operation is ignored. Target file exists.",
                        }
                    )
                    continue
                else:
                    if os.path.exists(target_path):
                        if os.path.islink(target_path):
                            os.unlink(target_path)
                        elif os.path.isfile(target_path):
                            os.remove(target_path)
                        elif os.path.isdir(target_path):
                            shutil.rmtree(target_path)
                        warnings.append(
                            {
                                "file": f_name,
                                "message": "Target file exists. It was deleted.",
                            }
                        )
                    shutil.move(file_path, target_path)
                successes.append(
                    {"file": f_name, "message": f"File is moved to {target_location}"}
                )

            except Exception as e:
                errors.append({"file": f_name, "message": str(e)})
    return {"successes": successes, "warnings": warnings, "errors": errors}
