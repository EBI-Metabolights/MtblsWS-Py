import io
import os
import pathlib
import shutil
import sys
import uuid
import zipfile
from typing import Dict, List, Union

from app.config.model.study import StudySettings
from app.study_folder_utils import FileDescriptor, get_study_folder_files
from app.tasks.worker import get_flask_app
from app.ws.folder_maintenance import MaintenanceException
from app.ws.settings.utils import get_study_settings
from app.ws.study.study_service import StudyService


def delete_test_file(study_id, file_path, item: FileDescriptor):
    if not os.path.exists(file_path):
        return
    print(
        f"{study_id}\tREMOVE\t{'FOLDER' if item.is_dir else 'FILE'}\t{item.extension}\t{item.relative_path}"
    )

    if item.is_dir:
        shutil.rmtree(file_path)
    else:
        os.remove(file_path)


fixed_content = uuid.uuid4()


def create_dummy_file(study_id, file_path, item: FileDescriptor, unique_content=True):
    if os.path.exists(file_path):
        return
    with open(file_path, "w") as f:
        if unique_content:
            dummy_content = str(uuid.uuid4())
        else:
            dummy_content = fixed_content
        f.writelines([f"{dummy_content}\n"])
    os.utime(file_path, (item.modified_time, item.modified_time))

    print(
        f"{study_id}\tCREATE\t{'FOLDER' if item.is_dir else 'FILE'}\t{item.extension}\t{item.relative_path}"
    )


def create_dummy_zip_file(
    study_id, file_path, item: FileDescriptor, unique_content=True
):
    if os.path.exists(file_path):
        return
    basename = os.path.basename(file_path)
    new_name, _ = os.path.splitext(basename)
    with open(file_path, "wb") as f:
        if unique_content:
            dummy_content = str(uuid.uuid4())
        else:
            dummy_content = fixed_content
        b = io.BytesIO()
        zf = zipfile.ZipFile(b, mode="w")
        zf.writestr(new_name, dummy_content)
        zf.close()
        f.write(b.getbuffer())
    os.utime(file_path, (item.modified_time, item.modified_time))
    print(f"{study_id}\tCREATE\tFILE\t{item.extension}\t{item.relative_path}")


def create_test_file(study_id, study_test_path: str, item: FileDescriptor):
    item_path = os.path.join(study_test_path, item.relative_path)

    if item.is_dir:
        if item.is_stop_folder and item.sub_filename:
            if os.path.exists(item_path):
                items = os.listdir(item_path)
                if not items:
                    os.utime(item_path, (item.modified_time, item.modified_time))
                    sub_item = FileDescriptor(
                        relative_path=f"{item.relative_path}/{item.sub_filename}",
                        is_dir=False,
                        modified_time=item.modified_time,
                        extension="",
                    )
                    create_dummy_file(
                        study_id,
                        os.path.join(study_test_path, sub_item.relative_path),
                        sub_item,
                    )
                    return
                else:
                    if len(items) == 1 and items[0] == item.sub_filename:
                        os.utime(item_path, (item.modified_time, item.modified_time))
                        os.utime(
                            os.path.join(item_path, item.sub_filename),
                            (item.modified_time, item.modified_time),
                        )
                        return
                    else:
                        shutil.rmtree(item_path, ignore_errors=True)

            os.makedirs(item_path, exist_ok=True)
            os.utime(item_path, (item.modified_time, item.modified_time))
            sub_item = FileDescriptor(
                relative_path=f"{item.relative_path}/{item.sub_filename}",
                is_dir=False,
                modified_time=item.modified_time,
                extension="",
            )
            create_dummy_file(
                study_id,
                os.path.join(study_test_path, sub_item.relative_path),
                sub_item,
            )
        else:
            os.makedirs(item_path, exist_ok=True)
            os.utime(item_path, (item.modified_time, item.modified_time))
    else:
        if item.extension.lower() == ".zip":
            create_dummy_zip_file(study_id, item_path, item)
        else:
            create_dummy_file(study_id, item_path, item)


def create_study_test_data_files(
    study_id_list: List[str],
    source_path=None,
    target_path: Union[None, str] = None,
    flask_app=None,
    settings: Union[None, StudySettings] = None,
):
    if not study_id_list:
        raise MaintenanceException(message="At least one study should be selected")
    if not source_path or not os.path.exists(source_path):
        raise MaintenanceException(message="source_path should be defined")
    if not target_path or not os.path.exists(target_path):
        raise MaintenanceException(message="target_path should be defined")
    if not settings:
        settings = get_study_settings()
    if not flask_app:
        flask_app = get_flask_app()

    for study_id in study_id_list:
        study_source_path = os.path.join(source_path, study_id)
        study_test_path = os.path.join(target_path, study_id)

        source_file_descriptors: Dict[str, FileDescriptor] = {}
        target_file_descriptors: Dict[str, FileDescriptor] = {}

        study_source_path_item = pathlib.Path(study_source_path)
        if os.path.exists(study_source_path):
            source_folders_iter = get_study_folder_files(
                study_source_path, source_file_descriptors, study_source_path_item
            )
            folders = [x for x in source_folders_iter]
        if not os.path.exists(study_test_path):
            os.makedirs(study_test_path, exist_ok=True)
        target_folders_iter = get_study_folder_files(
            study_source_path,
            target_file_descriptors,
            study_source_path_item,
            list_all_files=True,
        )
        folders = [x for x in target_folders_iter]

        extra_files: List[FileDescriptor] = []
        for key in target_file_descriptors:
            if key not in source_file_descriptors:
                extra_files.append(target_file_descriptors[key])

        for key in source_file_descriptors:
            item: FileDescriptor = source_file_descriptors[key]
            create_test_file(study_id, study_test_path, item)

        for descriptor in extra_files:
            file_path = os.path.join(study_test_path, descriptor.relative_path)
            delete_test_file(study_id, file_path, descriptor)

        print(f"{study_id} is completed")


if __name__ == "__main__":
    flask_app = get_flask_app()

    def sort_by_study_id(key: str):
        if key:
            val = os.path.basename(key).upper().replace("MTBLS", "")
            if val.isnumeric():
                return int(val)
        return -1

    source_path = []
    if len(sys.argv) > 1 and sys.argv[1]:
        source_path = sys.argv[1]

    target_path = None
    if len(sys.argv) > 2 and sys.argv[2]:
        target_path = sys.argv[2]

    with flask_app.app_context():
        studies = StudyService.get_instance().get_all_study_ids()
        skip_study_ids = [f"MTBLS{(i + 1)}" for i in range(2324)]
        # skip_study_ids = []
        study_ids = [
            study[0] for study in studies if study[0] and study[0] not in skip_study_ids
        ]
        # study_ids = ["MTBLS1391", "MTBLS1684", "MTBLS2413"]
        study_ids.sort(key=sort_by_study_id)
        results = create_study_test_data_files(
            study_ids, source_path, target_path, flask_app=flask_app
        )
    print("end")
