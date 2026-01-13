import os

from app.config import get_settings
from app.tasks.system_monitor_tasks.integrity_tests.utils import check_result


@check_result(category="paths")
def check_study_paths():
    mounted_paths = get_settings().study.mounted_paths
    paths = {}
    not_exist_folders_count = 0
    for field in mounted_paths.model_fields.keys():
        if field.endswith("_root_path") and "ftp" not in field:
            path_ = getattr(mounted_paths, field)
            if path_:
                if not os.path.exists(path_):
                    paths[field] = {"path": path_, "exists": False}
                    not_exist_folders_count += 1
                else:
                    paths[field] = {"path": path_, "exists": True}
    if not_exist_folders_count > 0:
        raise Exception(paths)

    return paths
