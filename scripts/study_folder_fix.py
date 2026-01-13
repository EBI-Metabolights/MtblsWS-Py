import glob
import os
import re
import sys
from typing import List

from unidecode import unidecode

from app.ws.db.schemes import Study
from app.ws.db.types import StudyStatus
from app.ws.folder_maintenance import StudyFolderMaintenanceTask
from app.ws.study.study_service import StudyService


def get_files(search_path, patterns: List[str], recursive: bool = False):
    files = []
    if not os.path.exists(search_path):
        return files
    for pattern in patterns:
        files.extend(glob.glob(os.path.join(search_path, pattern), recursive=recursive))
    files.sort()
    return files


if __name__ == "__main__":

    def sort_by_study_id(key: str):
        if key:
            val = os.path.basename(key).upper().replace("MTBLS", "")
            if val.isnumeric():
                return int(val)
        return -1

    study_ids = []
    if len(sys.argv) > 1 and sys.argv[1]:
        study_ids = sys.argv[1].split(",")

    items = set()
    # study_ids = items"MTBLS89"
    if not study_ids:
        studies = StudyService.get_instance().get_all_study_ids()
        skip_study_ids = []
        study_ids = [
            study[0] for study in studies if study[0] and study[0] not in skip_study_ids
        ]
    else:
        study_status_map = {data.name.upper(): data.value for data in StudyStatus}
        for item in study_ids:
            if item and item.upper().startswith("MTBLS"):
                items.add(item)
            elif item and item.upper() in study_status_map:
                status = StudyStatus(study_status_map[item.upper()])
                study_id_result = StudyService.get_instance().get_study_ids_with_status(
                    status
                )
                for study in study_id_result:
                    items.add(study[0])
        study_ids = list(items)
    study_ids.sort(key=sort_by_study_id, reverse=True)
    task_name = "POLARIZATION FIX"
    audit_folder_prefix = "STORAGE_MIGRATION_2023-10"
    audit_folder_suffix = "_BACKUP"
    log_folder_prefix = "STORAGE_MIGRATION_2023-10"
    log_folder_suffix = "_maintenance_log.tsv"
    alternating_set = set()
    positive_set = set()
    negative_set = set()
    for study_id in study_ids:
        audit_folder_created = False
        study: Study = StudyService.get_instance().get_study_by_acc(study_id=study_id)
        study_status = StudyStatus(study.status)
        public_release_date = study.releasedate
        submission_date = study.submissiondate
        maintenance_task = StudyFolderMaintenanceTask(
            study_id,
            study_status,
            public_release_date,
            submission_date,
            obfuscationcode=study.obfuscationcode,
            task_name=task_name,
            delete_unreferenced_metadata_files=False,
            settings=None,
            apply_future_actions=False,
            force_to_maintain=True,
            cluster_execution_mode=False,
            mhd_accession=study.mhd_accession,
            mhd_model_version=study.mhd_model_version,
            study_category=study.study_category,
            sample_template=study.sample_type,
            dataset_license=study.dataset_license,
            template_version=study.template_version,
            created_at=study.created_at,
            study_template=study.study_template,
        )
        audit_folder_path = maintenance_task.study_audit_files_path
        backup_folders = glob.glob(
            f"{audit_folder_path}/{audit_folder_prefix}*{audit_folder_suffix}"
        )
        backup_folder = None
        if backup_folders:
            backup_folder = backup_folders[0]

        internal_files_path = maintenance_task.study_internal_files_path
        log_files = glob.glob(
            f"{internal_files_path}/logs/{study_id}_{log_folder_prefix}*/*{log_folder_suffix}"
        )
        assay_files = set()
        for item in log_files:
            with open(item) as f:
                lines = f.readlines()
                for line in lines:
                    row = line.strip().split("\t")
                    if re.match(".+a_.+\.txt.+", line):
                        if "Parameter Value[Scan polarity]" in row[6]:
                            assay_files.add(row[5].strip())
        for assay_file in assay_files:
            target_file = assay_file
            target_file_base_name = os.path.basename(target_file)
            assay_updated = False
            if os.path.exists(target_file) and backup_folder:
                if not audit_folder_created:
                    audit_folder_created = True
                    maintenance_task.create_audit_folder()
                backup_file = None
                files = get_files(search_path=backup_folder, patterns=["a_*.txt"])
                for item in files:
                    basename = os.path.basename(item)
                    if basename == target_file_base_name:
                        backup_file = item
                    else:
                        updated_file_name = maintenance_task.sanitise_metadata_filename(
                            study_id, basename, prefix="a_"
                        )
                        if updated_file_name == target_file_base_name:
                            backup_file = item
                    if backup_file:
                        break
                if backup_file:
                    backup_file_pd = maintenance_task.read_tsv_file(backup_file)
                    target_file_pd = maintenance_task.read_tsv_file(target_file)
                    if len(backup_file_pd) == len(target_file_pd):
                        for index, row in target_file_pd.iterrows():
                            sample_name = backup_file_pd.iloc[index]["Sample Name"]
                            polarization: str = backup_file_pd.iloc[index][
                                "Parameter Value[Scan polarity]"
                            ]
                            target_sample_name_base = target_file_pd.iloc[index][
                                "Sample Name"
                            ]
                            target_sample_name = (
                                unidecode(target_sample_name_base)
                                .strip()
                                .replace(" ", "")
                            )
                            src_sample_name = (
                                unidecode(sample_name).strip().replace(" ", "")
                            )
                            if src_sample_name == target_sample_name:
                                polarization = backup_file_pd.iloc[index][
                                    "Parameter Value[Scan polarity]"
                                ]
                                new_value = None
                                if polarization:
                                    if "pos" in polarization.lower() and (
                                        "neg" in polarization.lower()
                                        or "nagetive" in polarization.lower()
                                    ):
                                        alternating_set.add(polarization.lower())
                                        new_value = "alternating"
                                    elif "pos" in polarization.lower():
                                        positive_set.add(polarization.lower())
                                        new_value = "positive"
                                    elif (
                                        "neg" in polarization.lower()
                                        or "nagative" in polarization.lower()
                                    ):
                                        new_value = "negative"
                                        negative_set.add(polarization.lower())
                                    elif "alt" in polarization.lower():
                                        alternating_set.add(polarization.lower())
                                        new_value = "alternating"
                                    if not new_value:
                                        print(
                                            f"{study_status.name} {study_id}, {target_file_base_name} Invalid value '{polarization}' at {index + 1} "
                                        )
                                    else:
                                        current_value = target_file_pd.iloc[index][
                                            "Parameter Value[Scan polarity]"
                                        ]
                                        if current_value != new_value:
                                            if study_status in [
                                                StudyStatus.INREVIEW,
                                                StudyStatus.PUBLIC,
                                            ]:
                                                target_file_pd.iloc[index][
                                                    "Parameter Value[Scan polarity]"
                                                ] = new_value
                                                # print(f"{study_status.name} {study_id}, {target_file_base_name} row: {index + 1}:  {current_value} -> {new_value} original: {polarization}")
                                            else:
                                                target_file_pd.iloc[index][
                                                    "Parameter Value[Scan polarity]"
                                                ] = polarization
                                                # print(f"{study_status.name} {study_id}, {target_file_base_name} row: {index + 1}:  keep old value: {polarization}")
                                            assay_updated = True
                                else:
                                    if study_status in [
                                        StudyStatus.INREVIEW,
                                        StudyStatus.PUBLIC,
                                    ]:
                                        print(
                                            f"{study_id}, {target_file_base_name} Empty value at {index + 1} "
                                        )

                            else:
                                not_sync = True
                                print(
                                    f"! {study_id}, {target_file_base_name} row: {index + 1}: Not sync  old: {sample_name} new: {target_sample_name}"
                                )
                    else:
                        not_sync = True
                        print(
                            f"! {study_id}, {target_file_base_name} row: {index + 1}: Not same column counts"
                        )
                else:
                    print(
                        f"! {study_id}, {target_file_base_name} Target file does not exist"
                    )
            if assay_updated:
                maintenance_task.write_tsv_file(target_file_pd, target_file)
    for item in alternating_set:
        print(item)
    for item in positive_set:
        print(item)
    for item in negative_set:
        print(item)
