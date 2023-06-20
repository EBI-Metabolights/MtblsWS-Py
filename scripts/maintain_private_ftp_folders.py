import os
from pathlib import Path

from app.ws.study.study_service import StudyService
from scripts.study_folder_maintenance import maintain_folders


if __name__ == "__main__":

    def sort_by_study_id(key: str):
        if key:
            val = os.path.basename(key).upper().replace("MTBLS", "")
            if val.isnumeric():
                return int(val)
        return -1

    studies = StudyService.get_instance().get_all_study_ids()
    skip_study_ids = []
    study_ids = [study[0] for study in studies if study[0] and study[0] not in skip_study_ids]

    study_ids.sort(key=sort_by_study_id)
    maintain_folders(study_id_list=study_ids, target="private-ftp", task_name="MAINTAIN_PRIVATE_FTP", output_summary_report="ftp_folder_maintenance", apply_future_actions=True)

    print("end")
