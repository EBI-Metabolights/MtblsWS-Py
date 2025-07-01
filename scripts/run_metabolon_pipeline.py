
import os
import pathlib
from app.config import get_settings
from app.config.model.hpc_cluster import DataMoverPathConfiguration
from app.config.model.study import StudySettings
from app.tasks.datamover_tasks.curation_tasks.metabolon import metabolon_confirm
from app.ws.db import schemes
from app.ws.study.study_service import StudyService


if __name__ == '__main__':
    study_id = "REQ20250409209843"
    mounted_paths: DataMoverPathConfiguration = get_settings().hpc_cluster.datamover.mounted_paths
    
    study: schemes.Study = StudyService.get_instance().get_study_by_acc(study_id)
    private_ftp_root_path = mounted_paths.cluster_private_ftp_root_path
    study_location = os.path.join(
        private_ftp_root_path, f"{study.acc.lower()}-{study.obfuscationcode}"
    )
    
    study_root_path = pathlib.Path(mounted_paths.cluster_study_metadata_files_root_path)
    target_root_path = pathlib.Path(mounted_paths.cluster_study_internal_files_root_path)
    target_location = target_root_path / study_id / pathlib.Path("metabolon_pipeline")
    email = "ozgury@ebi.ac.uk"
    # target_location = study_root_path / study_id
    # convert_to_isa(study_location=str(study_location), study_id=study_id)
    # create_isa_files(study_id=study_id, study_location=str(study_location), target_location=target_location)
    # target_location = target_root_path / study_id / "metabolon_pipeline/MZML_0015"
    # status, message = to_isa_tab("", str(target_location / "FILES"), str(target_location))
    # status, report = validate_mzml_files(study_id)
    metabolon_confirm(study_id=study_id, study_location=study_location, target_location=target_location, email=email)
    # print(status)
