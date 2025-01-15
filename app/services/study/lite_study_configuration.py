from pathlib import Path
import re
from typing import Union

from app.config import ApplicationSettings, get_settings
from app.services.study.model import StudyManagedFiles, StudyManagedFolders, StudyPaths
from app.utils import MetabolightsException
from app.ws.study import identifier_service


class LiteStudyConfiguration(object):
    def __init__(
        self,
        study_id: str,
        obfuscation_code: Union[None, str] = None,
        cluster_mode: bool = False,
        study_folder_relative_path: Union[None, str] = None,
        study_ftp_folder_relative_path: Union[None, str] = None,
        settings: Union[None, ApplicationSettings] = None,
    ) -> None:
        self.settings = settings
        if not self.settings:
            self.settings = get_settings()
        self.obfuscation_code = obfuscation_code
        self.study_id = study_id
        self.cluster_mode = cluster_mode
        valid = identifier_service.default_mtbls_identifier.validate_format(
            self.study_id
        )
        if not valid:
            if not identifier_service.default_submission_identifier.validate_format(
                self.study_id
            ):
                raise MetabolightsException(
                    message=f"Invalid study id '{self.study_id}'"
                )

        self._paths: StudyPaths = StudyPaths()
        self._managed_folders = StudyManagedFolders()
        self._managed_files = StudyManagedFiles()
        self.study_folder_relative_path = study_folder_relative_path
        self.study_ftp_folder_relative_path = study_ftp_folder_relative_path
        self.recalculate_paths()

    @property
    def paths(self):
        return self._paths

    @property
    def managed_files(self):
        return self._managed_files

    @property
    def managed_folders(self):
        return self._managed_folders

    def recalculate_managed_folders(self):
        folders = self.managed_folders
        paths = self.paths

        folders.rw_audit_folder = self.get_path(
            paths.rw_audit_files, self.settings.study.audit_folder_name
        )
        folders.rw_logs_folder = self.get_path(
            paths.rw_internal_files, self.settings.study.internal_logs_folder_name
        )
        folders.rw_temp_folder = self.get_path(
            paths.rw_internal_files, self.settings.study.internal_temp_folder_name
        )
        folders.chebi_pipeline_annotations_folder = self.get_path(
            paths.rw_internal_files,
            self.settings.chebi.pipeline.chebi_annotation_sub_folder,
        )
        folders.rw_integrity_check_folder = paths.rw_internal_files

        folders.audit_files_link = self.get_path(
            paths.rw_metadata_files, self.settings.study.audit_files_symbolic_link_name
        )
        folders.files_link = self.get_path(
            paths.rw_metadata_files,
            self.settings.study.readonly_files_symbolic_link_name,
        )
        folders.internal_files_link = self.get_path(
            paths.rw_metadata_files,
            self.settings.study.internal_files_symbolic_link_name,
        )

        folders.readonly_audit_folder = self.get_path(
            paths.readonly_audit_files, self.settings.study.audit_folder_name
        )
        folders.rw_study_recycle_bin_folder = self.get_path(
            paths.rw_audit_files, self.settings.study.internal_backup_folder_name
        )
        folders.readonly_study_recycle_bin_folder = self.get_path(
            paths.readonly_audit_files, self.settings.study.internal_backup_folder_name
        )
        study_folder = Path(self.study_id)

        if self.cluster_mode:
            folders.public_ftp_recycle_bin_folder = self.get_path(
                self.settings.hpc_cluster.datamover.mounted_paths.cluster_public_ftp_recycle_bin_root_path,
                study_folder,
            )
            folders.private_ftp_study_recycle_bin_folder = self.get_path(
                self.settings.hpc_cluster.datamover.mounted_paths.cluster_private_ftp_recycle_bin_root_path,
                self.study_ftp_folder_relative_path,
            )
        else:
            folders.public_ftp_recycle_bin_folder = self.get_path(
                self.settings.study.mounted_paths.public_ftp_recycle_bin_root_path,
                study_folder,
            )
            folders.private_ftp_study_recycle_bin_folder = self.get_path(
                self.settings.study.mounted_paths.private_ftp_recycle_bin_root_path,
                self.study_ftp_folder_relative_path,
            )
        self.recalculate_managed_files()

    def recalculate_managed_files(self):
        validation_report_file_name = self.settings.study.validation_report_file_name
        self.managed_files.validation_report = self.get_path(
            self.paths.rw_internal_files, validation_report_file_name
        )

    def recalculate_paths(self):
        self.study_folder_relative_path = (
            Path(self.study_folder_relative_path)
            if self.study_folder_relative_path
            else Path(self.study_id)
        )
        study_folder = self.study_folder_relative_path
        paths = self.paths
        if self.cluster_mode:
            mounted_paths = self.settings.hpc_cluster.datamover.mounted_paths

            paths.rw_metadata_files = self.get_path(
                mounted_paths.cluster_study_metadata_files_root_path, study_folder
            )
            paths.rw_internal_files = self.get_path(
                mounted_paths.cluster_study_internal_files_root_path, study_folder
            )
            paths.rw_audit_files = self.get_path(
                mounted_paths.cluster_study_audit_files_root_path, study_folder
            )

            paths.readonly_metadata_files = self.get_path(
                mounted_paths.cluster_study_readonly_metadata_files_root_path,
                study_folder,
            )
            paths.readonly_audit_files = self.get_path(
                mounted_paths.cluster_study_readonly_audit_files_root_path, study_folder
            )
            paths.actual_readonly_audit_files = self.get_path(
                mounted_paths.cluster_study_readonly_audit_files_actual_root_path,
                study_folder,
            )
            paths.actual_readonly_files = self.get_path(
                mounted_paths.cluster_study_readonly_files_actual_root_path,
                study_folder,
            )
            paths.readonly_integrity_check_files = self.get_path(
                mounted_paths.cluster_study_readonly_integrity_check_files_root_path,
                study_folder,
            )
            paths.readonly_public_metadata_versions = self.get_path(
                mounted_paths.cluster_study_readonly_public_metadata_versions_root_path,
                study_folder,
            )
            paths.public_ftp_files = self.get_path(
                mounted_paths.cluster_public_ftp_root_path, study_folder
            )
        else:
            mounted_paths = self.settings.study.mounted_paths

            paths.rw_metadata_files = self.get_path(
                mounted_paths.study_metadata_files_root_path, study_folder
            )
            paths.rw_internal_files = self.get_path(
                mounted_paths.study_internal_files_root_path, study_folder
            )
            paths.rw_audit_files = self.get_path(
                mounted_paths.study_audit_files_root_path, study_folder
            )

            paths.readonly_metadata_files = self.get_path(
                mounted_paths.study_readonly_metadata_files_root_path, study_folder
            )
            paths.readonly_audit_files = self.get_path(
                mounted_paths.study_readonly_audit_files_root_path, study_folder
            )
            paths.actual_readonly_audit_files = self.get_path(
                mounted_paths.study_readonly_audit_files_actual_root_path, study_folder
            )
            paths.actual_readonly_files = self.get_path(
                mounted_paths.study_readonly_files_actual_root_path, study_folder
            )

            paths.readonly_integrity_check_files = self.get_path(
                mounted_paths.study_readonly_integrity_check_files_root_path,
                study_folder,
            )
            paths.readonly_public_metadata_versions = self.get_path(
                mounted_paths.study_readonly_public_metadata_versions_root_path,
                study_folder,
            )
            paths.public_ftp_files = self.get_path(
                mounted_paths.public_ftp_root_path, study_folder
            )

        self.recalculate_private_ftp_paths()
        self.recalculate_managed_folders()

    def recalculate_private_ftp_paths(self):
        if not self.obfuscation_code and not self.study_ftp_folder_relative_path:
            self.paths.private_ftp_files = None
            return

        ftp_folder = Path(f"{self.study_id.lower()}-{self.obfuscation_code}")
        self.study_ftp_folder_relative_path = (
            self.study_ftp_folder_relative_path
            if self.study_ftp_folder_relative_path
            else ftp_folder
        )
        if self.cluster_mode:
            mounted_paths = self.settings.hpc_cluster.datamover.mounted_paths
            self.paths.private_ftp_files = self.get_path(
                mounted_paths.cluster_private_ftp_root_path, ftp_folder
            )

        else:
            mounted_paths = self.settings.study.mounted_paths
            self.paths.private_ftp_files = self.get_path(
                mounted_paths.private_ftp_root_path, ftp_folder
            )

    def get_path(self, root_path: Union[str, Path], relative_path: Union[str, Path]):
        if not root_path or not relative_path:
            return None

        relative_path = (
            relative_path if isinstance(relative_path, Path) else Path(relative_path)
        )
        root_path = root_path if isinstance(root_path, Path) else Path(root_path)

        return root_path / relative_path


if __name__ == "__main__":
    study_configuration = LiteStudyConfiguration(study_id="MTBLS1")

    print(study_configuration.study_id)
