#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Jan-09
#  Modified by:   kenneth
#
#  Copyright 2020 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import glob
import logging
import os
import shutil
import time

from isatools import model

from app.config import get_settings
from app.tasks.datamover_tasks.basic_tasks.study_folder_maintenance import (
    rename_folder_on_private_storage,
)
from app.utils import (
    MetabolightsException,
)
from app.ws.db import schemes, types
from app.ws.db_connection import (
    reserve_mtbls_accession,
    update_study_id_from_mtbls_accession,
    update_study_status,
)
from app.ws.folder_maintenance import StudyFolderMaintenanceTask
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient

logger = logging.getLogger("wslog")

wsc = WsClient()
iac = IsaApiClient()


class StudyStatusHelper:
    @staticmethod
    def refactor_study_folder(
        study: schemes.Study,
        study_location: str,
        user_token,
        study_id: str,
        updated_study_id: str,
    ) -> model.Study:
        if study_id == updated_study_id:
            return
        task_name = "ASSIGN_ACCESSION_NUMBER"
        maintenance_task = StudyFolderMaintenanceTask(
            updated_study_id,
            types.StudyStatus(study.status),
            study.releasedate,
            study.submissiondate,
            task_name=task_name,
            obfuscationcode=study.obfuscationcode,
            delete_unreferenced_metadata_files=False,
            settings=get_settings().study,
            apply_future_actions=True,
        )
        date_format = "%Y-%m-%d_%H-%M-%S"
        folder_name = time.strftime(date_format) + "_" + task_name
        maintenance_task.create_audit_folder(folder_name=folder_name, stage=None)

        isa_study_item, isa_inv, _ = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )
        # update investigation file
        isa_inv.identifier = updated_study_id
        if isa_inv:
            isa_study: model.Study = isa_study_item
            isa_study.identifier = updated_study_id
            isa_inv.identifier = updated_study_id
            study_filename: str = isa_study.filename
            isa_study.filename = study_filename.replace(study_id, updated_study_id, 1)
            for assay_item in isa_study.assays:
                assay: model.Assay = assay_item
                assay.filename = assay.filename.replace(study_id, updated_study_id, 1)
            iac.write_isa_study(
                isa_inv,
                user_token,
                study_location,
                save_investigation_copy=False,
                save_assays_copy=False,
                save_samples_copy=False,
            )
        else:
            logger.error(
                f"i_Investigation.txt file on {study_location} does not exist."
            )

        # update assay file (maf file references) and rename all metadata files
        metadata_files_result = glob.iglob(os.path.join(study_location, "?_*.t??"))
        metadata_files = [x for x in metadata_files_result]
        for metadata_file in metadata_files:
            base_name = os.path.basename(metadata_file)
            if base_name.startswith("a_"):
                assay_df = maintenance_task.read_tsv_file(metadata_file)
                for column in assay_df.columns:
                    if "Metabolite Assignment File" in column:
                        assay_df[column] = assay_df[column].apply(
                            lambda x: x.replace(study_id, updated_study_id, 1)
                            if x
                            else ""
                        )
                        maintenance_task.write_tsv_file(assay_df, metadata_file)
            new_name = os.path.basename(metadata_file).replace(
                study_id, updated_study_id, 1
            )
            target_metadata_path = os.path.join(
                os.path.dirname(metadata_file), new_name
            )
            if metadata_file != target_metadata_path:
                shutil.move(metadata_file, target_metadata_path)

        # create symbolic links on rw storage
        mounted_paths = get_settings().study.mounted_paths
        managed_paths = [
            mounted_paths.study_metadata_files_root_path,
            mounted_paths.study_audit_files_root_path,
            mounted_paths.study_internal_files_root_path,
        ]

        for root_path in managed_paths:
            new_path = os.path.join(root_path, updated_study_id)
            current_path = os.path.join(root_path, study_id)
            shutil.move(current_path, new_path)

        maintenance_task.maintain_rw_storage_folders()
        # if not os.path.exists(new_path):
        #     maintenance_task.maintain_study_symlinks(current_path, new_path)

        # create symbolic links on services storage
        # inputs = {"updated_study_id": updated_study_id, "study_id": study_id}
        # create_links_on_data_storage.apply_async(kwargs=inputs)

        # create symbolic links on private ftp storage
        inputs = {
            "updated_study_id": updated_study_id,
            "study_id": study_id,
            "obfuscation_code": study.obfuscationcode,
        }
        rename_folder_on_private_storage.apply_async(kwargs=inputs)
        return isa_study

    @staticmethod
    def update_db_study_id(
        current_study_id: str,
        current_study_status: types.StudyStatus,
        requested_study_status: types.StudyStatus,
        reserved_accession: str,
    ):
        mtbls_accession_states = (
            types.StudyStatus.PRIVATE,
            types.StudyStatus.INREVIEW,
            types.StudyStatus.PUBLIC,
        )
        provisional_id_states = (
            types.StudyStatus.PROVISIONAL,
            types.StudyStatus.DORMANT,
        )
        mtbls_prefix = get_settings().study.accession_number_prefix
        target_study_id = current_study_id
        if (
            requested_study_status in mtbls_accession_states
            and current_study_status in provisional_id_states
            and not current_study_id.startswith(mtbls_prefix)
        ):
            if not reserved_accession:
                reserve_mtbls_accession(current_study_id)
            target_study_id = update_study_id_from_mtbls_accession(current_study_id)
            if not target_study_id:
                raise MetabolightsException(
                    http_code=403,
                    message=f"Error while assigning MetaboLights accession number for {current_study_id}",
                )
        if not target_study_id:
            raise MetabolightsException(message="Could not update the study id")
        return target_study_id

    @staticmethod
    def update_status(
        study_id,
        study_status,
        first_public_date=None,
        first_private_date=None,
    ):
        study_status = study_status.lower()
        # Update database
        return update_study_status(
            study_id,
            study_status,
            first_public_date=first_public_date,
            first_private_date=first_private_date,
        )
