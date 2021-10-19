import json
import os
import logging

from ws.validation_dir.validations_utils import PermissionsObj, ValidationParams
from ws.validation import is_newer_files, update_val_schema_files, validate_study

logger = logging.getLogger('wslog')


class ValidationService:

    def __init__(self):
        pass

    def choose_validation_pathway(self, perms: PermissionsObj, validation_parameters: ValidationParams):

        if validation_parameters.section == 'all' or validation_parameters.log_category == 'all':
            validation_file = os.path.join(perms.study_location, 'validation_report.json')
            if os.path.isfile(validation_file):
                with open(validation_file, 'r', encoding='utf-8') as f:
                    validation_schema = json.load(f)
                    return validation_schema

        if (validation_parameters.static_validation_file and perms.study_status
            in ('in review', 'public')) or validation_parameters.force_static_validation:

            validation_file = os.path.join(perms.study_location, 'validation_report.json')

            # Some file in the filesystem is newer than the validation reports, so we need to re-generate
            if is_newer_files(perms.study_location):
                return update_val_schema_files(validation_file, perms.study_id, perms.study_location, perms.user_token,
                                               perms.obfuscation_code, log_category=validation_parameters.log_category,
                                               return_schema=True)

            if os.path.isfile(validation_file):
                try:
                    with open(validation_file, 'r', encoding='utf-8') as f:
                        validation_schema = json.load(f)
                except Exception as e:
                    logger.error(str(e))
                    validation_schema = \
                        update_val_schema_files(validation_file, perms.study_id, perms.study_location, perms.user_token,
                                                perms.obfuscation_code, log_category=validation_parameters.log_category,
                                                return_schema=True)

            else:
                validation_schema = \
                    update_val_schema_files(validation_file, perms.study_id, perms.study_location, perms.user_token,
                                            perms.obfuscation_code, log_category=validation_parameters.log_category,
                                            return_schema=True)

        else:
            validation_schema = \
                validate_study(perms.study_id, perms.study_location, perms.user_token, perms.obfuscation_code, validation_section=validation_parameters.section,
                               log_category=validation_parameters.log_category, static_validation_file=validation_parameters.static_validation_file)