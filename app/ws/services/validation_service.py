import json
import os
import logging

from ws.db_connection import override_validations
from ws.validation_dir.validations_utils import PermissionsObj, ValidationParams
from ws.validation import is_newer_files, update_val_schema_files, validate_study

logger = logging.getLogger('wslog')


class ValidationService:

    def __init__(self):
        pass

    def choose_validation_pathway(self, perms: PermissionsObj, validation_parameters: ValidationParams):
        """
        Choose how to handle validation based on query params and the size of the study and whether it's been validated
        before. There are five 'paths' in the logic below. At the conclusion of each, the validation schema is returned.
        1] A validation report already exists for the study. It is returned as part of the response.
        2] We want to validate the study again, but there are new files to factor in.
        3] We want to validate the study again, but we can't find the original validation report.
        4] We want to validate the study for the first time.
        5] We want to validate a large study, but due it's size we must do so from a static context.

        :param perms: PermissionsObj instance containing all the users permissions
        :param validation_parameters: ValidationParams instance containing all the parameters from the user request.
        :return: Validation schema object.
        """

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
                        return json.load(f)
                except Exception as e:
                    logger.error(str(e))
                    return  \
                        update_val_schema_files(validation_file, perms.study_id, perms.study_location, perms.user_token,
                                                perms.obfuscation_code, log_category=validation_parameters.log_category,
                                                return_schema=True)

            else:
                return \
                    update_val_schema_files(validation_file, perms.study_id, perms.study_location, perms.user_token,
                                            perms.obfuscation_code, log_category=validation_parameters.log_category,
                                            return_schema=True)

        else:
            return \
                validate_study(perms.study_id, perms.study_location, perms.user_token, perms.obfuscation_code, validation_section=validation_parameters.section,
                               log_category=validation_parameters.log_category, static_validation_file=validation_parameters.static_validation_file)


    def override(self, study_id, validation_data):
        """
        Override a specific validation rule or rules in the database for a particular study. If the rule is given as '*'
         this will override all validations and their warnings and errors. It should go without saying this wildcard
        functionality should be used sparingly and with a high degree of certainty that it is necessary.

        :param study_id: The ID of the study we want to override validations for.
        :param validation_data: The list of validations to override.
        :return: a dict containg a kv pair of result of operation: message providing more context.
        """

        val_feedback = ""
        override_list = []
        # First, get all existing validations from the database
        try:
            query_list = override_validations(study_id, 'query')
            if query_list:
                for val in query_list[0].split('|'):
                    override_list.append(val)
        except Exception as e:
            logger.error('Could not query existing overridden validations from the database')

        # only add unique validations to the update statement
        for val, val_message in validation_data[0].items():
            val_found = False
            for existing_val in override_list:
                if val + ":" in existing_val:  # Do we already have this validation rule in the database
                    val_found = True
                    val_feedback = val_feedback + "Validation '" + val + "' was already stored in the database. "

            if not val_found:
                override_list.append(val + ':' + val_message)
                val_feedback = "Validation '" + val + "' stored in the database"

        db_update_string = ""
        for existing_val in override_list:
            db_update_string = db_update_string + existing_val + '|'
        db_update_string = db_update_string[:-1]  # Remove trailing pipeline

        result = override_validations(study_id, 'update', override=db_update_string)

        if isinstance(result, tuple):
            # checking if its a tuple is sufficient as it will only ever return a tuple if it fails
            msg = 'Could not store overridden validations on the database: {0}'.format(result[1])
            logger.error(msg)
            return {"error": msg}

        return {"success": val_feedback}
