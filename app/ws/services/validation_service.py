import json
import os
import logging
import traceback

from app.ws.db_connection import override_validations
from app.ws.validation_dir.utils.validations_utils import ValidationUtils
from app.ws.validation import is_newer_files, update_val_schema_files, validate_study, submitJobToCluster, job_status, \
    is_newer_timestamp
from app.ws.model_classes.validation_parameters import ValidationParams, ClusterValidationParams
from ws.isaApiClient import IsaApiClient
from ws.model_classes.isa_wrapper import IsaApiWrapper
from ws.model_classes.permissions import PermissionsObj
from ws.utils import read_tsv

logger = logging.getLogger('wslog')


class ValidationService:
    """Validation Service - contains general logic for the validation process. The service will ultimately either
     run the validation as normal, in the background as a thread, or on an LSF cluster (except for when we're just
     overwriting validations). """

    def __init__(self, isa_api_client: IsaApiClient):
        self.validation_factory = None
        self.isa_api_client = isa_api_client
        self.isa_wrapper =


    def choose_validation_pathway(self, perms: PermissionsObj, validation_parameters: ValidationParams):
        """
        Entry method for webservice based validation.

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
                    return json.load(f)


        if (validation_parameters.static_validation_file and perms.study_status
            in ('in review', 'public')) or validation_parameters.force_static_validation:

            validation_file = os.path.join(perms.study_location, 'validation_report.json')

            # Some file in the filesystem is newer than the validation reports, so we need to re-generate
            if is_newer_files(perms.study_location):
                return self.update_val_schema_files(validation_file, perms.study_id, perms.study_location, perms.user_token,
                                               perms.obfuscation_code, log_category=validation_parameters.log_category,
                                               return_schema=True)

            if os.path.isfile(validation_file):
                try:
                    with open(validation_file, 'r', encoding='utf-8') as f:
                        return json.load(f)
                except Exception as e:
                    logger.error(str(e))
                    return  \
                        self.update_val_schema_files(validation_file, perms.study_id, perms.study_location, perms.user_token,
                                                perms.obfuscation_code, log_category=validation_parameters.log_category,
                                                return_schema=True)

            else:
                return \
                    self.update_val_schema_files(validation_file, perms.study_id, perms.study_location, perms.user_token,
                                            perms.obfuscation_code, log_category=validation_parameters.log_category,
                                            return_schema=True)

        else:
            return \
                validate_study(perms.study_id, perms.study_location, perms.user_token, perms.obfuscation_code, validation_section=validation_parameters.section,
                               log_category=validation_parameters.log_category, static_validation_file=validation_parameters.static_validation_file)


    def validate_study(self):


        pass

    def get_isa_api_client_information(self):

        try:

            if os.path.isfile(os.path.join(self._perms.study_location, 'i_Investigation.txt')):
                try:
                    isa_study, isa_inv, std_path = self.isa_api_client.get_isa_study(
                                                                     self._perms.study_id, self._perms.user_token,
                                                                     skip_load_tables=True,
                                                                     study_location=self._perms.study_location)

                    file_name = isa_study.filename
                    isa_sample_df = read_tsv(os.path.join(self._perms.study_location, file_name))
                    return IsaApiWrapper(isa_study, isa_inv, std_path, file_name, isa_sample_df,
                                         isa_study.filename, isa_study.assays, [x.filename for x in isa_study.assays])
                except FileNotFoundError:
                    logger.error("The isa study file was not found")
                except Exception as e:
                    logger.error("Could not load the minimum ISA-Tab files (generic reading error){error}".format(error=e))

            else:
                logger.error('generic file error in finding investigation file. Check file is present.')

        except ValueError:
            err = traceback.format_exc()
            logger.error("Cannot load ISA-Tab with sample and assay tables due to critical error: " + err)

        # return empty wrapper if unsuccessful. This will propagate to a validation error for the user.
        return IsaApiWrapper(None, None, None, None, None, None, None, None)

    def update_val_schema_files(self, validation_file, study_id, study_location, user_token, obfuscation_code,
                                log_category='all', return_schema=False):
        # Tidy up old files first
        if os.path.isfile(os.path.join(study_location, 'validation_files.json')):
            os.remove(os.path.join(study_location, 'validation_files.json'))

        if os.path.isfile(validation_file):
            os.remove(validation_file)

        f_start_time = time.time()
        file_list = []
        file_list = list_directories_full(study_location, file_list, base_study_location=study_location)
        try:
            with open(os.path.join(study_location, 'validation_files.json'), 'w', encoding='utf-8') as f1:
                json.dump(file_list, f1, ensure_ascii=False)
        except Exception as e1:
            logger.error('Error Writing validation file list: ' + str(e1))

        logger.info(
            study_id + " - Generating validations file list took %s seconds" % round(time.time() - f_start_time, 2))

        v_start_time = time.time()
        validation_schema = validate_study(study_id, study_location, user_token, obfuscation_code,
                                           log_category=log_category, static_validation_file=True)
        if log_category == 'all':  # Only write the complete file, not when we have a sub-section only query
            try:
                with open(validation_file, 'w', encoding='utf-8') as f:
                    # json.dump(validation_schema, f, ensure_ascii=False, indent=4)
                    json.dump(validation_schema, f, ensure_ascii=False)
            except Exception as e:
                logger.error('Error writing validation schema file: ' + str(e))
            logger.info(
                study_id + " - Generating validations list took %s seconds" % round(time.time() - v_start_time, 2))

        if return_schema:
            return validation_schema


    def override(self, study_id, validation_data):
        """
        Override a specific validation rule or rules in the database for a particular study. If the rule is given as '*'
         this will override all validations and their warnings and errors. It should go without saying this wildcard
        functionality should be used sparingly and with a high degree of certainty that it is necessary.

        :param study_id: The ID of the study we want to override validations for.
        :param validation_data: The list of validations to override.
        :return: a dict containing a kv pair of result of operation: message providing more context.
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

    def cluster(self, val_params: ClusterValidationParams, perms: PermissionsObj, script):
        """
        Either submits a job to the cluster, or returns the existing validation file. If a validation job is already
        running for the study, it will break and return a message to the user informing them that it is still running.
        We also check if there have been any updates to the study directory before submitting a new cluster job.

        :param val_params: ClusterValidationParams object containing the validation params such as section.
        :param perms: PermissionsObj
        """

        logger.info("Validation params are - " + str(val_params.log_category) + " " + str(val_params.section))
        para = ' -l {level} -i {study_id} -u {token} -s {section}'.format(
                                                                    level=val_params.log_category,
                                                                    study_id=perms.study_id,
                                                                    token=perms.user_token,
                                                                    section=val_params.section)

        file_name = ValidationUtils.find_validation_report_filename(val_params, perms)

        if file_name:
            # If we have a file_name, this means a job may already be in progress. We want to check the status of that
            # job before doing anything else.
            result = file_name[:-5].split('_')
            sub_job_id = result[2]
            # bacct -l 3861194
            status = job_status(sub_job_id)
            logger.info("job status " + sub_job_id + " " + status)

            if status == "PEND" or status == "RUN":
                return {
                    "message": "Validation is already in progress. Job " + sub_job_id + " is in running or pending state"}

            file_name = perms.study_location + "/" + file_name
            if os.path.isfile(file_name) and status == "DONE":
                if not val_params.force_run:
                    # if we don't want to force the validation to run for whatever reason, try and retrieve the existing
                    # validation schema.
                    return ValidationUtils.load_validation_file(file_name)
                else:
                    if is_newer_timestamp(perms.study_location, file_name):
                        # if the study folder has been updated, we want to delete the validation file and run a new job.
                        os.remove(file_name)
                        command = script + ' ' + para
                        return submitJobToCluster(command, val_params.section, perms.study_location)
                    else:
                        return ValidationUtils.load_validation_file(file_name)

            elif os.path.isfile(file_name) and os.path.getsize(file_name) > 0:
                if is_newer_timestamp(perms.study_location, file_name):
                    logger.info("Job status is not present but update in study directory detected, creating new job")
                    os.remove(file_name)
                    command = script + ' ' + para
                    return submitJobToCluster(command, val_params.section, perms.study_location)
                else:
                    logger.info(
                        "Job status is not present and no update detected in the study directory, returning"
                        " validation file."
                    )
                    return ValidationUtils.load_validation_file(file_name)
        else:
            try:
                os.remove(file_name)
            except Exception as e:
                pass
                # submit a new job return job id
                logger.info("No validation file present , creating new job")
                command = script + ' ' + para
                return submitJobToCluster(command, val_params.section, perms.study_location)
