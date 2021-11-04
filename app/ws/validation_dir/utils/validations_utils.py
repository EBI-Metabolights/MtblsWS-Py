import json
import logging
import os
import re

from flask import current_app as app
from app.ws.model_classes.validation_parameters import ValidationParams, ClusterValidationParams

logger = logging.getLogger('wslog')


class ValidationUtils:
    """
    A collection of static methods relating to the validation resources and services.
    """

    @staticmethod
    def get_study_validation_parameters(args, study_location):
        section = args['section']
        log_category = args['level']
        static_validation_file = args['static_validation_file']
        if not static_validation_file:
            static_validation_file = 'true'  # Set to same as input default value
        static_validation_file = True if static_validation_file.lower() == 'true' else False

        log_categories = "error", "warning", "info", "success", "all"
        if log_category is None or log_category not in log_categories:
            log_category = 'all'

        val_sections = "all", "isa-tab", "publication", "protocols", "people", "samples", "assays", "maf", "files"
        if section is None or section not in val_sections:
            section = 'all'

        try:
            number_of_files = sum([len(files) for r, d, files in os.walk(study_location)])
        except:
            number_of_files = 0

        validation_files_limit = app.config.get('VALIDATION_FILES_LIMIT')
        force_static_validation = False

        # We can only use the static validation file when all values are used. MOE uses 'all' as default
        if section != 'all' or log_category != 'all':
            static_validation_file = False

        if section == 'all' and log_category == 'all' and number_of_files >= validation_files_limit:
            force_static_validation = True  # ToDo, We need to use static files until pagenation is implemented
            static_validation_file = force_static_validation

        return ValidationParams(
            number_of_files, force_static_validation, validation_files_limit, log_category, section,
            static_validation_file)

    @staticmethod
    def get_cluster_validation_params(args):
        section = args['section']
        force_run = args['force_run']
        if section is None or section == "":
            section = 'meta'
        if force_run is None:
            force_run = False

        log_category = args['level']

        log_categories = "error", "warning", "info", "success", "all"
        if log_category is None or log_category not in log_categories:
            log_category = 'all'

        return ClusterValidationParams(
             log_category, section, force_run
        )

    @staticmethod
    def find_validation_report_filename(val_params, perms):
        file_name = None
        pattern = re.compile(".validation_" + val_params.section + "\S+.json")

        for filepath in os.listdir(perms.study_location):
            if pattern.match(filepath):
                file_name = filepath
                break
        return file_name

    @staticmethod
    def load_validation_file(file_name):
        try:
            with open(file_name, 'r', encoding='utf-8') as f:
                validation_schema = json.load(f)
                return validation_schema
        except Exception as e:
            logger.error(str(e))
            return {"message": "Error in reading the Validation file"}


