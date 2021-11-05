import logging
from abc import abstractmethod, ABC

from app.ws.model_classes.enums.individual_validation_status import IndividualValidationStatus
from app.ws.model_classes.isa_wrapper import IsaApiWrapper
from app.ws.model_classes.permissions import PermissionsObj
from app.ws.model_classes.validation_parameters import AbstractValParams

logger = logging.getLogger('wslog')


class AbstractValidator(ABC):

    # each validating method returns a status, an amber warning and a validation list for that section.
    # The validation list for that section is then added to the bigger all_validations list.

    def __init__(self, perms: PermissionsObj, val_params: AbstractValParams, isa_wrapper: IsaApiWrapper):
        """
        Init method
        :param perms: PermissionsObj instance, which has all the permissions belonging to a user.
        :param val_params: A Subclass instance inheriting from AbstractValParams containing the validation parameters
        such as section, number of files, static_validation.
        :param isa_wrapper: IsaApiWrapper instance containing all the information returned from the IsaApiClient about a
        particular study.
        """
        self._perms = perms
        self._val_params = val_params
        self._isa_wrapper = isa_wrapper

    @abstractmethod
    def validate(self):
        """
        This method should be the entrypoint for any validator, and should be home to the central logic / ruleset for
        that section. This is to make adding new rules in the future easier to do.
        """
        raise NotImplementedError

    def add_msg(self, validations, section, message, status, meta_file="", value="", descr="", val_sequence=0,
                log_category=IndividualValidationStatus.error):
        if log_category == status or log_category == 'all':
            validations.append(
                {"message": message, "section": section, "val_sequence": str(val_sequence), "status": status,
                 "metadata_file": meta_file, "value": value, "description": descr})

    def return_validations(self, section, validations, override_list=[]):
        # Add the validation sequence
        for val in validations:
            # idx += 1  # Set the sequence to 1, as this is the section we will override
            val_sequence = section + '_' + val['val_sequence']
            val["val_sequence"] = val_sequence
            val["val_override"] = 'false'
            val["val_message"] = ''
            if len(override_list) > 0:  # These are from the database, ie. already over-ridden
                try:
                    for db_val in override_list:
                        val_step = db_val.split(':')[0]
                        val_msg = db_val.split(':')[1]
                        if val_sequence == val_step or val_step == '*':  # "*" overrides all errors/warning/info etc
                            val_status = val['status']
                            val["val_override"] = 'true'
                            val["val_message"] = val_msg
                            if val_status == IndividualValidationStatus.warning or val_status == IndividualValidationStatus.error or val_status == IndividualValidationStatus.info:
                                val["status"] = IndividualValidationStatus.success
                            elif val_status == IndividualValidationStatus.success and val_step != '*':
                                val["status"] = IndividualValidationStatus.error
                except Exception as e:
                    logger.error('Could not read the validation override list, is the required ":" there? {error}'
                                 .format(error=e))

        error_found = False
        warning_found = False
        validates = True
        amber_warning = False

        # What is the overall validation status now?
        for val in validations:
            status = val["status"]
            if status == IndividualValidationStatus.error:
                error_found = True
            elif status == IndividualValidationStatus.warning:
                warning_found = True

        if error_found:
            validates = False
            ret_list = {"section": section, "details": validations, "message": "Validation failed",
                        "status": IndividualValidationStatus.error}
        elif warning_found:
            amber_warning = True
            ret_list = {"section": section, "details": validations,
                        "message": "Some optional information is missing for your study",
                        "status": IndividualValidationStatus.warning}
        else:
            ret_list = {"section": section, "details": validations, "message": "Successful validation",
                        "status": IndividualValidationStatus.success}

        return validates, amber_warning, ret_list

    @classmethod
    def exchange_data(cls):
        """pass data between one validator to another"""
        pass
