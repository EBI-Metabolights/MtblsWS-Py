import glob
import io
import logging
import os

from app.ws.validation_dir.validators.abstract_validator import AbstractValidator
from app.ws.model_classes.enums.individual_validation_status import IndividualValidationStatus


logger = logging.getLogger('wslog')


class BasicIsatabValidator(AbstractValidator):

    def validate(self):
        validates = True
        amber_warning = False
        validations = []
        val_section = "basic"

        # If the ISA API could not find or read the investigation file, tell the user. The validation will then skip
        # to the return.
        if not self._isa_wrapper.is_complete():
            self.add_msg(
                validations, "ISA-Tab", "Could not find or read the investigation file",
                IndividualValidationStatus.error, 'i_Investigation.txt', val_sequence=18,
                log_category=self._val_params.log_category)

        if self._isa_wrapper.isa_inv:
            self.add_msg(validations, val_section, "Successfully read the investigation file", IndividualValidationStatus.success,
                    'i_Investigation.txt', val_sequence=2, log_category=self._val_params.log_category)

            study_num = 0
            if self._isa_wrapper.isa_inv.studies:
                study_num = len(self._isa_wrapper.isa_inv.studies)
                if study_num > 1:
                    self.add_msg(validations, val_section,
                            "You can only submit one study per submission, this submission has " + str(
                                study_num) + " studies",
                            IndividualValidationStatus.error, 'i_Investigation.txt', val_sequence=2.1, log_category=self._val_params.log_category)

            if self._isa_wrapper.isa_study and study_num == 1:
                self.add_msg(validations, val_section, "Successfully read the study section of the investigation file",
                        IndividualValidationStatus.success,
                        'i_Investigation.txt', val_sequence=3, log_category=self._val_params.log_category)
            else:
                self.add_msg(validations, val_section,
                        "Could not correctly read the study section of the investigation file",
                        IndividualValidationStatus.error,
                        'i_Investigation.txt', val_sequence=4, log_category=self._val_params.log_category)
                validates = False

            if self._isa_wrapper.file_name:
                self.add_msg(validations, val_section, "Successfully found the reference to the sample sheet filename",
                        IndividualValidationStatus.success,
                        'i_Investigation.txt', val_sequence=5, log_category=self._val_params.log_category)
            else:
                self.add_msg(validations, val_section, "Could not find the reference to the sample sheet filename", IndividualValidationStatus.error,
                        self._isa_wrapper.file_name, val_sequence=6, log_category=self._val_params.log_category)
                validates = False

            # isaconfig
            if self._isa_wrapper.isa_inv.get_comment('Created With Configuration'):
                create_config = self._isa_wrapper.isa_inv.get_comment('Created With Configuration')
                open_config = None
                if self._isa_wrapper.isa_inv.get_comment('Last Opened With Configuration'):
                    open_config = self._isa_wrapper.isa_inv.get_comment('Last Opened With Configuration')

                if 'isaconfig' in create_config.value:
                    self.add_msg(validations, val_section, "Incorrect configuration files used to create the study ("
                            + create_config.value + "). The study may not contain required fields",
                            IndividualValidationStatus.warning, self._isa_wrapper.file_name, val_sequence=7, log_category=self._val_params.log_category)
                    amber_warning = True
                if 'isaconfig' in open_config.value:
                    self.add_msg(validations, val_section, "Incorrect configuration files used to edit the study ("
                            + open_config.value + "). The study may not contain required fields",
                            IndividualValidationStatus.warning, self._isa_wrapper.file_name, val_sequence=8, log_category=self._val_params.log_category)
                    amber_warning = True

            if validates:  # Have to have a basic investigation and sample file before we can continue
                if self._isa_wrapper.isa_study.samples:
                    self.add_msg(validations, val_section, "Successfully found one or more samples", IndividualValidationStatus.success, self._isa_wrapper.file_name,
                            val_sequence=9, log_category=self._val_params.log_category)
                elif len(self._isa_wrapper.isa_sample_df) != 0 and not self._isa_wrapper.isa_sample_df.empty:
                    self.add_msg(validations, val_section, "Successfully found one or more samples", IndividualValidationStatus.success, self._isa_wrapper.file_name,
                            val_sequence=10, log_category=self._val_params.log_category)
                else:
                    self.add_msg(validations, val_section, "Could not find any samples",
                                 IndividualValidationStatus.error, self._isa_wrapper.file_name, val_sequence=11, log_category=self._val_params.log_category)

                if self._isa_wrapper.assays:
                    self.add_msg(validations, val_section, "Successfully found one or more assays", IndividualValidationStatus.success, self._isa_wrapper.file_name,
                            val_sequence=12, log_category=self._val_params.log_category)
                else:
                    self.add_msg(validations, val_section, "Could not find any assays",
                            IndividualValidationStatus.error, self._isa_wrapper.file_name, val_sequence=13, log_category=self._val_params.log_category)

                if self._isa_wrapper.isa_study.factors:
                    self.add_msg(validations, val_section, "Successfully found one or more factors", IndividualValidationStatus.success, self._isa_wrapper.file_name,
                            val_sequence=14, log_category=self._val_params.log_category)
                else:
                    self.add_msg(validations, val_section, "Could not find any factors",
                            IndividualValidationStatus.warning, self._isa_wrapper.file_name, val_sequence=15, log_category=self._val_params.log_category)

                if self._isa_wrapper.isa_study.design_descriptors:
                    self.add_msg(validations, val_section, "Successfully found one or more descriptors", IndividualValidationStatus.success, self._isa_wrapper.file_name,
                            val_sequence=16, log_category=self._val_params.log_category)
                else:
                    self.add_msg(validations, val_section, "Could not find any study design descriptors",
                            IndividualValidationStatus.error, self._isa_wrapper.file_name, val_sequence=17, log_category=self._val_params.log_category)

                if self.find_text_in_isatab_file(self._perms.study_location, 'Thesaurus.owl#'):
                    # The hash in an ontology URL will cause problems for the ISA-API
                    self.add_msg(validations, val_section,
                            "URL's containing # will not load properly, please change to '%23'",
                            IndividualValidationStatus.warning, 'i_Investigation.txt', val_sequence=17.1, log_category=self._val_params.log_category)

                if self._isa_wrapper.isa_study.public_release_date:
                    public_release_date = self._isa_wrapper.isa_study.public_release_date
                    if public_release_date != self._perms.release_date:
                        self.add_msg(validations, val_section,
                                "The public release date in the investigation file " +
                                public_release_date + " is not the same as the database release date " +
                                self._perms.release_date, IndividualValidationStatus.warning, self._isa_wrapper.file_name, val_sequence=19.2, log_category=self._val_params.log_category)
                else:
                    self.add_msg(validations, val_section,
                            "Could not find the public release date in the investigation file",
                            IndividualValidationStatus.warning, self._isa_wrapper.file_name, val_sequence=19.1, log_category=self._val_params.log_category)


        return self.return_validations(val_section, validations,)


    @staticmethod
    def find_text_in_isatab_file(study_folder, text_to_find):
        found = False
        isa_tab_file = os.path.join(study_folder, 'i_*.txt')
        for ref_file in glob.glob(isa_tab_file):
            try:
                logger.info("Checking if text " + text_to_find + " is referenced in " + ref_file)
                if text_to_find in io.open(ref_file, 'r', encoding='utf8', errors="ignore").read():
                    found = True
            except Exception as e:
                logger.error('File Format error? Cannot read or open file ' + ref_file)
                logger.error(str(e))

        return found
