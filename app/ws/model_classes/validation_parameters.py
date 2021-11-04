from abc import ABC


class AbstractValParams(ABC):
    """Different validation contexts require varying amounts of information. Rather than make one bloated class that
    had a bunch of optional/default values, we instead have a few that inherit common attributes from a base class."""

    def __init__(self, log_category, section):
        self.log_category = log_category
        self.section = section


class ValidationParams(AbstractValParams):

    def __init__(self, number_of_files, force_static_validation, validation_files_limit, log_category, section,
                 static_validation_file):
        super().__init__(log_category, section)
        self.number_of_files = number_of_files
        self.force_static_validation = force_static_validation
        self.validation_files_limit = validation_files_limit
        self.static_validation_file = static_validation_file


class ClusterValidationParams(AbstractValParams):

    def __init__(self, log_category, section, force_run):
        super().__init__(log_category, section)
        self.force_run = force_run
