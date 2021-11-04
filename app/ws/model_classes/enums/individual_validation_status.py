from enum import Enum


class IndividualValidationStatus(Enum):
    warning = "warning"
    error = "error"
    success = "success"
    info = "info"
