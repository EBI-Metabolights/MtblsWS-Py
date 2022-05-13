from enum import Enum


class UserRole(Enum):
    SUBMITTER = 0
    CURATOR = 1
    ANONYMOUS = 2
    REVIEWER = 3  # this is not in java project
    SYSTEM_ADMIN = 4 # this is not in java project


class UserStatus(Enum):
    NEW = 0
    VERIFIED = 1
    ACTIVE = 2
    FROZEN = 3


class StudyStatus(Enum):
    SUBMITTED = 0
    INCURATION = 1
    INREVIEW = 2
    PUBLIC = 3
    DORMANT = 4


class MetabolightsException(Exception):

    def __init__(self, message: str, exception : Exception = None):
        super(MetabolightsException, self).__init__()
        self.message = message
        self.exception = exception


class MetabolightsDBException(MetabolightsException):

    def __init__(self, message: str, exception : Exception = None):
        super(MetabolightsDBException, self).__init__(message, exception)


class MetabolightsFileOperationException(MetabolightsException):

    def __init__(self, message: str, exception : Exception = None):
        super(MetabolightsFileOperationException, self).__init__(message, exception)


class MetabolightsAuthorizationException(MetabolightsException):

    def __init__(self, message: str, exception : Exception = None):
        super(MetabolightsAuthorizationException, self).__init__(message, exception)