from enum import Enum


class UserRole(Enum):
    SUBMITTER = 0
    CURATOR = 1
    ANONYMOUS = 2
    REVIEWER = 3
    SYSTEM_ADMIN = 4


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
