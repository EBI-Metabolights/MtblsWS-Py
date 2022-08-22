from enum import Enum


class UserRole(Enum):
    ROLE_SUBMITTER = 0
    ROLE_SUPER_USER = 1
    ANONYMOUS = 2
    REVIEWER = 3  # this is not in java project
    SYSTEM_ADMIN = 4  # this is not in java project


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


class StudyTaskStatus(str, Enum):
    NOT_EXECUTED = 'NOT_EXECUTED'
    EXECUTING = 'EXECUTING'
    EXECUTION_SUCCESSFUL = 'EXECUTION_SUCCESSFUL'
    EXECUTION_FAILED = 'EXECUTION_FAILED'


class StudyTaskName(str, Enum):
    REINDEX = 'REINDEX'
    SEND_TWEET = 'SEND_TWEET'
