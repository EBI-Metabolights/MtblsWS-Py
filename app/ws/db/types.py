from enum import Enum


class UserRole(Enum):
    ROLE_SUBMITTER = 0
    ROLE_SUPER_USER = 1
    ANONYMOUS = 2
    REVIEWER = 3  # this is not in java project
    SYSTEM_ADMIN = 4  # this is not in java project
    
    @staticmethod
    def from_name(name: str):
        if not name:
            return UserRole.ANONYMOUS
        if name.upper() == "ROLE_SUBMITTER":
            return UserRole.ROLE_SUBMITTER
        elif name.upper() == "ROLE_SUPER_USER":
            return UserRole.ROLE_SUPER_USER
        elif name.upper() == "REVIEWER":
            return UserRole.REVIEWER
        elif name.upper() == "SYSTEM_ADMIN":
            return UserRole.SYSTEM_ADMIN
        return UserRole.ANONYMOUS

class UserStatus(Enum):
    NEW = 0
    VERIFIED = 1
    ACTIVE = 2
    FROZEN = 3
    
    @staticmethod
    def from_name(name: str):
        if not name:
            return UserStatus.FROZEN
        if name.upper() == "NEW":
            return UserStatus.NEW
        elif name.upper() == "VERIFIED":
            return UserStatus.VERIFIED
        elif name.upper() == "ACTIVE":
            return UserStatus.ACTIVE
        elif name.upper() == "FROZEN":
            return UserStatus.FROZEN
        return UserStatus.FROZEN
    
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
    SEND_EMAIL = 'SEND_EMAIL'
