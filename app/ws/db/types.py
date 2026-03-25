import enum


class UserRole(enum.IntEnum):
    ROLE_SUBMITTER = 0
    ROLE_SUPER_USER = 1
    ANONYMOUS = 2
    REVIEWER = 3  # this is not in java project
    SYSTEM_ADMIN = 4  # this is not in java project

    @staticmethod
    def from_name(name: str):
        if not name:
            return UserRole.ANONYMOUS
        if name.upper() in {"ROLE_SUBMITTER", "SUBMITTER"}:
            return UserRole.ROLE_SUBMITTER
        if name.upper() in {"ROLE_SUPER_USER", "CURATOR"}:
            return UserRole.ROLE_SUPER_USER
        if name.upper() == "REVIEWER":
            return UserRole.REVIEWER
        if name.upper() in {"SYSTEM_ADMIN", "ADMIN"}:
            return UserRole.SYSTEM_ADMIN
        return UserRole.ANONYMOUS

    def to_name(self):
        if self.name == "ROLE_SUBMITTER":
            return "submitter"
        if self.name == "ROLE_SUPER_USER":
            return "curator"
        if self.name == "REVIEWER":
            return "reviewer"
        if self.name == "SYSTEM_ADMIN":
            return "admin"
        return "anonymous"


ActiveUserRoles: set[UserRole] = {
    UserRole.ROLE_SUBMITTER,
    UserRole.ROLE_SUPER_USER,
    UserRole.SYSTEM_ADMIN,
}


class CurationRequest(enum.IntEnum):
    MANUAL_CURATION = 0
    NO_CURATION = 1
    SEMI_AUTOMATED_CURATION = 2

    @staticmethod
    def from_name(name: str, fail_for_invalid_value: bool = False):
        if not name:
            if fail_for_invalid_value:
                raise ValueError(None)
            return None
        if name.upper() == "Manual Curation".upper():
            return CurationRequest.MANUAL_CURATION
        elif name.upper() == "No Curation".upper():
            return CurationRequest.NO_CURATION
        elif name.upper() == "Semi-automated Curation".upper():
            return CurationRequest.SEMI_AUTOMATED_CURATION
        if fail_for_invalid_value:
            raise ValueError(None)
        return None

    def to_camel_case_str(self):
        if self.value == 0:
            return "Manual Curation"
        if self.value == 1:
            return "No Curation"
        elif self.value == 2:
            return "Semi-automated Curation"
        return "Manual Curation"


class UserStatus(enum.IntEnum):
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


class StudyStatus(enum.IntEnum):
    PROVISIONAL = 0
    PRIVATE = 1
    INREVIEW = 2
    PUBLIC = 3
    DORMANT = 4

    @staticmethod
    def from_name(name: str, fail_for_invalid_value: bool = False):
        if not name:
            if fail_for_invalid_value:
                raise ValueError(name)
            return StudyStatus.PROVISIONAL
        if name.replace(" ", "").upper() == "PROVISIONAL":
            return StudyStatus.PROVISIONAL
        elif name.replace(" ", "").upper() == "PRIVATE":
            return StudyStatus.PRIVATE
        elif name.replace(" ", "").upper() == "INREVIEW":
            return StudyStatus.INREVIEW
        elif name.replace(" ", "").upper() == "PUBLIC":
            return StudyStatus.PUBLIC
        elif name.replace(" ", "").upper() == "DORMANT":
            return StudyStatus.DORMANT
        if fail_for_invalid_value:
            raise ValueError(name)
        return StudyStatus.DORMANT

    @staticmethod
    def from_int(value: int):
        if value == 0:
            return StudyStatus.PROVISIONAL
        if value == 1:
            return StudyStatus.PRIVATE
        elif value == 2:
            return StudyStatus.INREVIEW
        elif value == 3:
            return StudyStatus.PUBLIC
        elif value == 4:
            return StudyStatus.DORMANT
        return StudyStatus.DORMANT

    def to_camel_case_str(self):
        if self.value == 0:
            return "Provisional"
        if self.value == 1:
            return "Private"
        elif self.value == 2:
            return "In Review"
        elif self.value == 3:
            return "Public"
        elif self.value == 4:
            return "Dormant"
        return "Dormant"


class StudyCategory(enum.IntEnum):
    OTHER = 0
    MS_MHD_ENABLED = 1
    MS_IMAGING = 2
    MS_OTHER = 3
    NMR = 4
    MS_MHD_LEGACY = 5

    @staticmethod
    def from_name(name: str):
        if not name:
            return StudyCategory.OTHER
        try:
            return StudyCategory[name.upper().replace("-", "_")]
        except Exception:
            return StudyCategory.OTHER

    def get_label(self):
        return self.name.lower().replace("_", "-")


class StudyTaskStatus(enum.StrEnum):
    NOT_EXECUTED = "NOT_EXECUTED"
    EXECUTING = "EXECUTING"
    EXECUTION_SUCCESSFUL = "EXECUTION_SUCCESSFUL"
    EXECUTION_FAILED = "EXECUTION_FAILED"


class StudyTaskName(enum.StrEnum):
    REINDEX = "REINDEX"
    SEND_TWEET = "SEND_TWEET"
    SEND_EMAIL = "SEND_EMAIL"


class StudyRevisionStatus(enum.IntEnum):
    INITIATED = 0
    IN_PROGRESS = 1
    FAILED = 2
    COMPLETED = 3

    def get_as_string(self):
        name = self.name
        if name == "IN_PROGRESS":
            return "In Progress"
        return name.capitalize()


class MhdSubmissionStatus(enum.IntEnum):
    INITIATED = 0
    IN_PROGRESS = 1
    FAILED = 2
    COMPLETED = 3

    def get_as_string(self):
        name = self.name
        if name == "IN_PROGRESS":
            return "In Progress"
        return name.capitalize()
