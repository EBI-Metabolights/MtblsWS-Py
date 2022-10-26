from enum import Enum


class Acl(int, Enum):
    READ_ONLY = 0o550
    AUTHORIZED_READ = 0o750
    AUTHORIZED_READ_WRITE = 0o770
    UNKNOWN = 0o700
