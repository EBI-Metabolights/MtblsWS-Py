class StorageServiceException(Exception):
    ERR_CODE_FILE_NOT_EXIST = 1
    ERR_CODE_FILE_EXIST = 2
    ERR_CODE_OS_ERROR = 3
    ERR_CODE_NOT_ALLOWED_FILE = 4
    ERR_CODE_NOT_ALLOWED_OPERATION = 5

    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
