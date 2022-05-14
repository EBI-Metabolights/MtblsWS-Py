from flask_restful import abort


def metabolights_exception_handler(func):
    def exception_handler(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except MetabolightsException as e:
            abort(e.http_code, message=e.message, exception=e)

    return exception_handler


class MetabolightsException(Exception):

    def __init__(self, message: str, exception : Exception = None, http_code=400):
        super(MetabolightsException, self).__init__()
        self.message = message
        self.exception = exception
        self.http_code = http_code


class MetabolightsDBException(MetabolightsException):

    def __init__(self, message: str, exception : Exception = None, http_code=501):
        super(MetabolightsDBException, self).__init__(message, exception, http_code)


class MetabolightsFileOperationException(MetabolightsException):

    def __init__(self, message: str, exception : Exception = None, http_code=400):
        super(MetabolightsFileOperationException, self).__init__(message, exception, http_code)


class MetabolightsAuthorizationException(MetabolightsException):

    def __init__(self, message: str, exception : Exception = None, http_code=401):
        super(MetabolightsAuthorizationException, self).__init__(message, exception, http_code)
