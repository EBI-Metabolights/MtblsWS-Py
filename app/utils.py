from flask import make_response
import cProfile, pstats
import datetime
import os

def metabolights_exception_handler(func):
    def exception_handler(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except MetabolightsException as e:
            data = {"content": None, "message": e.message, "err": str(e)}
            response = make_response(data, e.http_code)
            return response
        except Exception as e:
            data = {"content": None, "message": "Error while procession data", "err": str(e)}
            response = make_response(data, 400)
            return response

    return exception_handler

def metabolights_profiler(func):
    def profiler_handler(*args, **kwargs):
        profiler = cProfile.Profile()
        profiler.enable()
        try:
            return func(*args, **kwargs)
        finally:
            profiler.disable()
            stats = pstats.Stats(profiler)
            os.makedirs(f"./.profile/{func.__module__}", exist_ok=True)
            stats.dump_stats(f'./.profile/{func.__module__}/{func.__qualname__}_profile_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.prof')
        
    return profiler_handler
class MetabolightsException(Exception):

    def __init__(self, message: str, exception: Exception = None, http_code=400):
        super(MetabolightsException, self).__init__()
        self.message = message
        self.exception = exception
        self.http_code = http_code

    def __str__(self):
        if self.exception:
            return f"{str(self.__class__.__name__)}: {self.message}, http_code: {self.http_code} Cause -->: [{str(self.exception)}]"
        else:
            return f"{str(self.__class__.__name__)}: {self.message}, http_code: {self.http_code}"


class MetabolightsDBException(MetabolightsException):

    def __init__(self, message: str, exception: Exception = None, http_code=501):
        super(MetabolightsDBException, self).__init__(message, exception, http_code)


class MetabolightsFileOperationException(MetabolightsException):

    def __init__(self, message: str, exception: Exception = None, http_code=400):
        super(MetabolightsFileOperationException, self).__init__(message, exception, http_code)


class MetabolightsAuthorizationException(MetabolightsException):

    def __init__(self, message: str, exception: Exception = None, http_code=401):
        super(MetabolightsAuthorizationException, self).__init__(message, exception, http_code)
