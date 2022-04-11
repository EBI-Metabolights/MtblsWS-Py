from app.ws.mtblsWSclient import WsClient
from app.ws.utils import log_request
from flask_restful import abort


wsc = WsClient()


class RequestValidationResult(object):

    def __init__(self, is_curator, read_access, write_access, obfuscation_code,
                 study_location, release_date, submission_date, study_status):
        self.is_curator = is_curator
        self.read_access = read_access
        self.write_access = write_access
        self.obfuscation_code = obfuscation_code
        self.study_location = study_location
        self.release_date = release_date
        self.submission_date = submission_date
        self.study_status = study_status


def validate_restricted_ws_request(request, study_id='MTBLS1'):
    log_request(request)

    # User authentication
    user_token = None
    if "user_token" in request.headers:
        user_token = request.headers["user_token"]
    else:
        # user token is required
        abort(401)

    # check for access rights
    is_curator, read_access, write_access, obfuscation_code, \
    study_location, release_date, submission_date, study_status = wsc.get_permissions(study_id, user_token)

    validation_result = RequestValidationResult(is_curator, read_access, write_access, obfuscation_code,
                                                study_location, release_date, submission_date, study_status)
    if not validation_result.read_access:
        abort(403)

    return validation_result
