from flask_restful import abort

from app.ws.mtblsWSclient import WsClient


class PermissionsObj:
    """
    This object will eventually be extended to all API resources. For the time being though, it will live in validation
    utils until a better fitting class is implemented.
    """

    def __init__(self, study_id, req_headers):
        """
        Query the java webservice to authenticate the user and their study, and then pass the returned values as
        attributes on the permissions object. Aborts if either user_token or study_id is not present.
        """
        self.user_token = None
        self.study_id = study_id

        if "user_token" in req_headers:
            self.user_token = req_headers["user_token"]

        if not self.is_complete():
            abort(401)

        webservice_client = WsClient()
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = webservice_client.get_permissions(study_id, self.user_token)

        self.is_curator = is_curator
        self.read_access = read_access
        self.write_access = write_access
        self.obfuscation_code = obfuscation_code
        self.study_location = study_location
        self.release_date = release_date
        self.submission_date = submission_date
        self.study_status = study_status.lower()

    def is_complete(self):
        return self.user_token is not None and self.study_id is not None
