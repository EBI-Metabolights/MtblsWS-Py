from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import *
from app.ws.isaApiClient import IsaApiClient

logger = logging.getLogger('wslog')
wsc = WsClient()
iac = IsaApiClient()

warning = "warning"
error = "error"
success = "success"

class Validation(Resource):
    @swagger.operation(
        summary="Validate study",
        notes='''Validating the overall study. 
        This method will validate the study metadata and check some of the files study folder''',
        parameters=[
            {
                "name": "study_id",
                "description": "Study to validate",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication. "
                           "Please provide a study id and a valid user token"
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def post(self, study_id):

        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(401)

        study_id = study_id.upper()

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        return validate_study(study_id, study_location, user_token)


def validate_study(study_id, study_location, user_token):
    validations = []
    isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                     skip_load_tables=True,
                                                     study_location=study_location)

    status, amber_warning, validations = validate_publication(isa_study, validations)
    # status, amber_warning, validations = validate_protocols(isa_study, validations)
    # status, validation.append(validate_protocols())
    # status, validation.append(validate_protocols())
    # status, validation.append(validate_protocols())
    # status, validation.append(validate_protocols())
    # status, validation.append(validate_protocols())
    # status, validation.append(validate_protocols())
    # status, validation.append(validate_protocols())
    # status, validation.append(validate_protocols())
    # status, validation.append(validate_protocols())

    if status:
        if amber_warning:
            return {"validation": validations, "study_validation_status": warning}
        else:
            return {"validation": validations, "study_validation_status": success}
    else:
        return {"validation": validations, "study_validation_status": error}


def validate_publication(isa_study, validations):
    # check for Publication
    validates = True
    amber_warning = False

    if isa_study.publications:
        amber_warning = False
        pmid = False
        doi = False

        for index, publication in enumerate(isa_study.publications):
            #ToDo, check doi and pmid strings

            if not publication:
                validations.append({"Publication": "Publication is missing. Please provide a tentative title of your paper",
                                    "status": error})
                validates = False
            else:
                validations.append({"Publication": "Found your paper",
                                    "status": success})

            if not publication.title:
                validations.append({"Publication title": "Please provide a tentative title of your paper",
                                    "status": error})
                validates = False
            elif publication.title:
                validations.append({"Publication title": "Found the title of your paper",
                                    "status": success})

            if not publication.doi:
                validations.append({"Publication DOI": "Please provide a doi for your paper",
                                    "status": warning})
                doi = False
                amber_warning = True
            elif publication.doi:
                validations.append({"Publication DOI": "Found the doi for your paper",
                                    "status": success})
                doi = True

            if not publication.pubmed_id:
                validations.append({"Publication PubMed ID": "Please provide a pmid for your paper",
                                    "status": warning})
                amber_warning = True
                pmid = False
            elif publication.pubmed_id:
                validations.append({"Publication PubMed ID": "Found the pmid for your paper",
                                    "status": success})
                pmid = True

            if not doi or not pmid:
                validations.append(
                    {"Publication doi and PubMed ID": "Please provide both a doi and pmid for your paper",
                     "status": warning})
                amber_warning = True
            elif doi and pmid:
                validations.append(
                    {"Publication doi and PubMed ID ": "Found both doi and pmid for your paper",
                     "status": success})

            if not publication.author_list:
                validations.append({"Publication author list": "Please provide an author list",
                                    "status": error})
                validates = False
            elif publication.author_list:
                validations.append({"Publication author list": "Found the author list",
                                    "status": success})

            if not publication.status:
                validations.append({"Publication status": "Please provide the publication status",
                                    "status": error})
                validates = False
            elif publication.status:
                validations.append({"Publication status": "Found the publication status",
                                    "status": success})
    else:
        validations.append({"Publication missing": "Please provide a publication using the tentative title of your paper",
                            "status": error})
        validates = False

    if not validates:
        ret_list = [{"publication": validations, "overall_status": "Validation failed",
                     "overall_status": error}]
    elif amber_warning:
        ret_list = [{"publication": validations,
                     "overall_status": "Some optional information is missing for your study",
                     "overall_status": warning}]
    else:
        ret_list = [{"publication": validations, "overall_status": "Successful validation",
                     "overall_status": success}]

    return validates, amber_warning, ret_list


def validate_protocols(isa_study, validations):
    validates = True
    amber_warning = False

    if not validates:
        ret_list = [{"protocols": validations, "overall_status": "Validation failed",
                     "overall_status": error}]
    elif amber_warning:
        ret_list = [{"protocols": validations,
                     "overall_status": "Some optional information is missing for your study",
                     "overall_status": warning}]
    else:
        ret_list = [{"protocols": validations, "overall_status": "Successful validation",
                     "overall_status": success}]

    return validates, amber_warning, ret_list
