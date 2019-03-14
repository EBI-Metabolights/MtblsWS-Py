import json
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


def add_msg(validations, section, message, status):
    validations.append({section: message, "status": status})


def get_validation_rules(validation_schema, sub_part):
    if validation_schema:
        study_val = validation_schema['study']
        val = study_val[sub_part]
        rules = val['rules'][0]
    return rules


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
    all_validations = []
    validation_schema = None

    try:
        validation_schema_file = app.config.get('VALIDATIONS_FILE')
        with open(validation_schema_file, 'r') as json_file:
            validation_schema = json.load(json_file)
    except:
        all_validations.append({"info": "Could not find the validation schema json file, only basic validation will take place", "status": success})

    # Validate basic ISA-Tab structure
    isa_study, isa_inv, std_path, status, amber_warning, isa_validation = \
        validate_basic_isa_tab(study_id, user_token, study_location)
    all_validations.append(isa_validation)

    # Validate publications reported on the study
    status, amber_warning, pub_validation = validate_publication(isa_study, validation_schema)
    all_validations.append(pub_validation)

    # Validate detailed metadata in ISA-Tab structure
    status, amber_warning, isa_meta_validation = validate_isa_tab_metadata(isa_inv, isa_study, validation_schema)
    all_validations.append(isa_meta_validation)

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
            return {"validations": all_validations, "study_validation_status": warning}
        else:
            return {"validations": all_validations, "study_validation_status": success}
    else:
        return {"validations": all_validations, "study_validation_status": error}


def validate_publication(isa_study, validation_schema):
    # check for Publication
    validates = True
    amber_warning = False
    validations = []

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
        ret_list = {"publication": validations, "overall_status": "Validation failed",
                     "overall_status": error}
    elif amber_warning:
        ret_list = {"publication": validations,
                     "overall_status": "Some optional information is missing for your study",
                     "overall_status": warning}
    else:
        ret_list = {"publication": validations, "overall_status": "Successful validation",
                     "overall_status": success}

    return validates, amber_warning, ret_list


def validate_basic_isa_tab(study_id, user_token, study_location):
    validates = True
    amber_warning = False
    validations = []

    isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                     skip_load_tables=False,
                                                     study_location=study_location)

    if isa_inv:
        validations.append({"ISA-Tab": "Successfully read the i_Investigation.txt files", "status": success})

        if isa_study:
            validations.append({"ISA-Tab": "Successfully read the study section of the investigation file",
                                "status": success})
        else:
            validations.append({"ISA-Tab": "Can not read the study section of the investigation file",
                                "status": error})
            validates = False

        if isa_study.filename:
            validations.append({"ISA-Tab": "Successfully found the reference to the sample sheet filename",
                                "status": success})
        else:
            validations.append({"ISA-Tab": "Could not find the reference to the sample sheet filename",
                                "status": error})
            validates = False

        if validates:  # Have to have a basic investigation and sample file before we can continue
            if isa_study.samples:
                validations.append({"ISA-Tab": "Successfully found one or more samples",
                                    "status": success})
            else:
                validations.append({"ISA-Tab": "Could not find any samples",
                                    "status": error})
                validates = False

            if isa_study.assays:
                validations.append({"ISA-Tab": "Successfully found one or more assays",
                                    "status": success})
            else:
                validations.append({"ISA-Tab": "Could not find any assays",
                                    "status": error})
                validates = False

            if isa_study.factors:
                validations.append({"ISA-Tab": "Successfully found one or more factors",
                                    "status": success})
            else:
                validations.append({"ISA-Tab": "Could not find any factors",
                                    "status": warning})
                amber_warning = True

            if isa_study.design_descriptors:
                validations.append({"ISA-Tab": "Successfully found one or more descriptors",
                                    "status": success})
            else:
                validations.append({"ISA-Tab": "Could not find any study design descriptors",
                                    "status": error})
                validates = False

    else:
        validations.append({"ISA-Tab": "Can not find or read the investigation files", "status": error})
        validates = False

    if not validates:
        ret_list = {"isa-tab": validations, "status_message": "Validation failed",
                     "overall_status": error}
    elif amber_warning:
        ret_list = {"isa-tab": validations,
                     "status_message": "Some optional information is missing for your study",
                     "overall_status": warning}
    else:
        ret_list = {"isa-tab": validations, "status_message": "Successful validation",
                     "overall_status": success}

    return isa_study, isa_inv, std_path, validates, amber_warning, ret_list


def validate_isa_tab_metadata(isa_inv, isa_study, validation_schema):
    validates = True
    amber_warning = False
    validations = []

    if validation_schema:
        title_rules = get_validation_rules(validation_schema, 'title')
        desc_rules = get_validation_rules(validation_schema, 'description')

    if isa_inv:

        # Title
        title_val_len = int(title_rules['value'])
        try:
            title_len = len(isa_inv.title)
        except:
            title_len = 0

        if title_len >= title_val_len:
            add_msg(validations, "Metadata", "The title length validates", success)
        else:
            add_msg(validations, "Metadata", title_rules['error'], error)
            validates = False

        # Description
        desc_val_len = int(desc_rules['value'])
        try:
            descr_len = len(isa_inv.description)
        except:
            descr_len = 0

        if descr_len >= desc_val_len:
            add_msg(validations, "Metadata", "The length of the description validates", success)
        else:
            add_msg(validations, "Metadata", desc_rules['error'], error)
            validates = False

    else:
        add_msg(validations, "Metadata", "Can not find or read the investigation files", error)
        validates = False

    if not validates:
        ret_list = {"isa-tab metadata": validations, "status_message": "Validation failed", "overall_status": error}
    elif amber_warning:
        ret_list = {"isa-tab metadata": validations,
                    "status_message": "Some optional information is missing for your study", "overall_status": warning}
    else:
        ret_list = {"isa-tab metadata": validations, "status_message": "Successful validation",
                    "overall_status": success}

    return validates, amber_warning, ret_list
