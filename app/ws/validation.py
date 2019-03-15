import json
import traceback
from flask import request, abort
from flask_restful import Resource
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


def add_msg(validations, section, message, status, value="", desrc=""):
    validations.append({section: message, "status": status, "value": value, "desciption": desrc})


def get_basic_validation_rules(validation_schema, part):
    if validation_schema:
        study_val = validation_schema['study']
        val = study_val[part]
        rules = val['rules'][0]
    return rules, val['description']


def get_complex_validation_rules(validation_schema, part, sub_part, sub_set):
    if validation_schema:
        study_val = validation_schema['study']
        val = study_val[part]
        sub = val[sub_part]
        sets = sub[sub_set]
        rules = sets['rules'][0]
    return rules, sets['description']


def extract_details(rule):
    try:
        try:
            val = int(rule['value'])
        except:
            val = rule['value']
        val_error = rule['error']
        val_condition = rule['condition']
        val_type = rule['type']
    except:
        return int(0), "n/a", "n/a", "n/a"
    return val, val_error, val_condition, val_type


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

    title_rules, title_val_description = get_complex_validation_rules(
        validation_schema, part='publications', sub_part='publication', sub_set='title')
    title_val_len, title_val_error, title_val_condition, title_val_type = extract_details(title_rules)

    if isa_study.publications:
        amber_warning = False
        pmid = False
        doi = False

        pmid_rules, pmid_val_description = get_complex_validation_rules(
            validation_schema, part='publications', sub_part='publication', sub_set='pubMedID')
        pmid_val_len, pmid_val_error, pmid_val_condition, pmid_val_type = extract_details(pmid_rules)

        doi_rules, doi_val_description = get_complex_validation_rules(
            validation_schema, part='publications', sub_part='publication', sub_set='doi')
        doi_val, doi_val_error, doi_val_condition, doi_val_type = extract_details(doi_rules)

        author_rules, author_val_description = get_complex_validation_rules(
            validation_schema, part='publications', sub_part='publication', sub_set='authorList')
        author_val_len, author_val_error, author_val_condition, author_val_type = extract_details(author_rules)

        status_rules, status_val_description = get_complex_validation_rules(
            validation_schema, part='publications', sub_part='publication', sub_set='status')
        status_val_len, status_val_error, status_val_condition, status_val_type = extract_details(status_rules)

        for index, publication in enumerate(isa_study.publications):

            if not publication:
                add_msg(validations, "Publication", title_val_error, error)
                validates = False
            else:
                add_msg(validations, "Publication", "Found a publication", success)

            if not publication.title:
                add_msg(validations, "Publication title", title_val_error, error)
                validates = False
            elif publication.title:
                if len(publication.title) >= title_val_len:
                    add_msg(validations, "Publication title", "Found the title of the publication", success)
                else:
                    add_msg(validations, "Publication title", title_val_error, warning,
                            value=publication.title, desrc=title_val_description)

            if not publication.doi:
                add_msg(validations, "Publication DOI", doi_val_description, warning)
                doi = False
                amber_warning = True
            elif publication.doi:
                doi = publication.doi
                doi_pattern = re.compile(doi_val)
                doi_check = doi_pattern.match(doi)
                if doi_check:
                    add_msg(validations, "Publication DOI", "Found the doi for the publication", success)
                    doi = True
                else:
                    add_msg(validations, "Publication DOI", doi_val_error, error, doi)
                    doi = False

            if not publication.pubmed_id:
                add_msg(validations, "Publication PubMed ID", pmid_val_description, warning)
                amber_warning = True
                pmid = False
            elif publication.pubmed_id:
                if int(publication.pubmed_id) >= int(pmid_val_len):
                    add_msg(validations, "Publication PubMed ID", "Found the pmid for the publication", success)
                    pmid = True
                else:
                    add_msg(validations, "Publication PubMed ID", pmid_val_error, error,
                            value=publication.pubmed_id, desrc=pmid_val_description)
                    pmid = False

            if not doi or not pmid:
                add_msg(validations, "Publication doi and PubMed ID",
                        "Please provide both a valid doi and pmid for the publication", warning)
                amber_warning = True
            elif doi and pmid:
                add_msg(validations, "Publication doi and PubMed ID ",
                        "Found both doi and pmid for the publication", success)

            if not publication.author_list:
                add_msg(validations, "Publication author list", author_val_description, error)
                validates = False
            elif publication.author_list:
                if len(publication.author_list) >= author_val_len:
                    add_msg(validations, "Publication author list",
                            "Found the author list for the publication", success)
                else:
                    add_msg(validations, "Publication author list", author_val_error, error,
                            value=publication.author_list, desrc=author_val_description)
                    validates = False

            if not publication.status:
                add_msg(validations, "Publication status", "Please provide the publication status", error)
                validates = False
            elif publication.status:
                pub_status = publication.status
                if len(pub_status.term) >= status_val_len:
                    add_msg(validations, "Publication status", "Found the publication status", success)
                else:
                    add_msg(validations, "Publication status", status_val_error, success,
                            value=pub_status.title, desrc=status_val_description)
    else:
        add_msg(validations, "Publication missing", title_val_error, error)
        validates = False

    if not validates:
        ret_list = {"publication": validations, "status_message": "Validation failed",
                    "overall_status": error}
    elif amber_warning:
        ret_list = {"publication": validations,
                    "status_message": "Some optional information is missing for your study",
                    "overall_status": warning}
    else:
        ret_list = {"publication": validations, "status_message": "Successful validation",
                    "overall_status": success}

    return validates, amber_warning, ret_list


def validate_basic_isa_tab(study_id, user_token, study_location):
    validates = True
    amber_warning = False
    validations = []

    try:
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)
    except ValueError:
        err = traceback.format_exc()
        add_msg(validations, "ISA-Tab", "Critical error: " + err, error)

    if isa_inv:
        add_msg(validations, "ISA-Tab", "Successfully read the i_Investigation.txt files", success)

        if isa_study:
            add_msg(validations, "ISA-Tab", "Successfully read the study section of the investigation file", success)
        else:
            add_msg(validations, "ISA-Tab", "Can not read the study section of the investigation file", error)
            validates = False

        if isa_study.filename:
            add_msg(validations, "ISA-Tab", "Successfully found the reference to the sample sheet filename", success)
        else:
            add_msg(validations, "ISA-Tab", "Could not find the reference to the sample sheet filename", error)
            validates = False

        if validates:  # Have to have a basic investigation and sample file before we can continue
            if isa_study.samples:
                add_msg(validations, "ISA-Tab", "Successfully found one or more samples", success)
            else:
                add_msg(validations, "ISA-Tab", "Could not find any samples", error)
                validates = False

            if isa_study.assays:
                add_msg(validations, "ISA-Tab", "Successfully found one or more assays", success)
            else:
                add_msg(validations, "ISA-Tab", "Could not find any assays", error)
                validates = False

            if isa_study.factors:
                add_msg(validations, "ISA-Tab", "Successfully found one or more factors", success)
            else:
                add_msg(validations, "ISA-Tab", "Could not find any factors", warning)
                amber_warning = True

            if isa_study.design_descriptors:
                add_msg(validations, "ISA-Tab", "Successfully found one or more descriptors", success)
            else:
                add_msg(validations, "ISA-Tab", "Could not find any study design descriptors", error)
                validates = False

    else:
        add_msg(validations, "ISA-Tab", "Can not find or read the investigation files", error)
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
        title_rules, title_descr = get_basic_validation_rules(validation_schema, 'title')
        desc_rules, desc_desrc = get_basic_validation_rules(validation_schema, 'description')

    if isa_inv:

        # Title
        val_len, val_error, val_condition, val_type = extract_details(title_rules)
        try:
            title_len = len(isa_study.title)
        except:
            title_len = 0

        if title_len >= val_len:
            add_msg(validations, "Metadata", "The title length validates", success)
        else:
            add_msg(validations, "Metadata", val_error, error, value=isa_study.title, desrc=title_descr)
            validates = False

        # Description
        val_len, val_error, val_condition, val_type = extract_details(desc_rules)
        try:
            descr_len = len(isa_study.description)
        except:
            descr_len = 0

        if descr_len >= val_len:
            add_msg(validations, "Metadata", "The length of the description validates", success)
        else:
            add_msg(validations, "Metadata", val_error, error, value=isa_study.description, desrc=desc_desrc)
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
