import json
import traceback
import string
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


def return_validations(section, validations, validates, amber_warning):
    if not validates:
        ret_list = {section: validations, "status_message": "Validation failed",
                    "overall_status": error}
    elif amber_warning:
        ret_list = {section: validations,
                    "status_message": "Some optional information is missing for your study",
                    "overall_status": warning}
    else:
        ret_list = {section: validations, "status_message": "Successful validation",
                    "overall_status": success}

    return validates, amber_warning, ret_list


def remove_nonprintable(text):
    import string
    # Get the difference of all ASCII characters from the set of printable characters
    nonprintable = set([chr(i) for i in range(128)]).difference(string.printable)
    # Use translate to remove all non-printable characters
    return text.translate({ord(character):None for character in nonprintable})


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
    error_found = False
    warning_found = False

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
    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    # Validate publications reported on the study
    status, amber_warning, pub_validation = validate_publication(isa_study, validation_schema)
    all_validations.append(pub_validation)
    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    # Validate detailed metadata in ISA-Tab structure
    status, amber_warning, isa_meta_validation = validate_isa_tab_metadata(isa_inv, isa_study, validation_schema)
    all_validations.append(isa_meta_validation)

    # Validate Person (authors)
    status, amber_warning, isa_person_validation = validate_contacts(isa_study, validation_schema)
    all_validations.append(isa_person_validation)

    # Validate Protocols
    status, amber_warning, isa_protocol_validation = validate_protocols(isa_study, validation_schema)
    all_validations.append(isa_protocol_validation)

    # Validate Samples
    # status, amber_warning, isa_sample_validation = validate_samples(isa_study, validation_schema)
    # all_validations.append(isa_sample_validation)

    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    if error_found:
        return {"validation": {"study_validation_status": error, "validations": all_validations}}

    if warning_found:
        return {"validation": {"study_validation_status": warning, "validations": all_validations}}

    return {"validation": {"study_validation_status": success, "validations": all_validations}}


def validate_samples(isa_study, validation_schema):
    # check for Publication
    validates = True
    val_section = "samples"
    amber_warning = False
    validations = []

    if validation_schema:
        study_val = validation_schema['study']
        val = study_val['samples']

    # Todo, val.default order
    #val.samples


    name_rules, name_val_description = get_complex_validation_rules(
        validation_schema, part='samples', sub_part='protocol', sub_set='name')
    name_val_len, name_val_error, name_val_condition, name_val_type = extract_details(name_rules)

    desc_rules, desc_val_description = get_complex_validation_rules(
        validation_schema, part='protocols', sub_part='protocol', sub_set='description')
    desc_val_len, desc_val_error, desc_val_condition, desc_val_type = extract_details(desc_rules)

    param_rules, param_val_description = get_complex_validation_rules(
        validation_schema, part='protocols', sub_part='protocol', sub_set='parameterName')
    param_val_len, param_val_error, param_val_condition, param_val_type = extract_details(param_rules)

    if isa_study.protocols:
        for protocol in isa_study.protocols:
            prot_name = protocol.name
            prot_desc = protocol.description
            clean_prot_desc = remove_nonprintable(prot_desc)
            prot_params = protocol.protocol_type

            # non printable characters
            if prot_desc != clean_prot_desc:
                add_msg(validations, "Protocol", "Protocol description contains non printable characters",
                        error, value=prot_desc)
                validates = False
            else:
                add_msg(validations, "Protocol", "Protocol description only contains printable characters",
                        success, value=prot_desc)

            if len(prot_name) >= name_val_len:
                add_msg(validations, "Protocol", "Protocol name validates", success, value=prot_name)
            else:
                add_msg(validations, "Protocol", name_val_error, error, value=prot_name, desrc=name_val_description)
                validates = False

            if len(prot_desc) >= desc_val_len:
                if prot_desc == 'Please update this protocol description':
                    add_msg(validations, "Protocol", desc_val_error, warning, value=prot_desc,
                            desrc='Please update this protocol description')
                    amber_warning = True
                add_msg(validations, "Protocol", "Protocol description validates", success, value=prot_desc)
            else:
                add_msg(validations, "Protocol", desc_val_error, error, value=prot_desc, desrc=desc_val_description)
                validates = False

            if len(prot_params.term) >= param_val_len:
                add_msg(validations, "Protocol", "Protocol parameter validates", success, value=prot_params.term)
            else:
                add_msg(validations, "Protocol", param_val_error, error, value=prot_params.term, desrc=param_val_description)
                validates = False

    return return_validations(val_section, validations, validates, amber_warning)


def validate_protocols(isa_study, validation_schema):
    # check for Publication
    validates = True
    val_section = "protocols"
    amber_warning = False
    validations = []
    protocol_order_list = None
    is_nmr = False
    is_ms = False
    default_prots = []

    if isa_study.assays:
        for assay in isa_study.assays:
            assay_type_onto = assay.technology_type
            if assay_type_onto.term == 'mass spectrometry':
                is_ms = True
            elif assay_type_onto.term == 'NMR spectroscopy':
                is_nmr = True

    # List if standard protocols that should be present
    if validation_schema:
        study_val = validation_schema['study']
        val = study_val['protocols']
        protocol_order_list = val['default']
        for prot in protocol_order_list:
            if is_nmr:
                if 'NMR ' in prot['technique']:
                    default_prots.append(prot)
            elif is_ms:
                if 'mass ' in prot['technique']:
                    default_prots.append(prot)

    # protocol order
    for idx, protocol in enumerate(default_prots):
        prot_val_name = protocol['title']
        isa_prot_name = ""
        try:
            isa_prot = isa_study.protocols[idx]
            isa_prot_name = isa_prot.name

            if prot_val_name != isa_prot_name:
                add_msg(validations, val_section, "Protocol '" + isa_prot_name + "' is not in the correct position",
                        warning)
                amber_warning = True
            else:
                add_msg(validations, val_section, "Protocol '" + isa_prot_name + "' is in the correct position",
                        success)
        except:
            add_msg(validations, val_section, "Protocol '" + isa_prot_name + "' was not found", error)
            

    name_rules, name_val_description = get_complex_validation_rules(
        validation_schema, part='protocols', sub_part='protocol', sub_set='name')
    name_val_len, name_val_error, name_val_condition, name_val_type = extract_details(name_rules)

    desc_rules, desc_val_description = get_complex_validation_rules(
        validation_schema, part='protocols', sub_part='protocol', sub_set='description')
    desc_val_len, desc_val_error, desc_val_condition, desc_val_type = extract_details(desc_rules)

    param_rules, param_val_description = get_complex_validation_rules(
        validation_schema, part='protocols', sub_part='protocol', sub_set='parameterName')
    param_val_len, param_val_error, param_val_condition, param_val_type = extract_details(param_rules)

    if isa_study.protocols:
        for protocol in isa_study.protocols:
            prot_name = protocol.name
            prot_desc = protocol.description
            clean_prot_desc = remove_nonprintable(prot_desc)
            prot_params = protocol.protocol_type

            # non printable characters
            if prot_desc != clean_prot_desc:
                add_msg(validations, "Protocol", "Protocol description contains non printable characters",
                        error, value=prot_desc)
                validates = False
            else:
                add_msg(validations, "Protocol", "Protocol description only contains printable characters",
                        success, value=prot_desc)

            if len(prot_name) >= name_val_len:
                add_msg(validations, "Protocol", "Protocol name validates", success, value=prot_name)
            else:
                add_msg(validations, "Protocol", name_val_error, error, value=prot_name, desrc=name_val_description)
                validates = False

            if len(prot_desc) >= desc_val_len:
                if prot_desc == 'Please update this protocol description':
                    add_msg(validations, "Protocol", desc_val_error, warning, value=prot_desc,
                            desrc='Please update this protocol description')
                    amber_warning = True
                add_msg(validations, "Protocol", "Protocol description validates", success, value=prot_desc)
            else:
                add_msg(validations, "Protocol", desc_val_error, error, value=prot_desc, desrc=desc_val_description)
                validates = False

            if len(prot_params.term) >= param_val_len:
                add_msg(validations, "Protocol", "Protocol parameter validates", success, value=prot_params.term)
            else:
                add_msg(validations, "Protocol", param_val_error, error, value=prot_params.term, desrc=param_val_description)
                validates = False

    return return_validations(val_section, validations, validates, amber_warning)


def validate_contacts(isa_study, validation_schema):
    # check for Publication
    validates = True
    amber_warning = False
    validations = []

    lastName_rules, lastName_val_description = get_complex_validation_rules(
        validation_schema, part='people', sub_part='person', sub_set='lastName')
    lastName_val_len, lastName_val_error, lastName_val_condition, lastName_val_type = extract_details(lastName_rules)

    firstName_rules, firstName_val_description = get_complex_validation_rules(
        validation_schema, part='people', sub_part='person', sub_set='firstName')
    firstName_val_len, firstName_val_error, firstName_val_condition, firstName_val_type = extract_details(firstName_rules)

    email_rules, email_val_description = get_complex_validation_rules(
        validation_schema, part='people', sub_part='person', sub_set='email')
    email_val_len, email_val_error, email_val_condition, email_val_type = extract_details(email_rules)

    affiliation_rules, affiliation_val_description = get_complex_validation_rules(
        validation_schema, part='people', sub_part='person', sub_set='affiliation')
    affiliation_val_len, affiliation_val_error, affiliation_val_condition, affiliation_val_type = extract_details(affiliation_rules)

    if isa_study.contacts:
        for person in isa_study.contacts:
            last_name = person.last_name
            first_name = person.first_name
            email = person.email
            affiliation = person.affiliation

            if last_name:
                if len(last_name) >= lastName_val_len:
                    add_msg(validations, "Person", "Persons last name validates", success)
                else:
                    add_msg(validations, "Person", lastName_val_error, error, value=last_name,
                            desrc=lastName_val_description)
                    validates = False

            if first_name:
                if len(first_name) >= firstName_val_len:
                    add_msg(validations, "Person", "Persons first name validates", success)
                else:
                    add_msg(validations, "Person", firstName_val_error, error, value=first_name,
                            desrc=firstName_val_description)
                    validates = False

            if email:
                if len(email) >= email_val_len:
                    add_msg(validations, "Person", "Persons email validates", success)
                else:
                    add_msg(validations, "Person", email_val_error, error, value=email, desrc=email_val_error)
                    validates = False

            if affiliation:
                if len(affiliation) >= affiliation_val_len:
                    add_msg(validations, "Person", "Persons affiliation validates", success)
                else:
                    add_msg(validations, "Person", affiliation_val_error, error, value=affiliation,
                            desrc=affiliation_val_error)
                    validates = False

    if not validates:
        ret_list = {"people": validations, "status_message": "Validation failed",
                    "overall_status": error}
    elif amber_warning:
        ret_list = {"people": validations,
                    "status_message": "Some optional information is missing for your study",
                    "overall_status": warning}
    else:
        ret_list = {"people": validations, "status_message": "Successful validation",
                    "overall_status": success}

    return validates, amber_warning, ret_list


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
                    add_msg(validations, "Publication DOI", doi_val_error, warning, doi)
                    doi = False
                    amber_warning = True

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
        add_msg(validations, "ISA-Tab",
                "Loading ISA-Tab without sample and assay tables. "
                "Protocol parameters does not match the protocol definition", warning)
        logger.error("Loading ISA-Tab without sample and assay tables due to critical error: " + err)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

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

        # isaconfig
        if isa_inv.get_comment('Created With Configuration'):
            create_config = isa_inv.get_comment('Created With Configuration')
            open_config = None
            if isa_inv.get_comment('Last Opened With Configuration'):
                open_config = isa_inv.get_comment('Last Opened With Configuration')

            if 'isaconfig' in create_config.value:
                add_msg(validations, "ISA-Tab", "Incorrect configuration files used to create the study. "
                                                "The study may not contain required fields", warning)
                amber_warning = True
            if 'isaconfig' in open_config.value:
                add_msg(validations, "ISA-Tab", "Incorrect configuration files used to edit the study. "
                                                "The study may not contain required fields", warning)
                amber_warning = True

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
