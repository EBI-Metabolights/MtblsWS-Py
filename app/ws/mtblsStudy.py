import json
from flask import request, abort, send_file
from flask.json import jsonify
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import *
from distutils.dir_util import copy_tree
from operator import itemgetter

logger = logging.getLogger('wslog')
wsc = WsClient()


# Allow for a more detailed logging when on DEBUG mode
def log_request(request_obj):
    if app.config.get('DEBUG'):
        if app.config.get('DEBUG_LOG_HEADERS'):
            logger.debug('REQUEST HEADERS -> %s', request_obj.headers)
        if app.config.get('DEBUG_LOG_BODY'):
            logger.debug('REQUEST BODY    -> %s', request_obj.data)
        if app.config.get('DEBUG_LOG_JSON'):
            try:
                logger.debug('REQUEST JSON    -> %s', request_obj.json)
            except:
                logger.debug('REQUEST JSON    -> EMPTY')


class MtblsStudies(Resource):
    @swagger.operation(
        summary="Get all Studies",
        notes="Get a list of all public Studies.",
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self):
        log_request(request)
        logger.info('Getting all public studies')
        pub_list = wsc.get_public_studies()
        logger.info('... found %d public studies', len(pub_list['content']))
        return jsonify(pub_list)


class IsaTabInvestigationFile(Resource):

    @swagger.operation(
        summary="Get ISA-Tab Investigation file",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "investigation_filename",
                "description": "Investigation filename",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
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
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('investigation_filename', help='Investigation filename')
        inv_filename = None
        if request.args:
            args = parser.parse_args(req=request)
            inv_filename = args['investigation_filename'].lower() if args['investigation_filename'] else None
        if not inv_filename:
            logger.warning("Missing Investigation filename. Using default i_Investigation.txt")
            inv_filename = 'i_Investigation.txt'
        # check for access rights
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        logger.info('Getting ISA-Tab Investigation file for %s', study_id)
        location = study_location  # wsc.get_study_location(study_id, user_token)
        files = glob.glob(os.path.join(location, inv_filename))
        if files:
            file_path = files[0]
            filename = os.path.basename(file_path)
            try:
                return send_file(file_path, cache_timeout=-1,
                                 as_attachment=True, attachment_filename=filename)
            except OSError as err:
                logger.error(err)
                abort(404, "Wrong filename or file could not be read.")
        else:
            abort(404, "Wrong filename or file could not be read.")


class IsaTabSampleFile(Resource):
    @swagger.operation(
        summary="Get ISA-Tab Sample file",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "sample_filename",
                "description": "Sample filename",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
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
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('sample_filename', help='Sample filename')
        sample_filename = None
        if request.args:
            args = parser.parse_args(req=request)
            sample_filename = args['sample_filename'].lower() if args['sample_filename'] else None
        if not sample_filename:
            logger.warning("Missing Sample filename.")
            abort(400, "Missing Sample filename.")

        # check for access rights
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        logger.info('Getting ISA-Tab Sample file for %s', study_id)
        location = study_location
        files = glob.glob(os.path.join(location, sample_filename))
        if files:
            file_path = files[0]
            filename = os.path.basename(file_path)
            try:
                return send_file(file_path, cache_timeout=-1,
                                 as_attachment=True, attachment_filename=filename)
            except OSError as err:
                logger.error(err)
                abort(404, "Wrong filename or file could not be read.")
        else:
            abort(404, "Wrong filename or file could not be read.")


class IsaTabAssayFile(Resource):

    @swagger.operation(
        summary="Get ISA-Tab Assay file",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "assay_filename",
                "description": "Assay filename",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
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
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('assay_filename', help='Assay filename')
        assay_filename = None
        if request.args:
            args = parser.parse_args(req=request)
            assay_filename = args['assay_filename'].lower() if args['assay_filename'] else None
        if not assay_filename:
            logger.warning("Missing Assay filename.")
            abort(400, "Missing Assay filename.")

        # check for access rights
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        logger.info('Getting ISA-Tab Assay file for %s', study_id)
        location = study_location

        files = glob.glob(os.path.join(location, assay_filename))
        if files:
            file_path = files[0]
            filename = os.path.basename(file_path)
            try:
                return send_file(file_path, cache_timeout=-1,
                                 as_attachment=True, attachment_filename=filename)
            except OSError as err:
                logger.error(err)
                abort(404, "Wrong filename or file could not be read.")
        else:
            abort(404, "Wrong filename or file could not be read.")


class StudyFiles(Resource):

    @swagger.operation(
        summary="Get a list of all files in the study and upload folder(s)",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
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
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, study_id):

        # param validation
        if study_id is None:
            abort(404)

        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        logger.info('Getting list of all files for MTBLS Study %s', study_id)
        upload_location = app.config.get('MTBLS_FTP_ROOT') + study_id.lower() + "-" + obfuscation_code
        logger.info('Getting list of all files for MTBLS Study %s. Study folder: %s. Upload folder: %s', study_id,
                    study_location, upload_location)
        study_files = get_all_files(study_location)
        upload_files = get_all_files(upload_location)

        # Sort the two lists
        study_files, upload_files = [sorted(l, key=itemgetter('file')) for l in (study_files, upload_files)]

        upload_diff = [dict(i) for i in
                       {frozenset(row.items()) for row in upload_files} - 
                       {frozenset(row.items()) for row in study_files}]

        upload_location = upload_location.split('/mtblight')  # FTP/Aspera root starts here

        return jsonify({'studyFiles': study_files,
                        'upload': upload_diff,
                        'upload_all': upload_files,
                        'upload_location': upload_location[1],
                        'obfuscation_code': obfuscation_code})


class CloneAccession(Resource):
    @swagger.operation(
        summary="Create a new study and upload folder",
        notes='''<b>Will clone default LC-MS study if no parameter given</b>''',
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study to clone",
                "required": False,
                "allowMultiple": False,
                "paramType": "query",
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
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            },
            {
                "code": 408,
                "message": "Request Timeout. The MetaboLights queue took too long to complete."
            }
        ]
    )
    def post(self):

        parser = reqparse.RequestParser()
        parser.add_argument('study_id', help="Study Identifier")
        study_id = None

        if request.args:
            args = parser.parse_args(req=request)
            study_id = args['study_id']

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # param validation
        if study_id is None:
            study_id = 'MTBLS121'  # This is the standard LC-MS study. This is private but safe for all to clone

        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)

        if not read_access:
            abort(403)

        study_id = study_id.upper()

        if user_token is None:
            abort(403)

        study_date = get_year_plus_one()
        logger.info('Creating a new MTBLS Study (cloned from %s) with release date %s', study_id, study_date)
        new_folder_name = user_token + '~~' + study_date + '~' + 'new_study_requested_' + get_timestamp()

        study_to_clone = study_location
        queue_folder = wsc.get_queue_folder()
        existing_studies = wsc.get_all_studies_for_user(user_token)
        logger.info('Found the following studies: ' + existing_studies)

        logger.info('Adding ' + study_to_clone + ', using name ' + new_folder_name +
                    ', to the queue folder ' + queue_folder)
        # copy the study onto the queue folder
        try:
            logger.info('Attempting to copy ' + study_to_clone + ' to MetaboLights queue folder ' +
                        os.path.join(queue_folder, new_folder_name))
            copy_tree(study_to_clone, os.path.join(queue_folder, new_folder_name))  # copy the folder to the queue
            # There is a bug in copy_tree which prevents you to use the same destination folder twice
        except:
            return {"error": "Could not add study into the MetaboLights queue"}

        logger.info('Folder successfully added to the queue')
        # get a list of the users private studies to see if a new study has been created
        new_studies = wsc.get_all_studies_for_user(user_token)
        number = 0
        while existing_studies == new_studies:
            number = number + 1
            if number == 20:  # wait for 20 secounds for the MetaboLights queue to process the study
                logger.info('Waited to long for the MetaboLights queue, waiting for email now')
                abort(408)

            logger.info('Checking if the new study has been processed by the queue')
            time.sleep(5)  # Have to check every so many secounds to see if the queue has finished
            new_studies = wsc.get_all_studies_for_user(user_token)

        logger.info('Ok, now there is a new private study for the user')

        # Tidy up the response strings before converting to lists
        new_studies_list = new_studies.replace('[', '').replace(']', '').replace('"', '').split(',')
        existing_studies_list = existing_studies.replace('[', '').replace(']', '').replace('"', '').split(',')

        logger.info('returning the new study, %s', user_token)
        # return the new entry, i.e. difference between the two lists
        diff = list(set(new_studies_list) - set(existing_studies_list))

        study_id = diff[0]
        status = wsc.create_upload_folder(study_id, user_token)
        # logger.info('Study upload folder creation status: ' + status)
        data_dict = json.loads(status)
        os_upload_path = data_dict["message"]
        upload_location = os_upload_path.split('/mtblight')  # FTP/Aspera root starts here

        return {"new_study": study_id, 'upload_location': upload_location[1]}


class CreateUploadFolder(Resource):
    @swagger.operation(
        summary="Create a new study upload folder",
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study Identifier to add an upload folder to",
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
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        logger.info('Creating a new study upload folder for study %s', study_id)
        status = wsc.create_upload_folder(study_id, user_token)

        data_dict = json.loads(status)
        os_upload_path = data_dict["message"]
        upload_location = os_upload_path.split('/mtblight')  # FTP/Aspera root starts here

        return {'os_upload_path': os_upload_path, 'upload_location': upload_location[1]}


class saveAuditFiles(Resource):
    @swagger.operation(
        summary="Save a copy of the metadata into an audit folder",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier to create audit record for",
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
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        logger.info('Creating a new study audit folder for study %s', study_id)
        status, dest_path = write_audit_files(study_location)

        if status:
            return {'Success': 'Created audit record for ' + study_id}
        else:
            return {'Error': 'Failed to create audit folder ' + dest_path}


class CreateAccession(Resource):
    @swagger.operation(
        summary="Create a new study",
        notes='''Create a new empty, without upload folder''',
        parameters=[
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
                "message": "Unauthorized. Access to the resource requires user authentication."
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
    def post(self):

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None:
            abort(403)

        logger.info('Creating a new MTBLS Study')

        study_message = wsc.add_empty_study(user_token)
        data_dict = json.loads(study_message)
        study = data_dict["message"]
        logger.info('Created new study ' + study)

        study_path = app.config.get('STUDY_PATH')
        from_path = study_path + 'DUMMY'
        to_path = study_path + study

        try:
            copy_file(from_path, to_path)
        except:
            logger.error('Could not copy files from %s to %s', from_path, to_path)

        return {"new_study": study}


def write_audit_files(study_location):
    """
    Write back an ISA-API Investigation object directly into ISA-Tab files
    :param user_token: User API key for accession check
    :param study_id: Study accession number
    :param study_location: the filesystem where the study is located
    :return:
    """
    # dest folder name is a timestamp
    update_path_suffix = app.config.get('UPDATE_PATH_SUFFIX')
    update_path = os.path.join(study_location, update_path_suffix)
    dest_path = new_timestamped_folder(update_path)

    try:
        # make a copy of ISA-Tab & MAF
        for isa_file in glob.glob(os.path.join(study_location, "?_*.t*")):
            isa_file_name = os.path.basename(isa_file)
            src_file = isa_file
            dest_file = os.path.join(dest_path, isa_file_name)
            logger.info("Copying %s to %s", src_file, dest_file)
            copy_file(src_file, dest_file)
    except:
        return False, dest_path

    return True, dest_path


