from flask import request, abort, send_file
from flask_restful import Resource
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import *
from app.ws.isaApiClient import IsaApiClient
from mzml2isa.parsing import convert
from lxml import etree

logger = logging.getLogger('wslog')
wsc = WsClient()
iac = IsaApiClient()


def convert_to_isa(study_id, input_folder, outout_folder):
    try:
        convert(input_folder, outout_folder, study_id)
    except(Exception):
        return False, {"Error": "Could not convert mzML to ISA-Tab study " + study_id}

    return True, {"success": "ISA-Tab files generated for study " + study_id}


def validate_xml(xsd, xml):

    xmlschema_doc = etree.parse(xsd)
    xmlschema = etree.XMLSchema(xmlschema_doc)

    # parse xml
    try:
        doc = etree.parse(xml)
        print('XML well formed, syntax ok.')

    # check for file IO error
    except IOError:
        return False, {"Error": "Can not read the file " + xml}

    # check for XML syntax errors
    except etree.XMLSyntaxError:
        return False, {"Error": "File " + xml + " is not a valid XML file"}

    # validate against schema
    try:
        xmlschema.assertValid(doc)
        print('XML valid, schema validation ok: ' + xml)
        return True, {"Success": "File " + xml + " is a valid XML file"}

    except etree.DocumentInvalid:
        print('Schema validation error, see error_schema.log')
        return False, {"Error": "Can not validate the file " + xml}


class Convert2ISAtab(Resource):
    @swagger.operation(
        summary="Convert mzML files into ISA-Tab",
        notes='''</P><B>Be aware that any ISA-Tab files will be overwritten for this study</P>
        This process can run for a long while, please be patient</B>''',
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study Identifier for generating new ISA-Tab files",
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

        input_folder = study_location
        outout_folder = study_location

        logger.info('Creating a new study upload folder for study %s', study_id)
        status, message = convert_to_isa(study_id, input_folder, outout_folder)
        if status:
            location = study_location
            files = glob.glob(os.path.join(location, 'i_Investigation.txt'))
            if files:
                file_path = files[0]
                filename = os.path.basename(file_path)
                try:
                    return send_file(file_path, cache_timeout=-1,
                                     as_attachment=True, attachment_filename=filename)
                except OSError as err:
                    logger.error(err)
                    abort(404, "Generated ISA-Tab i_Investigation.txt file could not be read.")
            else:
                abort(404, "Generated ISA-Tab i_Investigation.txt file could not be read.")
        else:
            return message


class ValidateMzML(Resource):
    @swagger.operation(
        summary="Validate mzML files",
        notes='''Validating mzML file structure. 
        This method will validate mzML files in both the study folder and the upload folder''',
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study Identifier with mzML files to validate",
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

        upload_location = app.config.get('MTBLS_FTP_ROOT') + study_id.lower() + "-" + obfuscation_code

        result = {"Success": "All mzML files validated"}

        # Getting xsd schema for validation
        items = app.config.get('MZML_XSD_SCHEMA')
        xsd_name = items[0]
        script_loc = items[1]

        for file_loc in [upload_location, study_location]:  # Check both study and upload location

            if os.path.isdir(file_loc):  # Only check if the folder exists
                files = glob.glob(os.path.join(file_loc, '*.mzML'))  # Are there mzML files there?
                for file in files:
                    try:
                        status, result = validate_xml(os.path.join(script_loc, xsd_name), file)
                        if not status:
                            return result
                    except Exception:
                        return result

        return result
