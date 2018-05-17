from flask import request
from flask_restful import Resource, abort, reqparse
from app.ws.mm_models import *
from flask_restful_swagger import swagger
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from flask import current_app as app


logger = logging.getLogger('wslog')
iac = IsaApiClient()
wsc = WsClient()


# Allow for a more detailed logging when on DEBUG mode
def log_request(request_obj):
    if app.config.get('DEBUG'):
        if app.config.get('DEBUG_LOG_HEADERS'):
            logger.debug('REQUEST HEADERS -> %s', request_obj.headers)
        if app.config.get('DEBUG_LOG_BODY'):
            logger.debug('REQUEST BODY    -> %s', request_obj.data)
        if app.config.get('DEBUG_LOG_JSON'):
            logger.debug('REQUEST JSON    -> %s', request_obj.json)


class StudyAssays(Resource):

    @swagger.operation(
        summary="Get Study Assays",
        notes="""Get Study Assay list.""",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "list_only",
                "description": "List filenames only",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
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
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('list_only', help='List filenames only')
        list_only = None
        if request.args:
            args = parser.parse_args(req=request)
            list_only = args['list_only']

        logger.info('Getting Assays for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        # Using context to avoid envelop tags in contained objects
        sch = AssaySchema()
        sch.context['assay'] = Assay()
        logger.info('Got %s assays', len(isa_study.assays))
        if list_only in ['true', 'True']:
            sch = AssaySchema(only=['filename'])
            sch.context['assay'] = Assay()
        return sch.dump(isa_study.assays, many=True)


class StudyAssay(Resource):

    @swagger.operation(
        summary="Get Study Assay",
        notes="""Get Study Assay.""",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "assay_id",
                "description": "Assay number",
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
                "required": False,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
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
    def get(self, study_id, assay_id):
        # param validation
        if study_id is None:
            abort(404)
        if study_id is None:
            abort(404)
        try:
            assay_num = int(assay_id) - 1
        except ValueError:
            abort(404)
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']

        logger.info('Getting Assay %s for %s, using API-Key %s', assay_id, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        if assay_num < 0 or \
                assay_num > len(isa_study.assays) - 1:
            abort(404)
        isa_assay = isa_study.assays[assay_num]
        # Using context to avoid envelop tags in contained objects
        sch = AssaySchema()
        sch.context['assay'] = Assay()
        logger.info('Got %s', isa_assay.filename)
        return sch.dump(isa_assay)
