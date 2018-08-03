from flask import request
from flask_restful import Resource, abort, reqparse
from app.ws.mm_models import *
from flask_restful_swagger import swagger
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from flask import current_app as app
import logging


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


def extended_response(data=None, errs=None, warns=None):
    ext_resp = {"data": data if data else list(),
                "errors": errs if errs else list(),
                "warnings": warns if warns else list()}
    return ext_resp


# res_path = /studies/<string:study_id>/assays
# http://host:500/mtbls/ws/studies/MTBLS10/assays?list_only=true
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
        list_only = True
        if request.args:
            args = parser.parse_args(req=request)
            list_only = False if args['list_only'].lower() != 'true' else True

        logger.info('Getting Assays for %s', study_id)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        # Using context to avoid envelop tags in contained objects
        sch = AssaySchema()
        sch.context['assay'] = Assay()
        logger.info('Got %s assays', len(isa_study.assays))
        if list_only:
            sch = AssaySchema(only=['filename'])
            sch.context['assay'] = Assay()
        return extended_response(sch.dump(isa_study.assays, many=True))


# res_path = /studies/<string:study_id>/assays/<string:assay_id>
# http://host:port/mtbls/ws/studies/MTBLS10/assays/1
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

        logger.info('Getting Assay %s for %s', assay_id, study_id)
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
        return extended_response(sch.dump(isa_assay))


# res_path = /studies/<string:study_id>/assays/<string:assay_id>/processSequence
# http://host:port/mtbls/ws/studies/MTBLS10/assays/1/processSequence?list_only=true
class AssayProcesses(Resource):

    @swagger.operation(
        summary="Get Assay Process Sequence",
        notes="""Get Assay Process Sequence.
                  <br>
                  Use process or protocol name as query parameter for specific searching.""",
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
                "name": "name",
                "description": "Process name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "prot_name",
                "description": "Protocol name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "list_only",
                "description": "List names only",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
            },
            {
                "name": "use_default_values",
                "description": "Provide default values when empty",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": False,
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
    def get(self, study_id, assay_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        if assay_id is None:
            abort(404)
        try:
            assay_num = int(assay_id) - 1
        except ValueError:
            abort(404)
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help='Assay Processes name')
        parser.add_argument('prot_name', help='Protocol name')
        parser.add_argument('list_only', help='List names only')
        parser.add_argument('use_default_values', help='Provide default values when empty')
        list_only = True
        obj_name = None
        prot_name = None
        use_default_values = False
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name'].lower() if args['name'] else None
            prot_name = args['prot_name'].lower() if args['prot_name'] else None
            list_only = False if args['list_only'].lower() != 'true' else True
            use_default_values = False if args['use_default_values'].lower() != 'true' else True

        logger.info('Getting Processes for Assay %s in %s', assay_id, study_id)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        if assay_num < 0 or \
                assay_num > len(isa_study.assays) - 1:
            abort(404)
        isa_assay = isa_study.assays[assay_num]
        obj_list = isa_assay.process_sequence
        found = list()
        warns = []
        if not obj_name and not prot_name:
            found = obj_list
        else:
            for index, proto in enumerate(obj_list):
                if proto.name.lower() == obj_name \
                        or proto.executes_protocol.name.lower() == prot_name:
                    found.append(proto)
        if not found:
            abort(404)
        logger.info('Found %d protocols', len(found))

        # use default values
        if use_default_values:
            set_default_proc_name(obj_list, warns)

            proc_list = get_first_process(obj_list)
            set_default_output(isa_assay, proc_list, warns)

        sch = ProcessSchema(many=True)
        if list_only:
            sch = ProcessSchema(only=('name', 'executes_protocol.name',
                                      'prev_process.executes_protocol.name',
                                      'next_process.executes_protocol.name'), many=True)
        return extended_response(data={'processSequence': sch.dump(found).data},
                                 warns=warns)


def set_default_output(isa_assay, proc_list, warns):
        for i, proc in enumerate(proc_list):
            # check Extraction outputs
            if proc.executes_protocol.name == 'Extraction':
                if not proc.outputs:
                    # take inputs from next process
                    if proc.next_process.inputs:
                        proc.outputs = proc.next_process.inputs
                        warns.append({'message': 'Using  ' + proc.next_process.name if proc.next_process.name else proc.next_process.executes_protocol.name + ' inputs' + ' as outputs for ' + proc.name})
                    # create from self inputs
                    elif proc.inputs:
                        # create output
                        for input in proc.inputs:
                            if isinstance(input, Sample):
                                extract = Extract(name=input.name + '_' + 'Extract',
                                                  comments=[{'name': 'Inferred',
                                                             'value': 'Value was missing in ISA-Tab, '
                                                                      'so building from Sample name.'}])
                                proc.outputs.append(extract)
                                isa_assay.other_material.append(extract)
                                warns.append({'message': 'Created new Extract ' + extract.name})


def set_default_proc_name(obj_list, warns):
    for i, proc in enumerate(obj_list):
        if not proc.name:
            proc.name = 'Process' + '_' + proc.executes_protocol.name
            warns.append({'message': 'Added name to Process ' + proc.name})


def get_first_process(proc_list):
    procs = list()
    for i, proc in enumerate(proc_list):
        print(i, proc.name, ' - ', proc.executes_protocol.name,
              ' -> ', proc.next_process.name, proc.next_process.executes_protocol.name)
        if not proc.prev_process:
            procs.append(proc)
    return procs


# res_path = /studies/<string:study_id>/assays/<string:assay_id>/sources
# http://host:port/mtbls/ws/studies/MTBLS10/assays/1/sources?list_only=true
class AssaySources(Resource):

    def get(self, study_id, assay_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        if assay_id is None:
            abort(404)
        try:
            assay_num = int(assay_id) - 1
        except ValueError:
            abort(404)
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help='Study Sample name')
        parser.add_argument('list_only', help='List names only')
        list_only = True
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name'].lower() if args['name'] else None
            list_only = False if args['list_only'].lower() != 'true' else True

        logger.info('Getting Assay Sources for %s in %s', assay_id, study_id)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        if assay_num < 0 or \
                assay_num > len(isa_study.assays) - 1:
            abort(404)
        isa_assay = isa_study.assays[assay_num]
        obj_list = isa_assay.sources
        # Using context to avoid envelop tags in contained objects
        sch = SourceSchema()
        sch.context['source'] = Source()
        if not obj_name:
            # return a list of objs
            logger.info('Got %s sources', len(obj_list))
            if list_only:
                sch = SourceSchema(only=['name'])
                sch.context['source'] = Source()
            return extended_response(sch.dump(obj_list, many=True))
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(obj_list):
                if obj.name.lower() == obj_name:
                    found = True
                    break
            if not found:
                abort(404)
            logger.info('Got %s', obj.name)
            return extended_response(sch.dump(obj))


# res_path = /studies/<string:study_id>/assays/<string:assay_id>/samples
# http://host:port/mtbls/ws/studies/MTBLS10/assays/1/samples?list_only=true
class AssaySamples(Resource):

    @swagger.operation(
        summary="Get Assay Samples",
        notes="""Get Assay Samples.""",
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
                "name": "name",
                "description": "Assay Sample name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "list_only",
                "description": "List names only",
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
    def get(self, study_id, assay_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        if assay_id is None:
            abort(404)
        try:
            assay_num = int(assay_id) - 1
        except ValueError:
            abort(404)
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help='Assay Sample name')
        parser.add_argument('list_only', help='List names only')
        list_only = True
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name'].lower() if args['name'] else None
            list_only = False if args['list_only'].lower() != 'true' else True

        logger.info('Getting Samples for Assay %s in %s', assay_id, study_id)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        if assay_num < 0 or \
                assay_num > len(isa_study.assays) - 1:
            abort(404)
        isa_assay = isa_study.assays[assay_num]
        obj_list = isa_assay.samples
        # Using context to avoid envelop tags in contained objects
        sch = SampleSchema()
        sch.context['sample'] = Sample()
        if not obj_name:
            # return a list of objs
            logger.info('Got %s samples', len(obj_list))
            if list_only:
                sch = SampleSchema(only=['name'])
                sch.context['sample'] = Sample()
            return extended_response(sch.dump(obj_list, many=True))
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(obj_list):
                if obj.name.lower() == obj_name:
                    found = True
                    break
            if not found:
                abort(404)
            logger.info('Got %s', obj.name)
            return extended_response(sch.dump(obj))


# res_path = /studies/<string:study_id>/assays/<string:assay_id>/otherMaterials
# http://host:port/mtbls/ws/studies/MTBLS10/assays/1/otherMaterials?list_only=true
class AssayOtherMaterials(Resource):

    @swagger.operation(
        summary="Get Assay Other Materials",
        notes="""Get Assay Other Materials.
                  <br>
                  Use sample name as a query parameter to filter out.""",
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
                "name": "name",
                "description": "Assay Material name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "list_only",
                "description": "List names only",
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
    def get(self, study_id, assay_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        if assay_id is None:
            abort(404)
        try:
            assay_num = int(assay_id) - 1
        except ValueError:
            abort(404)
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help='Assay Other Materials name')
        parser.add_argument('list_only', help='List names only')
        list_only = True
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name'].lower() if args['name'] else None
            list_only = False if args['list_only'].lower() != 'true' else True

        logger.info('Getting Other Materials for Assay %s in %s', assay_id, study_id)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        if assay_num < 0 or \
                assay_num > len(isa_study.assays) - 1:
            abort(404)
        isa_assay = isa_study.assays[assay_num]
        obj_list = isa_assay.other_material
        # Using context to avoid envelop tags in contained objects
        sch = OtherMaterialSchema()
        sch.context['other_material'] = Material()
        if not obj_name:
            # return a list of objs
            logger.info('Got %s Materials', len(obj_list))
            if list_only:
                sch = OtherMaterialSchema(only=['name'])
                sch.context['other_material'] = Material()
            return extended_response(sch.dump(obj_list, many=True))
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(obj_list):
                if obj.name.lower() == obj_name:
                    found = True
                    break
            if not found:
                abort(404)
            logger.info('Got %s', obj.name)
            return extended_response(sch.dump(obj))


# res_path = /studies/<string:study_id>/assays/<string:assay_id>/dataFiles
# http://host:port/mtbls/ws/studies/MTBLS2/assays/1/dataFiles?list_only=false
class AssayDataFiles(Resource):
    @swagger.operation(
        summary="Get Assay Data File",
        notes="""Get Assay Data File.
                  <br>
                  Use file name as query parameter for specific searching.""",
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
                "name": "name",
                "description": "Data File name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "list_only",
                "description": "List names only",
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
    def get(self, study_id, assay_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        if assay_id is None:
            abort(404)
        try:
            assay_num = int(assay_id) - 1
        except ValueError:
            abort(404)
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help='Assay Processes name')
        parser.add_argument('list_only', help='List names only')
        list_only = True
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name'].lower() if args['name'] else None
            list_only = False if args['list_only'].lower() != 'true' else True

        logger.info('Getting Data Files for Assay %s in %s', assay_id, study_id)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        if assay_num < 0 or \
                assay_num > len(isa_study.assays) - 1:
            abort(404)
        isa_assay = isa_study.assays[assay_num]
        obj_list = isa_assay.data_files
        found = list()
        if not obj_name:
            found = obj_list
        else:
            for index, obj in enumerate(obj_list):
                if obj.filename.lower() == obj_name :
                    found.append(obj)
        if not found:
            abort(404)
        logger.info('Found %d data files', len(found))

        sch = DataFileSchema(many=True)
        if list_only:
            sch = DataFileSchema(only=('filename',), many=True)
        return extended_response(data={'dataFiles': sch.dump(found).data})
