from flask import request, abort
from flask_restful import Resource, reqparse
from marshmallow import ValidationError
from app.ws.mm_models import *
from flask_restful_swagger import swagger
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from flask import current_app as app
import logging
import json

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


def get_assay(assay_list, filename):
    for indx, assay in enumerate(assay_list):
        if assay.filename.lower() == filename:
            return assay


def get_source(source_list, source_name):
    for source in source_list:
        if source.name.lower() == source_name.lower():
            return source
    return None


def get_sample(sample_list, sample_name):
    for sample in sample_list:
        if sample.name.lower() == sample_name.lower():
            return sample
    return None


def get_protocol(protocol_list, protocol_name):
    for protocol in protocol_list:
        if protocol.name.lower() == protocol_name.lower():
            return protocol
    return None


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
                "name": "filename",
                "description": "Assay filename",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
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
        parser.add_argument('filename', help='Assay filename')
        filename = None
        parser.add_argument('list_only', help='List names only')
        list_only = True
        if request.args:
            args = parser.parse_args(req=request)
            filename = args['filename'].lower() if args['filename'] else None
            list_only = True if args['list_only'].lower() == 'true' else False

        logger.info('Getting Assay %s for %s', filename, study_id)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        obj_list = isa_study.assays
        found = list()
        warns = []
        if not filename:
            found = obj_list
        else:
            assay = get_assay(obj_list, filename)
            if assay:
                found.append(assay)
        if not found:
            abort(404)
        logger.info('Found %d assays', len(found))

        sch = AssaySchema(many=True)
        if list_only:
            sch = AssaySchema(only=('filename',), many=True)
        return extended_response(data={'assays': sch.dump(found).data})


class AssayProcesses(Resource):

    @swagger.operation(
        summary="Get Assay Process Sequence",
        notes="""Get Assay Process Sequence.
                  <br>
                  Use assay filename, process or protocol name to filter results.""",
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
                "name": "assay_filename",
                "description": "Assay filename",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "process_name",
                "description": "Process name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "protocol_name",
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
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('assay_filename', help='Assay filename')
        assay_filename = None
        parser.add_argument('process_name', help='Assay Processes name')
        process_name = None
        parser.add_argument('protocol_name', help='Protocol name')
        protocol_name = None
        parser.add_argument('list_only', help='List names only')
        list_only = True
        parser.add_argument('use_default_values', help='Provide default values when empty')
        use_default_values = False
        if request.args:
            args = parser.parse_args(req=request)
            assay_filename = args['assay_filename'].lower() if args['assay_filename'] else None
            process_name = args['process_name'].lower() if args['process_name'] else None
            protocol_name = args['protocol_name'].lower() if args['protocol_name'] else None
            list_only = True if args['list_only'].lower() == 'true' else False
            use_default_values = True if args['use_default_values'].lower() == 'true' else False

        logger.info('Getting Processes for Assay %s in %s', assay_filename, study_id)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        assay_list = list()
        warns = []
        if not assay_filename:
            assay_list = isa_study.assays
            warns.append({'message': 'No Assay filename provided, so merging ProcessSequence for all assays.'})
        else:
            assay = get_assay(isa_study.assays, assay_filename)
            if assay:
                assay_list.append(assay)
        if not assay_list:
            abort(404)

        found = list()
        for assay in assay_list:
            process_list = assay.process_sequence
            if not process_name and not protocol_name:
                found = process_list
            else:
                for index, proto in enumerate(process_list):
                    if proto.name.lower() == process_name or \
                            proto.executes_protocol.name.lower() == protocol_name:
                        found.append(proto)
            if not found:
                abort(404)
            logger.info('Found %d protocols', len(assay_list))

            # use default values
            if use_default_values:
                set_default_proc_name(process_list, warns)

                proc_list = get_first_process(process_list)
                set_default_output(assay, proc_list, warns)

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
                        warns.append({'message': 'Using ' + (proc.next_process.name if proc.next_process.name else proc.next_process.executes_protocol.name) + ' inputs' + ' as outputs for ' + proc.name})
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
        if not proc.prev_process:
            procs.append(proc)
    return procs


class AssaySamples(Resource):

    @swagger.operation(
        summary="Get Assay Samples",
        notes="""Get Assay Samples.
                  <br>
                  Use assay filename or sample name to filter results.""",
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
                "name": "assay_filename",
                "description": "Assay filename",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
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
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('assay_filename', help='Assay filename')
        assay_filename = None
        parser.add_argument('name', help='Assay Sample name')
        sample_name = None
        parser.add_argument('list_only', help='List names only')
        list_only = True
        if request.args:
            args = parser.parse_args(req=request)
            assay_filename = args['assay_filename'].lower() if args['assay_filename'] else None
            sample_name = args['name'].lower() if args['name'] else None
            list_only = True if args['list_only'].lower() == 'true' else False

        logger.info('Getting Samples for Assay %s in %s', assay_filename, study_id)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        assay_list = list()
        warns = []
        if not assay_filename:
            assay_list = isa_study.assays
            warns.append({'message': 'No Assay filename provided, so merging Samples for all assays.'})
        else:
            assay = get_assay(isa_study.assays, assay_filename)
            if assay:
                assay_list.append(assay)
        if not assay_list:
            abort(404)

        found = list()
        for assay in assay_list:
            obj_list = assay.samples
            if not sample_name:
                found = obj_list
            else:
                for index, obj in enumerate(obj_list):
                    if obj.name.lower() == sample_name:
                        found.append(obj)
            if not found:
                abort(404)
            logger.info('Found %d Materials', len(assay_list))

        sch = SampleSchema(many=True)
        if list_only:
            sch = SampleSchema(only=('name',), many=True)
        return extended_response(data={'samples': sch.dump(found).data}, warns=warns)


    @swagger.operation(
        summary='Update Assay Samples',
        notes="""Update a list of Assay Samples. Only existing Samples will be updated, unknown will be ignored. 
        To change name, only one sample can be processed at a time.""",
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
                "name": "assay_filename",
                "description": "Assay filename",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "name",
                "description": "Assay Sample name. Leave empty if updating more than one sample.",
                "required": False,
                "allowEmptyValue": True,
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
            },
            {
                "name": "samples",
                "description": 'Assay Sample list in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
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
                "name": "save_audit_copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
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
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('assay_filename', help='Assay filename')
        assay_filename = None
        parser.add_argument('name', help='Assay Sample name')
        sample_name = None
        parser.add_argument('list_only', help='List names only')
        list_only = True
        if request.args:
            args = parser.parse_args(req=request)
            assay_filename = args['assay_filename'].lower() if args['assay_filename'] else None
            sample_name = args['name'].lower() if args['name'] else None
            list_only = True if args['list_only'].lower() == 'true' else False
        if not assay_filename:
            logger.warning("Missing Assay filename.")
            abort(400)

        # header content validation
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and \
                request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        # body content validation
        sample_list = list()
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['samples']
            # if partial=True missing fields will be ignored
            result = SampleSchema().load(data, many=True, partial=False)
            sample_list = result.data
            if len(sample_list) == 0:
                logger.warning("No valid data provided.")
                abort(400)
        except (ValidationError, Exception) as err:
            logger.warning("Bad format JSON request.", err)
            abort(400, err)

        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        logger.info('Updating Samples for Assay %s in %s', assay_filename, study_id)
        assay = get_assay(isa_study.assays, assay_filename)
        if not assay:
            abort(404)

        logger.info('Updating Study Samples details for %s in %s,', assay_filename, study_id)
        updated_samples = list()
        if sample_name:
            if len(sample_list) > 1:
                logger.warning("Requesting name update for more than one sample")
                abort(400, "Requesting name update for more than one sample")
            sample = sample_list[0]
            if self.update_sample(isa_study, sample_name, sample):
                updated_samples.append(sample)
        else:
            for i, sample in enumerate(sample_list):
                if self.update_sample(isa_study, sample.name.lower(), sample):
                    updated_samples.append(sample)

        # check if all samples were updated
        warns = ''
        if len(updated_samples) != len(sample_list):
            warns = 'Some of the samples were not updated. ' \
                    'Updated ' + str(len(updated_samples)) + ' out of ' + str(len(sample_list))
            logger.warning(warns)

        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path,
                            save_investigation_copy=save_audit_copy,
                            save_samples_copy=save_audit_copy, save_assays_copy=save_audit_copy)

        sch = SampleSchema(many=True)
        if list_only:
            sch = SampleSchema(only=('name',), many=True)
        return extended_response(data={'samples': sch.dump(updated_samples).data}, warns=warns)

    def update_sample(self, isa_study, sample_name, new_sample):

        for i, sample in enumerate(isa_study.samples):
            if sample.name.lower() == sample_name:
                isa_study.samples[i].name = new_sample.name
                isa_study.samples[i].characteristics = new_sample.characteristics
                isa_study.samples[i].derives_from = new_sample.derives_from
                isa_study.samples[i].factor_values = new_sample.factor_values
                isa_study.samples[i].comments = new_sample.comments

        for i, process in enumerate(isa_study.process_sequence):
            for ii, sample in enumerate(process.outputs):
                if isinstance(sample, Sample) and sample.name.lower() == sample_name:
                    isa_study.process_sequence[i].outputs[ii].name = new_sample.name
                    isa_study.process_sequence[i].outputs[ii].characteristics = new_sample.characteristics
                    isa_study.process_sequence[i].outputs[ii].factor_values = new_sample.factor_values
                    isa_study.process_sequence[i].outputs[ii].derives_from = new_sample.derives_from
                    isa_study.process_sequence[i].outputs[ii].comments = new_sample.comments

        for isa_assay in isa_study.assays:
            for i, sample in enumerate(isa_assay.samples):
                if sample.name.lower() == sample_name:
                    isa_assay.samples[i].name = new_sample.name
                    isa_assay.samples[i].characteristics = new_sample.characteristics
                    isa_assay.samples[i].derives_from = new_sample.derives_from
                    isa_assay.samples[i].factor_values = new_sample.factor_values
                    isa_assay.samples[i].comments = new_sample.comments

        for i, process in enumerate(isa_assay.process_sequence):
            for ii, sample in enumerate(process.inputs):
                if isinstance(sample, Sample) and sample.name.lower() == sample_name:
                    isa_assay.process_sequence[i].inputs[ii].name = new_sample.name
                    isa_assay.process_sequence[i].inputs[ii].characteristics = new_sample.characteristics
                    isa_assay.process_sequence[i].inputs[ii].factor_values = new_sample.factor_values
                    isa_assay.process_sequence[i].inputs[ii].derives_from = new_sample.derives_from
                    isa_assay.process_sequence[i].inputs[ii].comments = new_sample.comments

                    logger.info('Updated sample: %s', new_sample.name)
                    return True
        return False


class AssayOtherMaterials(Resource):

    @swagger.operation(
        summary="Get Assay Other Materials",
        notes="""Get Assay Other Materials.
                  <br>
                  Use assay filename or material name to filter results.""",
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
                "name": "assay_filename",
                "description": "Assay filename",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
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
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('assay_filename', help='Assay filename')
        assay_filename = None
        parser.add_argument('name', help='Assay Other Materials name')
        obj_name = None
        parser.add_argument('list_only', help='List names only')
        list_only = True
        if request.args:
            args = parser.parse_args(req=request)
            assay_filename = args['assay_filename'].lower() if args['assay_filename'] else None
            obj_name = args['name'].lower() if args['name'] else None
            list_only = True if args['list_only'].lower() == 'true' else False

        logger.info('Getting Other Materials for Assay %s in %s', assay_filename, study_id)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        assay_list = list()
        warns = []
        if not assay_filename:
            assay_list = isa_study.assays
            warns.append({'message': 'No Assay filename provided, so merging Other Materials for all assays.'})
        else:
            assay = get_assay(isa_study.assays, assay_filename)
            if assay:
                assay_list.append(assay)
        if not assay_list:
            abort(404)

        found = list()
        for assay in assay_list:
            obj_list = assay.other_material
            if not obj_name:
                found = obj_list
            else:
                for index, obj in enumerate(obj_list):
                    if obj.name.lower() == obj_name:
                        found.append(obj)
            if not found:
                abort(404)
            logger.info('Found %d Materials', len(assay_list))

        sch = OtherMaterialSchema(many=True)
        if list_only:
            sch = OtherMaterialSchema(only=('name',), many=True)
        return extended_response(data={'otherMaterials': sch.dump(found).data}, warns=warns)


class AssayDataFiles(Resource):
    @swagger.operation(
        summary="Get Assay Data File",
        notes="""Get Assay Data File.
                  <br>
                  Use filename as query parameter for specific searching.""",
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
                "name": "assay_filename",
                "description": "Assay filename",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "data_filename",
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
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('assay_filename', help='Assay filename')
        assay_filename = None
        parser.add_argument('data_filename', help='Assay Data File name')
        data_filename = None
        parser.add_argument('list_only', help='List names only')
        list_only = True
        if request.args:
            args = parser.parse_args(req=request)
            assay_filename = args['assay_filename'].lower() if args['assay_filename'] else None
            data_filename = args['data_filename'].lower() if args['data_filename'] else None
            list_only = True if args['list_only'].lower() == 'true' else False

        logger.info('Getting Data Files for Assay %s in %s', assay_filename, study_id)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        assay_list = list()
        warns = []
        if not assay_filename:
            assay_list = isa_study.assays
            warns.append({'message': 'No Assay filename provided, so merging Data files for all assays.'})
        else:
            assay = get_assay(isa_study.assays, assay_filename)
            if assay:
                assay_list.append(assay)
        if not assay_list:
            abort(404)

        found = list()
        for assay in assay_list:
            datafile_list = assay.data_files
            if not data_filename:
                found = datafile_list
            else:
                for index, obj in enumerate(datafile_list):
                    if obj.filename.lower() == data_filename :
                        found.append(obj)
            if not found:
                abort(404)
            logger.info('Found %d data files', len(assay_list))

        sch = DataFileSchema(many=True)
        if list_only:
            sch = DataFileSchema(only=('filename',), many=True)
        return extended_response(data={'dataFiles': sch.dump(found).data}, warns=warns)
