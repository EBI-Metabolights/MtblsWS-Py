import logging
import os
from flask import request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from app.tasks.common_tasks.basic_tasks.elasticsearch import delete_compound_index, reindex_all_compounds, reindex_compound
from app.tasks.common_tasks.admin_tasks.es_and_db_compound_synchronization import sync_compound_on_es_and_db
from app.utils import MetabolightsException, metabolights_exception_handler, MetabolightsDBException
from flask import send_file, make_response
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import RefMetabolite
from app.ws.settings.utils import get_study_settings
from app.ws.study.user_service import UserService
from app.services.external.eb_eye_search import EbEyeSearchService
from app.tasks.common_tasks.report_tasks.eb_eye_search import eb_eye_build_compounds
from app.ws.utils import log_request
from app.ws.db import models

logger = logging.getLogger('wslog')


class MtblsCompounds(Resource):
    @swagger.operation(
        summary="Get all compounds list - accession number",
        notes="Get a list of all studies for a user. This also includes the status, release date, title and abstract",
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

        logger.info('Getting  All Compound IDs ')

        with DBManager.get_instance().session_maker() as db_session:
            accs = db_session.query(RefMetabolite.acc).all()
            acc_list = []
            for acc in accs:
                acc_list.append(''.join(acc))

        result = {'content': acc_list, 'message': None, "err": None}
        return result

class EbEyeCompounds(Resource):
    @swagger.operation(
        summary="Export Metabolights Compound for EB Eye",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "accession",
                "description": "Compound Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            }
        ],
        responseMessages=[
        {"code": 200, "message": "OK."},
        {"code": 401, "message": "Unauthorized. Access to the resource requires user authentication."},
        {"code": 403, "message": "Forbidden. Access to the study is not allowed for this user."},
        {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."}
        ]
    )
    @metabolights_exception_handler
    def get(self, accession):
        log_request(request)
        # param validation
        if accession is None:
            abort(401, message="Missing accession")
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_curator_role(user_token)

        accession = accession.upper()
        compounds_details = MtblsCompoundsDetails()
        compounds_details.validate_requested_accession(accession)

        logger.info(f'Getting EB EYE export for Compound  {accession}')
        doc = EbEyeSearchService.get_compound(compound_acc=accession)
        xml_str = doc.toprettyxml(indent="  ")                                      
        response = make_response(xml_str)                                           
        response.headers['Content-Type'] = 'text/xml; charset=utf-8' 
        return response
    
    
class EbEyeCompoundsAll(Resource):
    @swagger.operation(
        summary="Export Metabolights Compound for EB Eye",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=[
        {"code": 200, "message": "OK."},
        {"code": 401, "message": "Unauthorized. Access to the resource requires user authentication."},
        {"code": 403, "message": "Forbidden. Access to the study is not allowed for this user."},
        {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."}
        ]
    )
    @metabolights_exception_handler
    def get(self):
        log_request(request)
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_curator_role(user_token)
        inputs = {"user_token": user_token}
        task = eb_eye_build_compounds.apply_async(kwargs=inputs, expires=60*5)
        response = {'Task started':f'Task id {task.id}'}
        return response
    
    
class MtblsCompoundsDetails(Resource):
    @swagger.operation(
        summary="Get details for a Metabolights Compound",
        parameters=[
            {
                "name": "accession",
                "description": "Compound Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
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
    @metabolights_exception_handler
    def get(self, accession):
        log_request(request)
        # param validation
        if accession is None:
            abort(401, message="Missing accession")

        accession = accession.upper()
        self.validate_requested_accession(accession)

        logger.info('Getting Compound details for accession number  %s', accession)

        with DBManager.get_instance().session_maker() as db_session:
            metabolite = db_session.query(RefMetabolite).filter(RefMetabolite.acc == accession).first()

            if not metabolite:
                raise MetabolightsDBException(f"{accession} does not exist")

            metabo_lights = models.MetaboLightsCompoundModel.model_validate(metabolite)
            dict_date = metabo_lights.model_dump()

        result = {'content': dict_date, 'message': None, "err": None}
        return result

    def validate_requested_accession(self, requested_acc):
        compound_id_prefix = "MTBLC"
        if not requested_acc.startswith(compound_id_prefix):
            raise MetabolightsException(f"Passed accession :- {requested_acc} is invalid. Accession must start with %s" % compound_id_prefix)


class MtblsCompoundFile(Resource):
    @swagger.operation(
        summary="Get compound details file",
        notes="Get compound details file",
        parameters=[
            {
                "name": "accession",
                "description": "Compound Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The compound is returned"
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
    @metabolights_exception_handler
    def get(self, accession):
        log_request(request)
        if not accession:
            logger.info('No compound_id given')
            abort(404)
        compound_id = accession.upper()

        study_settings = get_study_settings()
        compound_file_path = os.path.join(study_settings.mounted_paths.compounds_root_path, compound_id, compound_id + "_data.json")
    
        if os.path.exists(compound_file_path):
            resp = make_response(send_file(compound_file_path))
            resp.headers['Content-Type'] = 'application/json'
            return resp
        else:
            raise MetabolightsException(http_code=400, message="invalid compound file")



class MtblsCompoundSpectraFile(Resource):
    @swagger.operation(
        summary="Get compound spectra file",
        notes="Get compound spectra file",
        parameters=[
            {
                "name": "accession",
                "description": "Compound Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
                        
            {
                "name": "spectra_id",
                "description": "Spectra id",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The compound is returned"
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
    @metabolights_exception_handler
    def get(self, accession, spectra_id):
        log_request(request)
        if not accession or not spectra_id:
            logger.info('No compound_id or spectra_id given')
            abort(404)
        compound_id = accession.upper()

        settings = get_study_settings()
        spectrum_path = os.path.join(settings.mounted_paths.compounds_root_path, compound_id, compound_id + "_spectrum")
        specra_file_path = os.path.join(spectrum_path, spectra_id, spectra_id + ".json")
    
        if os.path.exists(specra_file_path):
            resp = make_response(send_file(specra_file_path))
            resp.headers['Content-Type'] = 'application/json'
            return resp
        else:
            raise MetabolightsException(http_code=400, message="invalid spectra file")
        
class MtblsCompoundIndex(Resource):
    @swagger.operation(
        summary="Index a compound ",
        notes="Index a compound and return indexed data",
        parameters=[
            {
                "name": "accession",
                "description": "Compound Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "user-token",
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
                "message": "OK. The compound is returned"
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
    @metabolights_exception_handler
    def post(self, accession):
        log_request(request)
        if not accession:
            logger.info('No compound_id given')
            abort(404)
        compound_id = accession.upper()

        # User authentication
        user_token = ''
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
            
        logger.info('Indexing a compound')

        result = reindex_compound(user_token, compound_id)

        result = {'content': result, 'message': None, "err": None}
        return result

    @swagger.operation(
        summary="Index a compound ",
        notes="Index a compound and return indexed data",
        parameters=[
            {
                "name": "accession",
                "description": "Compound Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "user-token",
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
                "message": "OK. The compound is returned"
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
    @metabolights_exception_handler
    def delete(self, accession):
        log_request(request)
        if not accession:
            logger.info('No compound_id given')
            abort(404)
        compound_id = accession.upper()

        # User authentication
        user_token = ''
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
            
        logger.info('Deleting a compound')

        result = delete_compound_index(user_token, compound_id)

        result = {'content': result, 'message': None, "err": None}
        return result

class MtblsCompoundIndexAll(Resource):
    @swagger.operation(
        summary="Index all compounds ",
        notes="Start a task to index all compound and return task id. Result will be sent by email.",
        parameters=[
            {
                "name": "user-token",
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
                "message": "OK. The compound is returned"
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
    @metabolights_exception_handler
    def post(self):
        log_request(request)
        

        # User authentication
        user_token = ''
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        
        try:
            logger.info('Indexing all compounds')
            inputs = {"user_token": user_token, "send_email_to_submitter": True}
            result = reindex_all_compounds.apply_async(kwargs=inputs, expires=60*5)

            result = {'content': f"Task has been started. Result will be sent by email. Task id: {result.id}", 'message': None, "err": None}
            return result
        except Exception as ex:
            raise MetabolightsException(http_code=500, message=f"Reindex task submission was failed", exception=ex)
        
        

class MtblsCompoundIndexSync(Resource):
    @swagger.operation(
        summary="Sync compounds on MetaboLights database and elasticsearch",
        notes="Start a task to sync compounds on database and elasticsearch and return task id. Result will be sent by email.",
        parameters=[
            {
                "name": "user-token",
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
                "message": "OK. The compound is returned"
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
    @metabolights_exception_handler
    def post(self):
        log_request(request)
        

        # User authentication
        user_token = ''
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        
        try:
            inputs = {"user_token": user_token, "send_email_to_submitter": True }
            
            result = sync_compound_on_es_and_db.apply_async(kwargs=inputs, expires=60*5)

            result = {'content': f"Task has been started. Result will be sent by email. Task id: {result.id}", 'message': None, "err": None}
            return result
        except Exception as ex:
            raise MetabolightsException(http_code=500, message=f"Sync all compounds task submission was failed", exception=ex)