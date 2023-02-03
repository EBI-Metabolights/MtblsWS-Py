import logging
from flask import request, current_app as app, jsonify
from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from app.utils import MetabolightsException, metabolights_exception_handler, MetabolightsDBException
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import RefMetabolite
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

        with DBManager.get_instance(app).session_maker() as db_session:
            accs = db_session.query(RefMetabolite.acc).all()
            acc_list = []
            for acc in accs:
                acc_list.append(''.join(acc))

        result = {'content': acc_list, 'message': None, "err": None}
        return result


class MtblsCompoundsDetails(Resource):
    @swagger.operation(
        summary="Get details for a Metabolights Compound",
        parameters=[
            {
                "name": "accession",
                "description": "Study Identifier",
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

        with DBManager.get_instance(app).session_maker() as db_session:
            metabolite = db_session.query(RefMetabolite).filter(RefMetabolite.acc == accession).first()

            if not metabolite:
                raise MetabolightsDBException(f"{accession} does not exist")

            metabo_lights = models.MetaboLightsModel.from_orm(metabolite)
            dict_date = metabo_lights.dict()

        result = {'content': dict_date, 'message': None, "err": None}
        return result

    def validate_requested_accession(self, requested_acc):
        compound_id_prefix = app.config.get("MTBLS_COMPOUND_ID_PREFIX")
        if not requested_acc.startswith(compound_id_prefix):
            raise MetabolightsException(f"Passed accession :- {requested_acc} is invalid. Accession must start with %s" % compound_id_prefix)