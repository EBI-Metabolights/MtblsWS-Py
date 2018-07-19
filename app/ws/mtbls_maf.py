import logging, json, pandas as pd, numpy as np
from flask import request, abort, jsonify
from flask_restful import Resource
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient

"""
MTBLS MAF

Manage the metabolite annotation files (MAF) from a MTBLS studies.  MAF JSON obtained from the Java-based MTBLS WS
"""

logger = logging.getLogger('wslog')
# MetaboLights (Java-Based) WebService client
wsc = WsClient()


class MtblsMAF(Resource):
    """Get MAF from studies (assays)"""
    @swagger.operation(
        summary="Get MAF for MTBLS Assay",
        nickname="Get MAF for MTBLS Assay",
        notes="Get a given MAF associated with assay {assay_id} for a MTBLS Study with {study_id} in JSON format",
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
                "description": "MTBLS Assay Identifier",
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
                "message": "OK. The MAF is returned, JSON format."
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
        """
        Get MAF from from MetaboLights WS
        :param study_id: MTBLS study identifier
        :param assay_id: The number of the assay for the given study_id
        :return: a JSON representation of the MTBLS Study object
        """

        # param validation
        if study_id is None:
            abort(404)

        if assay_id is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('Getting MAF from Assay %s from MTBLS Study %s, using API-Key %s', assay_id, study_id, user_token)
        maf = wsc.get_study_maf(study_id, assay_id, user_token)
        return maf


class ReadMaf(Resource):
    """Get MAF from filesystem"""
    @swagger.operation(
        summary="Get MAF for a MTBLS study using filename",
        nickname="Get MAF for MTBLS Assay",
        notes="Get a given Metabolite Annotation File for a MTBLS Study with in JSON format.",
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
                "name": "file_name",
                "description": "Metabolite Annotation File name",
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
                "message": "OK. The MAF is returned, JSON format."
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
    def get(self, study_id, file_name):
        """
        Get MAF from from MetaboLights API
        :param study_id: MTBLS study identifier
        :param assay_id: The number of the assay for the given study_id
        :return: a JSON representation of the MTBLS Study object
        """

        # param validation
        if study_id is None:
            abort(404)

        if file_name is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('MAF: Getting ISA-JSON Study %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)

        study_path = wsc.get_study_location(study_id, user_token)
        #path = '/nfs/public/rw/homes/tc_cm01/metabolights/dev/studies/MTBLS1'
        df = pd.read_csv(study_path + "/" + file_name, sep="\t", header=0, encoding='utf-8')
        df1 = df.replace(np.nan, '', regex=True)
        df_dict = self.totuples(df1.reset_index())
        return df_dict


    # Convert panda DataFrame to json tuples object
    def totuples(self, df):
        d = [
            dict([
                (colname, row[i])
                for i, colname in enumerate(df.columns)
            ])
            for row in df.values
        ]
        return {'mafdata': d}
