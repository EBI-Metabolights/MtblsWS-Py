import logging
import os
import random
from zipfile import ZipFile

from flask import request, send_file, make_response
from flask_restful import Resource, reqparse, abort
from flask_restful_swagger import swagger
from app.utils import metabolights_exception_handler
from app.ws.db.schemes import Study

from app.ws.mtblsWSclient import WsClient
from app.ws.settings.utils import get_study_settings
from app.ws.study.folder_utils import get_basic_files
from app.ws.study.study_service import StudyService, identify_study_id

import os
import ftplib
from contextlib import closing

logger = logging.getLogger('wslog')
# MetaboLights (Java-Based) WebService client
wsc = WsClient()


class DownloadDataFile(Resource):
    @swagger.operation(
        summary="Download data file from server",
        notes="Download/Stream data files from study folder",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS accession",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "file_path",
                "description": "Relate files path",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "user-token",
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
    def get(self, study_id):
        # param validation
        if study_id is None:
            logger.info('No study_id given')
            abort(404)
        study_id = study_id.upper()

        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            user_token = "public_access_only"
  
        file_path = None

        if request.args:
            file_path = request.args.get('file_path') if request.args.get('file_path') else None

        if file_path is None:
            logger.info('No file name given')
            abort(404)


        settings = get_study_settings()
        
        with closing(ftplib.FTP()) as ftp:
            try:
                complete_file_path = f"/pub/databases/metabolights/studies/public/{study_id}/{file_path}"
                head, tail = os.path.split(file_path)
                local_filename = f'/tmp/{tail}'
                ftp.connect('ftp.ebi.ac.uk', 21, 30*5) #5 mins timeout
                ftp.login('', '')
                ftp.set_pasv(True)
                with open(local_filename, 'w+b') as f:
                    res = ftp.retrbinary('RETR %s' % complete_file_path, f.write)

                    if not res.startswith('226 Transfer complete'):
                        print('Downloaded of file {0} is not compile.'.format(complete_file_path))
                        os.remove(local_filename)
                        return None

                resp = make_response(send_file(local_filename, as_attachment=True, download_name=local_filename, max_age=0))
                resp.headers["Content-Disposition"] = "attachment; filename={}".format(local_filename)
                #resp.headers['Content-Type'] = 'application/octet-stream'
                os.remove(local_filename)
                return resp
            except Exception as e:
                abort(501, message="Error while downloading from FTP " + str(e))
