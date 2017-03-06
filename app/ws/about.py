import config
from flask_restful import Resource
from flask_restful_swagger import swagger

"""
MtblsWS-Py About

Basic description of the Web Service

author: jrmacias@ebi.ac.uk
date: 2017-03-06
"""


class About(Resource):
    """Basic description of the Web Service"""
    @swagger.operation(
        summary="About this Web Service",
        notes="Basic description of the Web Service",
        nickname="about",
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            }
        ]
    )
    def get(self):
        return {"WS name": config.WS_APP_NAME,
                "WS description": config.WS_APP_DESCRIPTION,
                "API": {
                    "version": config.API_VERSION,
                    "documentation": config.WS_APP_BASE_LINK + config.API_DOC + ".html",
                    "specification": config.WS_APP_BASE_LINK + config.API_DOC + ".json",
                },
                "URL": config.WS_APP_BASE_LINK + config.RESOURCES_PATH,
                }
