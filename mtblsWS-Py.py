from flask import Flask
from flask_restful import Api, Resource
from flask_restful_swagger import swagger
import config


class About(Resource):
    """Basic description of the Web Service"""
    @swagger.operation(
        summary='About this WS',
        notes='Basic description of the Web Service',
        nickname='about',
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            }
        ]
    )
    def get(self):
        return {'WS name': config.APP_NAME,
                'API': {
                    'version': config.APP_VERSION,
                    'documentation': config.APP_BASE_LINK + config.API_DOC + '.html',
                    'specification': config.APP_BASE_LINK + config.API_DOC + '.json',
                },
                'URL': config.APP_BASE_LINK + config.RESOURCES_PATH,
                }


app = Flask(__name__)
app.config.from_object(config)

api = swagger.docs(Api(app),
                   apiVersion=config.APP_VERSION,
                   basePath=config.APP_BASE_LINK,
                   api_spec_url=config.API_DOC,
                   resourcePath=config.RESOURCES_PATH)

api.add_resource(About, config.RESOURCES_PATH)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=config.PORT, debug=config.DEBUG)
