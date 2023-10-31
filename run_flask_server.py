import logging.config
from wsapp import application
from app.config import get_settings
logger = logging.getLogger('wslog')

logger.info("Starting server %s v%s", get_settings().server.description.ws_app_name,
            get_settings().server.description.ws_app_version)
print("Starting application on port %s" % str(get_settings().server.service.rest_api_port))
application.run(host="0.0.0.0", port=get_settings().server.service.rest_api_port, debug=get_settings().flask.DEBUG,
                threaded=True, use_reloader=False)
logger.info("Finished server %s v%s", get_settings().server.description.ws_app_name,
           get_settings().server.description.ws_app_version)


