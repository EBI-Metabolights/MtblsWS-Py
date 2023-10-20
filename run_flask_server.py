#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-May-23
#  Modified by:   kenneth
#
#  Copyright 2019 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

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


