MTBLS Python-based REST service
================


Branch: Master
[![Build Status](https://travis-ci.org/EBI-Metabolights/MtblsWS-Py.svg?branch=master)](https://travis-ci.org/EBI-Metabolights/MtblsWS-Py) [![Coverage Status](https://coveralls.io/repos/github/EBI-Metabolights/MtblsWS-Py/badge.svg?branch=master)](https://coveralls.io/github/EBI-Metabolights/MtblsWS-Py?branch=master)

Docker Hub: [/mtblsws-py](https://hub.docker.com/r/jrmacias/mtblsws-py/)

Using  
- [ISA-API](https://github.com/ISA-tools/isa-api)
- [Flask](http://flask.pocoo.org/)
- [Flask-RESTful](https://flask-restful.readthedocs.io/)
- [flask-restful-swagger](https://github.com/rantav/flask-restful-swagger)
- [marshmallow](https://marshmallow.readthedocs.io/en/latest/index.html)
- [Flask-CORS](http://flask-cors.readthedocs.io/en/latest/api.html)
- [owlready2](https://pythonhosted.org/Owlready2/)
- [gunicorn](https://gunicorn.org)
- [mzml2isa](https://github.com/ISA-tools/mzml2isa)
- [gunicorn](https://gunicorn.org)
- [gevent](http://www.gevent.org)
- [psycopg2](http://initd.org/psycopg/)
- [lxml](https://lxml.de)
- [jira](https://pypi.org/project/jira/)
- [pubchempy](https://pubchempy.readthedocs.io/en/latest/)
- [cirpy](https://cirpy.readthedocs.io/en/latest/)
- [zeep](https://python-zeep.readthedocs.io/en/master/)
- [pronto](https://pronto.readthedocs.io/en/latest/)
- [ctfile](https://pypi.org/project/ctfile/)
- [pyopenms](https://pypi.org/project/pyopenms/)
- [gspread](https://gspread.readthedocs.io/en/latest/)
- [oauth2client](https://github.com/googleapis/oauth2client)


Installing the required Python libraries
--------------------------

create a new virtual environment and simply execute "pip install -r requirements.txt"

See config.py for details of how to connect to a MetaboLights ISA-Tab database schema.


Using the REST service
--------------------------
To see what REST calls you can make and how to format your queries, have a look at the on-line Swagger UI API documentation. 
Start either the Flask server (for local development), or Gunicorn for a live environment. 



Once your WS is up and running point a web browser to http://localhost:5000/metabolights/ws/api/spec.html.

Please note that some of the functionality will only work if this is running on EMBL-EBI MetaboLights infrastructure.

Please contact the MetaboLights Team at metabolights-help@ebi.ac.uk for further information.


Feedback
------------
Please give us feedback, we are always looking at ways to improve MetaboLights.

