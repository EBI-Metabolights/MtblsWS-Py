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



Using the REST service
--------------------------
To see what REST calls you can make and how to format your queries, have a look at the on-line Swagger UI API documentation. 
Start either the Flask server (for local development), or Gunicorn for a live environment. 
Once your WS is up and running point a web browser to http://localhost:5000/metabolights/ws/api/spec.html.

Please note that some of the functionality will only work if this is running on EMBL-EBI MetaboLights infrastructure.

Please contact the [MetaboLights Team](http://www.ebi.ac.uk/metabolights/contact)  for further onto_information.


Feedback
------------
Please give us feedback, we are always looking at ways to improve MetaboLights.
