import datetime
import os
from smtplib import SMTP
from app.services.storage_service.storage_service import StorageService
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.study.user_service import UserService
from app.tasks.worker import celery, get_flask_app
import requests

check_functions = {}
ordererd_check_function_names = []
def check_result(func):
    def runner(flask_app):
        try:
            result = func(flask_app)
            if result:
                return {"result": "OK", "message": ""}
            else:
                return {"result": "FAILED", "message":"Result is empty"}
        except Exception as exc:
            return {"result": "FAILED", "message":f"{str(exc)}"}
    check_functions[func.__name__] = runner
    ordererd_check_function_names.append(func.__name__)
    return runner

####################################################################################################
    
@check_result
def check_postgresql(flask_app):
    mtbls_submitter_user = flask_app.config.get("MTBLS_SUBMITTER_EMAIL")
    return UserService.get_instance(flask_app).get_simplified_user_by_username(mtbls_submitter_user)

@check_result
def check_elasticsearch(flask_app):
    return ElasticsearchService.get_instance(flask_app).get_study("MTBLS1")

@check_result
def check_private_ftp(flask_app):
    private_ftp_sm = StorageService.get_ftp_private_storage(flask_app)
    return private_ftp_sm.remote.create_folder("mtbls-test-9999999999-folder")

@check_result
def check_email(flask_app):
    host = flask_app.config.get("MAIL_SERVER")
    port = str(flask_app.config.get("MAIL_PORT"))
    
    with SMTP(host=host, port=port) as smtp:
        result = smtp.noop()
        return result

@check_result
def check_mtblsws_py(flask_app):
    host = flask_app.config.get("MTBLS_WS_HOST")
    port = str(flask_app.config.get("PORT"))
    context = flask_app.config.get("RESOURCES_PATH")

    url = f"{host}:{port}{context}" if port else f"{host}{context}"
        
    response = requests.get(url)
    response.raise_for_status()
    return response.text

# @check_result
def check_classyfire(flask_app):
    url = flask_app.config.get("CLASSYFIRE_ULR")
    data = {"label": "MetaboLights WS", "query_input": "InChI=1S/CH4O/c1-2/h2H,1H3", "query_type": "STRUCTURE"}
    response = requests.post(url + '/queries.json', data=data, headers={"Content-Type": "application/json"})
    response.raise_for_status()
    return str(response.json()['id'])
    
####################################################################################################
#### Integration check 
####################################################################################################

@celery.task(name="app.tasks.common_tasks.admin_tasks.integration_check.check_integrations")
def check_integrations():
    flask_app = get_flask_app()
    test_results = {}
    with flask_app.app_context():
        for function_name in ordererd_check_function_names:
            test_results[function_name] = check_functions[function_name](flask_app)

        status = "OK"
        for test_type in test_results:
            if test_results[test_type]["result"] != "OK":
                status = "ERROR"
                
                # send_technical_issue_email("Integration Check Failure", str(test_results))
                break
        
        return {"status": status, "executed_on":  os.uname().nodename, "time": int(datetime.datetime.now().timestamp()) , "test_results": test_results}