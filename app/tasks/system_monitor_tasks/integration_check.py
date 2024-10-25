import os

from app.tasks.system_monitor_tasks.integrity_tests.utils import ordered_check_function_names, function_categories, check_functions
from app.utils import current_time
from app.tasks.worker import celery
from app.tasks.system_monitor_tasks.integrity_tests import chebi, internal_components, paths 
####################################################################################################
####################################################################################################
#### Integration check 
####################################################################################################

@celery.task(name="app.tasks.common_tasks.admin_tasks.integration_check.check_integrations")
def check_integrations():
    test_results = {}
    for function_name in ordered_check_function_names:
        category = function_categories[function_name]
        if category not in test_results:
            test_results[category] = {}
        test_results[category][function_name] = check_functions[category][function_name]()

    status = "OK"
    failed_tests = []
    for category in test_results:
        test_types = test_results[category]
        for item in test_types:
            if test_types[item]["result"] != "OK":
                status = "ERROR"
                failed_tests.append((category, item))
                
                # send_technical_issue_email("Integration Check Failure", str(test_results))
                break
        
    return {"status": status, "executed_on":  os.uname().nodename, "time": int(current_time().timestamp()) , "failed_tests": failed_tests, "test_results": test_results}

if __name__ == "__main__":
    result = check_integrations()
    print(result)