
import datetime
import glob
import json
import logging
import os
import pathlib
import time
import celery
from app.config import get_settings
from app.tasks.worker import (MetabolightsTask, celery, send_email)
from app.tasks.worker import MetabolightsTask

from app.ws.isaApiClient import IsaApiClient
from app.ws.metabolon_utils import convert_to_isa, create_isa_files, validate_mzml_files
from app.ws.mtblsWSclient import WsClient

wsc = WsClient()
iac = IsaApiClient()
logger = logging.getLogger('wslog')


@celery.task(bind=True, base=MetabolightsTask, name="app.tasks.common_tasks.curation_tasks.metabolon.metabolon_confirm")
def metabolon_confirm(self, study_id: str, study_location: str, target_location: str, user_token, email):

    message = {}
    success = True
    start = time.time()
    try:
        message["task_status"] = 'Error'
        # Validate all mzML files, in both study and upload folders
        # This method also copy files to the study folder and adds a new extension in the upload folder.
        val_status = ''
        val_message = ''
        try:
            val_message = 'Could not validate all the mzML files'
            val_status, val_message = validate_mzml_files(study_id)
        except Exception as exc:
            message.update({'mzML validation': 'Failed', "result": val_message})
            success = False
            
        
        # Adding the success to the final message we will return
        if val_status:
            message.update({'mzML validation': 'Successful'})
        else:
            message.update({'mzML validation': 'Failed', "result": ""})
            success = False

        # Create ISA-Tab files using mzml2isa
        conv_status =''
        conv_message = ''
        try:
            conv_message = 'Could not convert all the mzML files to ISA-Tab'
            conv_status, conv_message = convert_to_isa(study_location, study_id)
        except Exception as exc:
            message.update({'mzML2ISA conversion': 'Failed', "result": conv_message + f" {str(exc.args)}"})
            success = False

        if conv_status:
            message.update({'mzML2ISA conversion': 'Successful'})
        else:
            message.update({'mzML2ISA conversion': 'Failed', "result": ""})
            success = False

        split_status = ''
        split_message = ''
        try:
            split_message = 'Could not correctly create ISA files'
            split_status, split_message = create_isa_files(study_id, study_location, target_location=target_location)
        except Exception as exc:
            message.update({'ISA file creation': 'Failed', "result": split_message  + f" {str(exc.args)}"})
            success = False

        if split_status:
            message.update({'ISA file creation': 'Successful'})
        else:
            message.update({'ISA file creation': 'Failed', "result": ""})
            success = False
    
    except Exception as ex:
        message.update({"Failure reason": f"{str(ex)}"})
        message["task_status"] = 'unexpected error'
        raise ex
    finally:
        end = time.time()
        result = {
            "status": success,
            "study_id": study_id,
            "result": message,
            "start_time": datetime.datetime.fromtimestamp(start).strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": datetime.datetime.fromtimestamp(end).strftime("%Y-%m-%d %H:%M:%S"),
            "report_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "executed_on":  os.uname().nodename
            }
        
        body_intro = f"You can see the result of your task {str(self.request.id)}.<p>" 
        result_str = body_intro + json.dumps(result, indent=4)
        result_str = result_str.replace("\n", "<p>")
        send_email("Result of the task: partner metabolon confirm", result_str, None, email, None)