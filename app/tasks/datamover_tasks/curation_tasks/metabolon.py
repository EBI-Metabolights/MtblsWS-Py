
import datetime
import json
import logging
import os
from app.tasks.worker import MetabolightsTask, celery, send_email

from app.utils import current_time

from app.ws.isaApiClient import IsaApiClient
from app.ws.metabolon_utils import check_input_files, convert_to_isa, create_isa_files, validate_mzml_files
from app.ws.mtblsWSclient import WsClient

wsc = WsClient()
iac = IsaApiClient()
logger = logging.getLogger('wslog')


@celery.task(bind=True, base=MetabolightsTask, name="app.tasks.datamover_tasks.curation_tasks.metabolon.metabolon_confirm")
def metabolon_confirm(self, study_id: str, study_location: str, email: str, target_location: str=None):

    message = {}
    success = False
    start = current_time()
    try:
        # pipeline_folder = os.path.join(get_settings().study.mounted_paths.study_internal_files_root_path, study_id, "metabolon_pipeline")
        # Validate all mzML files, in both study and upload folders
        # This method also copy files to the study folder and adds a new extension in the upload folder.
        input_file_check_message = 'Could not validate input files'
        input_file_check_success = False
        try:
            input_file_check_success, input_file_check_message = check_input_files(study_id, study_location=study_location)
        except Exception as exc:
            input_file_check_message =  str(exc)
        
        # Adding the success to the final message we will return
        if input_file_check_success:
            message.update({'Input File check': 'Successful'})
        else:
            message.update({'Input File check': 'Failed', "result": input_file_check_message})
            raise Exception("Metabolon pipeline: Input File check failed.")
        
        validation_message = 'Could not validate all the mzML files'
        validation_success = False
        try:
            validation_success, validation_message = validate_mzml_files(study_id)
        except Exception as exc:
            validation_message =  str(exc)
        
        # Adding the success to the final message we will return
        if validation_success:
            message.update({'mzML validation': 'Successful'})
        else:
            message.update({'mzML validation': 'Failed', "result": validation_message})
            raise Exception("Metabolon pipeline: mzML file validation failed.")

        # Create ISA-Tab files using mzml2isa
        convert_to_isa_success = False
        convert_to_isa_message = 'Could not convert all the mzML files to ISA-Tab'
        try:
            convert_to_isa_success, convert_to_isa_message = convert_to_isa(study_location, study_id)
        except Exception as exc:
            convert_to_isa_message = str(exc)


        if convert_to_isa_success:
            message.update({'mzML2ISA conversion': 'Successful'})
        else:
            message.update({'mzML2ISA conversion': 'Failed', "result": convert_to_isa_message})
            raise Exception("mzML2ISA failed.")


        create_isa_files_success = False
        create_isa_files_message = 'Could not correctly create ISA files'
        try:
            create_isa_files_success, create_isa_files_message = create_isa_files(study_id, study_location, target_location=target_location)
        except Exception as exc:
            create_isa_files_message = str(exc)
            
        if create_isa_files_success:
            message.update({'ISA file creation': 'Successful'})
        else:
            message.update({'ISA file creation': 'Failed', "result": create_isa_files_message})
            raise Exception("ISA file creation failed.")
        success = True
    except Exception as ex:
        message.update({"Failure reason": f"{str(ex)}"})
        raise ex
    finally:
        end = current_time()
        time_difference = end - start
        hours, remainder = divmod(time_difference.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        time_format = "{:02}:{:02}:{:02} [HH:MM:ss]".format(int(hours), int(minutes), int(seconds))


        result = {
            "status": "Successful" if success else "Failed",
            "study_id": study_id,
            "result": message,
            "start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end.strftime("%Y-%m-%d %H:%M:%S"),
            "elapsed_time": time_format,
            "report_time": current_time().strftime("%Y-%m-%d %H:%M:%S"),
            "executed_on":  os.uname().nodename
            }
        
        body_intro = f"You can see the result of your task {str(self.request.id)} for study {study_id}.<p>" 
        result_str = body_intro + json.dumps(result, indent=4)
        result_str = result_str.replace("\n", "<p>")
        send_email("Result of the task: partner metabolon confirm for " + study_id, result_str, None, email, None)

# if __name__ == '__main__':
#     study_id = "MTBLS3476"
#     settings = get_settings()
#     study_root_path = pathlib.Path(settings.study.mounted_paths.study_metadata_files_root_path)
#     target_root_path = pathlib.Path(settings.study.mounted_paths.study_internal_files_root_path)
#     study_location = study_root_path / study_id
#     metabolon_confirm(study_id=study_id, study_location=str(study_location), email="")