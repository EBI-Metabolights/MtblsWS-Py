
import datetime
import glob
import json
import logging
import os
import time
import celery
from app.config import get_settings
from app.tasks.worker import (MetabolightsTask, celery, send_email)
from app.tasks.worker import MetabolightsTask

from app.ws.db_connection import update_release_date
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.settings.utils import get_study_settings
from app.ws.utils import validate_mzml_files, convert_to_isa, copy_file, read_tsv, write_tsv, \
    update_correct_sample_file_name, get_year_plus_one

wsc = WsClient()
iac = IsaApiClient()
logger = logging.getLogger('wslog')


@celery.task(bind=True, base=MetabolightsTask, name="app.tasks.common_tasks.curation_tasks.metabolon.metabolon_confirm")
def metabolon_confirm(self, study_id: str, study_location: str, user_token, email):

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
        except:
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
        except:
            message.update({'mzML2ISA conversion': 'Failed', "result": conv_message})
            success = False

        if conv_status:
            message.update({'mzML2ISA conversion': 'Successful'})
        else:
            message.update({'mzML2ISA conversion': 'Failed', "result": ""})
            success = False

        split_status = ''
        split_message = ''
        # Split the two pos/neg assays from Metabolon into 4
        try:
            split_message = 'Could not correctly split the assays'
            split_status, split_message = split_metabolon_assays(study_location, study_id)
        except:
            message.update({'Assay splitting': 'Failed', "result": split_message})
            success = False

        if split_status:
            message.update({'Assay splitting': 'Successful'})
        else:
            message.update({'Assay splitting': 'Failed', "result": ""})
            success = False

        copy_status = ''
        copy_message = ''
        # copy Metabolon investigation file into the study folder
        try:
            copy_status, copy_message = copy_metabolon_template(study_id, user_token, study_location)
        except:
            message.update({'Investigation template replacement': 'Failed', "result": copy_message + " The investigation file still needs replacing"})
            success = False
            
        if copy_status:
            message.update({'Investigation template replacement': 'Successful'})
        else:
            message.update({'Investigation template replacement': 'Failed', "result": ""})
            success = False

        if success:
            message["task_status"] = 'All conversion steps completed successfully'
    
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
    


def copy_metabolon_template(study_id, user_token, study_location):
    settings = get_study_settings()
    status, message = True, "Copied Metabolon template into study " + study_id
    invest_file = settings.investigation_file_name

    # Get the correct location of the Metabolon template study
    template_study_location = get_settings().file_resources.study_partner_metabolon_template_path
    template_study_location = os.path.join(template_study_location, invest_file)
    dest_file = os.path.join(study_location, invest_file)

    try:
        copy_file(template_study_location, dest_file)
    except:
        return False, "Could not copy Metabolon template into study " + study_id

    try:
        
        # Updating the ISA-Tab investigation file with the correct study id
        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id=study_id, api_key=user_token, skip_load_tables=True, study_location=study_location)

        # Adding the study identifier
        isa_study.identifier = study_id
        isa_inv.identifier = study_id

        # Also make sure the sample file is in the standard format of 's_MTBLSnnnn.txt'
        isa_study, sample_file_name = update_correct_sample_file_name(isa_study, study_location, study_id)

        # Set publication date to one year in the future
        plus_one_year = get_year_plus_one(isa_format=True)
        date_now = get_year_plus_one(todays_date=True, isa_format=True)

        isa_inv.public_release_date = plus_one_year
        isa_inv.submission_date = date_now
        isa_study.public_release_date = plus_one_year
        isa_study.submission_date = date_now

        # Updated the files with the study accession
        try:
            iac.write_isa_study(
                inv_obj=isa_inv, api_key=user_token, std_path=study_location,
                save_investigation_copy=False, save_samples_copy=False, save_assays_copy=False
            )
        except Exception as e:
            logger.info("Could not write the study: " + study_id + ". Error: " + str(e))

        try:
            update_release_date(study_id, plus_one_year)
            wsc.reindex_study(study_id, user_token)
            # message = message + '. ' + api_version + '. ' + mzml2isa_version
        except Exception as e:
            logger.info("Could not updated database and re-index study: " + study_id + ". Error: " + str(e))
    except Exception as e:
        return False, "Could not update Metabolon template for study " + study_id + ". Error: " + str(e)

    return status, message


def split_metabolon_assays(study_location, study_id):
    p_start = 'a__POS'
    n_start = 'a__NEG'
    end = '_m'
    pos = p_start + end
    neg = n_start + end
    sample_col = 'Sample Name'

    for a_files in glob.glob(os.path.join(study_location, 'a__*_metabolite_profiling_mass_spectrometry.txt')):
        if pos in a_files:
            p_assay = read_tsv(a_files)
            p_filename = a_files
            try:
                # split based on 'POSEAR' and 'POSLAT'
                write_tsv(p_assay.loc[p_assay[sample_col].str.contains('POSEAR')],
                          p_filename.replace(pos, p_start + '_1' + end))
                write_tsv(p_assay.loc[p_assay[sample_col].str.contains('POSLAT')],
                          p_filename.replace(pos, p_start + '_2' + end))
            except:
                return False, "Failed to generate 2 POSITIVE ISA-Tab assay files for study " + study_id

        elif neg in a_files:
            n_assay = read_tsv(a_files)
            n_filename = a_files
            try:
                # split based on 'NEG' and 'POL'
                write_tsv(n_assay.loc[n_assay[sample_col].str.contains('NEG')],
                          n_filename.replace(neg, n_start + '_1' + end))
                write_tsv(n_assay.loc[n_assay[sample_col].str.contains('POL')],
                          n_filename.replace(neg, n_start + '_2' + end))
            except:
                return False, "Failed to generate 2 NEGATIVE ISA-Tab assay files for study " + study_id

    status, message = True, "Generated 4 ISA-Tab assay files for study " + study_id

    return status, message