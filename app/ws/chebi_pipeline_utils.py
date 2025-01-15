

import glob
import json
import logging
import os
import re
import shlex
import shutil
import sys
import time
from typing import List
import urllib.parse
from pathlib import Path
import zipfile

import cirpy
import ctfile
import numpy as np
import pandas as pd
import pronto
import pubchempy as pcp
import requests
import subprocess
from flask import Flask
from flask_restful import abort
from pubchempy import get_compounds
from zeep import Client
from unidecode import unidecode
from app.config import get_settings
from app import application_path

from app.services.storage_service.acl import Acl
from app.services.storage_service.storage import Storage
from app.services.storage_service.storage_service import StorageService
from app.tasks.datamover_tasks.basic_tasks.execute_commands import execute_bash_command
from app.tasks.hpc_rsync_worker import HpcRsyncWorker
from app.ws.chebi.search.curated_metabolite_table import CuratedMetaboliteTable
from app.ws.chebi.wsproxy import ChebiWsProxy, get_chebi_ws_proxy
from app.ws.cluster_jobs import submit_job
from app.ws.db_connection import get_user_email
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.settings.utils import get_study_settings
from app.ws.study.folder_utils import  get_all_files_from_filesystem
from app.ws.study.study_service import StudyService
from app.ws.utils import read_tsv, write_tsv, safe_str
from chembl_structure_pipeline.standardizer import parse_molblock, update_mol_valences
from rdkit import Chem

logger = logging.getLogger('wslog')

# MetaboLights (Java-Based) WebService client
wsc = WsClient()
iac = IsaApiClient()

def run_chebi_pipeline(study_id, user_token, annotation_file_name, run_silently: bool = False, classyfire_search: bool=True, update_study_maf: bool=False, run_on_cluster: bool=True, email=None, task_name=None):

    settings = get_settings()
    http_base_location = settings.server.service.ws_app_base_link
    http_file_location = http_base_location + os.sep + study_id + os.sep + 'files'
    study = StudyService.get_instance().get_study_by_acc(study_id)
    obfuscation_code = study.obfuscationcode
    user_email = get_user_email(user_token)
    study_metadata_location = os.path.join(settings.study.mounted_paths.study_metadata_files_root_path, study_id)
    cmd = ""
    if run_on_cluster:
        cmd = "curl --silent --request POST"
        param = " -d 'classyfire_search=" + str(
            classyfire_search).lower() + "' -d 'annotation_file_name=#FILE_NAME#' -d 'update_study_maf=" + str(
            update_study_maf).lower() + "' "
        cmd = cmd + param + " -i -H \\'Accept: application/json\\' -H \\'Content-Type: application/json\\' -H \\'user_token: " + user_token + "\\' '"
        cmd = cmd + get_settings().chebi.pipeline.chebi_pipeline_url + study_id + \
                "/chebi-pipeline?source=cluster' "
        print_log("cluster job -  %s", cmd)

    maf_len = 0
    new_maf_len = 0
    pubchem_file = "Executed on the cluster"
    dime_db_api_active = None
    cactus_search_status = None
    opsin_search_status = None
    chemspider_search_status = None
    unichem_search_status = None
    classyfire_search_status = None
    classy_file_downoaded = None
    
    if annotation_file_name is None:
        old_file_name = ""
        # Loop through all m_*_v2_maf.tsv files
        study_files, upload_files, upload_diff, upload_location = \
            get_all_files_from_filesystem(
                study_id, obfuscation_code, study_metadata_location, directory=None, include_raw_data=False)
        maf_count = 0
        maf_changed = 0
        for file in study_files:
            file_name = file['file']
            if file_name.startswith('m_') and file_name.endswith('.tsv'):
                maf_count += 1
                maf_len, new_maf_len, pubchem_file, dime_db_api_active,cactus_search_status, \
                opsin_search_status,chemspider_search_status,unichem_search_status,classyfire_search_status,classy_file_downoaded,chebi_search_status, \
                chebi_master_list_initialised,chebi_search_manager_initiated = \
                    search_and_update_maf(study_id, study_metadata_location, file_name, classyfire_search, user_token,
                                            run_silently=run_silently, update_study_maf=update_study_maf,
                                            obfuscation_code=obfuscation_code, email=email, task_name=task_name)
                if maf_len != new_maf_len:
                    maf_changed += 1
    else:
        annotation_file_name = annotation_file_name.strip()
        cmd = cmd.replace("#FILE_NAME#", annotation_file_name)  # Replace the dummy file name reference in URL
        if run_on_cluster:
            # create param file
            print_log("Starting cluster job for ChEBI pipeline: " + cmd)
            status, message, job_out, job_err, log_file = submit_job(True, account=user_email, job_cmd=cmd,
                                                                        job_params=None, log=False)
            print_log("job submitted")
            if status:
                return {"success": message, "message": job_out, "errors": job_err}
            else:
                return {"error": message, "message": job_out, "errors": job_err}
        else:
            maf_len, new_maf_len, pubchem_file, dime_db_api_active, cactus_search_status, opsin_search_status, \
            chemspider_search_status,unichem_search_status,classyfire_search_status,classy_file_downoaded,chebi_search_status, \
            chebi_master_list_initialised,chebi_search_manager_initiated  = \
            search_and_update_maf(study_id, study_metadata_location, annotation_file_name, classyfire_search, user_token,
                                        run_silently=run_silently, update_study_maf=update_study_maf,
                                        obfuscation_code=obfuscation_code, email=email, task_name=task_name)
            pubchem_file = http_file_location + pubchem_file.split('/' + study_id)[1]

        # if file still present rmeove it

        return {"in_rows": maf_len, "out_rows": new_maf_len, "pubchem_file": pubchem_file, "DimeDB API hit":dime_db_api_active,
                "OPSIN API hit":opsin_search_status, "Cactus API hit":cactus_search_status, 
                "ChemSpider API hit" :chemspider_search_status, "Unichem API hit" : unichem_search_status,"CHEBI API hit":chebi_search_status,
                "Classyfire API hit":classyfire_search_status, "Classyfire SDF downloaded":classy_file_downoaded, 
                "CHEBI Master List initialised":chebi_master_list_initialised, "CHEBI search manager initiated": chebi_search_manager_initiated}

    return {"success": str(maf_count) + " MAF files found, " + str(maf_changed) + " files needed updating."}



instance_dir = os.path.join(application_path, "instance")
flask_app = Flask(__name__, instance_relative_config=True, instance_path=instance_dir)
settings = get_settings()
pubchem_end = "_pubchem.tsv"
complete_end = "_complete.sdf"
pubchem_sdf_extension = '_pubchem.sdf'
classyfire_end = "_classyfire"
anno_sub_folder = settings.chebi.pipeline.chebi_annotation_sub_folder
final_cid_column_name = "final_external_id"
unknown_list = "unknown", "un-known", "n/a", "un_known", "not known", "not-known", "not_known", "unidentified", \
               "not identified", "unmatched"
resource_folder = os.path.join(application_path, "resources")

search_flag = 'search_flag'
maf_compound_name_column = "metabolite_identification"
alt_name_column = "alt_name"
database_identifier_column = "database_identifier"
final_inchi_column = "final_inchi"
csid_ik_column = "csid_ik"
spreadsheet_fields = [database_identifier_column,
                      "chemical_formula",
                      "smiles",
                      "inchi",
                      maf_compound_name_column,
                      "row_id",
                      "combination",
                      search_flag,
                      alt_name_column,
                      "pubchem_first_synonym",
                      final_cid_column_name,
                      "pubchem_cid",
                      "pubchem_cid_ik",
                      csid_ik_column,
                      "final_smiles",
                      final_inchi_column,
                      "final_inchi_key",
                      "direct_parent",
                      "classyfire_search_id",
                      "search_type",
                      "ID",
                      "NAME",
                      "DEFINITION",
                      "IUPAC_NAME",
                      "SYNONYM",
                      "DATABASE_ACCESSION",
                      "CAS_NO",
                      "REFERENCE",
                      "RELATIONSHIP",
                      "ORGANISM",
                      "ORGANISM_PART",
                      "STRAIN",
                      "SOURCE_PMID",
                      "SOURCE_DOI",
                      "SOURCE_ARTICLE",
                      "SOURCE_METABOLIGHTS",
                      "COMMENT",
                      "pubchem_smiles",
                      "cactus_smiles",
                      "opsin_smiles",
                      "pubchem_inchi",
                      "cactus_inchi",
                      "opsin_inchi",
                      "pubchem_inchi_key",
                      "cactus_inchi_key",
                      "opsin_inchi_key",
                      "pubchem_formula",
                      "pubchem_synonyms",
                      "cactus_synonyms",
                      "glytoucan_id",
                      "unichem_id",
                      "cas_id",
                      "dime_id"]


def get_idx(col_name, pubchem_df_headers=None):
    idx = None
    for idx, name in enumerate(spreadsheet_fields):
        if col_name.lower() == name.lower():
            if pubchem_df_headers is not None and idx >= 5:
                for p_idx, p_name in enumerate(pubchem_df_headers.values):
                    if name == p_name[0]:
                        if idx != p_idx:  # Same name but different column position!
                            return p_idx
                        else:
                            return idx
            return idx

    print_log("Can not find definition or index for spreadsheet field " + col_name)
    return idx


def read_sdf_file(sdf_file_name):
    mols = None
    try:
        with open(sdf_file_name, 'r', encoding='utf-8') as infile:
            mols = ctfile.load(infile)
    except Exception as e:
        print_log("    -- Could not read MOL structure in file " + sdf_file_name + ". Error: " + str(e))
    return mols


def split_rows(maf_df, annotation_file=None):
    # Split rows with pipe-lines "|"
    pipe_found = False
    new_maf = maf_df
    try:
        with open(annotation_file, 'r') as f:
            for line in f:
                if '|' in line:
                    pipe_found = True
                    f.close()
                    break
    except UnicodeDecodeError:
        print_log('Warning: Checking for pipelines in the MAF failed. Trying to read using Excel "ISO-8859-1" encoding',
                  mode='warning')
        with open(annotation_file, 'r', encoding="ISO-8859-1") as f:
            for line in f:
                if '|' in line:
                    pipe_found = True
                    f.close()
                    break

    if pipe_found:
        print_log("    -- Found pipe-line '|' will split MAF rows")
        try:
            new_maf = split_maf_df(annotation_file)
        except Exception as e:
            print_log("    -- ERROR: MAF splitter failed!. Error: " + str(e), mode='error')
            return maf_df
    else:
        print_log("Did not find any pipe-lines '|' in the MAF.")

    return new_maf


def split_maf_df(maf_file_name):
    split_file = maf_file_name + "_SPLIT"
    stderr = None

    try:
        args = [os.path.join(resource_folder, 'maf-splitter.jar'), maf_file_name]  # Any number of args
        stdout, stderr = jar_wrapper(*args)
    except Exception as e:
        print_log("    -- Could not split MAF file. Error: " + str(e) + str(stderr))

    try:
        new_df = read_tsv(split_file)
        os.remove(split_file)
    except Exception as e:
        print_log("Could not read and/or remove 'split' file " + split_file + ". " + str(e))

    return new_df


def jar_wrapper(*args):
    process = subprocess.Popen(['java', '-jar'] + list(args), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.wait()
    stdout, stderr = process.communicate()
    return stdout, stderr


def print_log(message, silent=False, mode='info'):
    if not silent:
        print(str(message))
        if mode == 'info':
            logger.info(str(message))
        elif mode == 'error':
            logger.error(str(message))
        else:
            logger.warning(str(message))


def check_maf_for_pipes(study_location, annotation_file_name):
    annotation_file = os.path.join(study_location, annotation_file_name)
    try:
        maf_df = read_tsv(annotation_file)
    except FileNotFoundError:
        abort(400, message="The file " + annotation_file + " was not found")
    except Exception as e:
        abort(400, message="Errors reading file " + annotation_file + " " + str(e))
    maf_len = len(maf_df.index)

    # Any rows to split?
    new_maf_df = split_rows(maf_df, annotation_file)
    new_maf_len = len(new_maf_df.index)
    annotation_folder_path = os.path.join(study_location, get_study_settings().internal_files_symbolic_link_name, anno_sub_folder)
    os.makedirs(annotation_folder_path, exist_ok=True)
    file_name = os.path.join(annotation_folder_path, annotation_file_name + '.split')
    if maf_len != new_maf_len:  # We did find |, so we create a new MAF
        write_tsv(new_maf_df, file_name)

    return maf_df, maf_len, new_maf_df, new_maf_len, file_name


def check_if_unknown(comp_name):
    comp_name = comp_name.lower()

    if comp_name in unknown_list:
        return False

    for c_name in unknown_list:
        if comp_name.endswith(c_name):
            return False

        if comp_name.startswith(c_name):
            return False
    try:
        if int(comp_name) > 0:
            print_log("    -- Compound name is only a number? Ignoring '" + comp_name + "'")
            return False  # Name is just a number?
    except ValueError:
        return True
    except Exception as e:
        print_log("    -- ERROR: Testing if compound name is only a number failed '" + comp_name + "'. Error: " +
                  str(e), mode='error')
        return True

    return True


def clean_comp_name(comp_name):
    remove_chars = ['/', ' ', '(', ')', ')', ',', ':', ';', '\\', '-', '\'', '"', '?', '{', '}', '*']

    for c in remove_chars:
        comp_name = comp_name.replace(c, '')
    return comp_name


def get_pubchem_substance(comp_name, res_type):
    results = {}
    result = ""
    comp_name_url = urllib.parse.quote(comp_name)
    url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug/substance/name/" + comp_name_url + "/cids/JSON"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            try:
                json_resp = resp.json()
                results = json_resp
            except Exception as ex:
                logger.warning(f"Invalid result from {url}  {str(ex)}")
        if resp.status_code == 404:
            print_log("    -- No PubChem Substance found for '" + comp_name + "'")

        for idx, result in enumerate(results["InformationList"]['Information']) if results else []:
            print_log("    -- Found PubChem Substance(s) for '" + comp_name + "' (" + res_type + "), search record #" + str(
                idx + 1))
            if res_type == 'cid':
                return result['CID'][0]  # Only return the top hit
            else:
                return result['SID']
    except Exception as e:
        print_log("    -- Error querying Pubchem API : " + str(e), mode='error')
    return result


def get_pubchem_synonyms(comp_name):
    result = []
    results = pcp.get_synonyms(comp_name + '/cids/', namespace='comp_name', domain='substance', searchtype='name')
    for result in results:
        return result  # Only return the top hit

    return result


def get_existing_values(df, comp_name):
    print_log("    -- Checking if we have already searched for '" + comp_name + "'")
    row = df[(df.metabolite_identification == comp_name) &
             (df.search_flag == '1') | (df.search_flag == '1.0')]
    if len(row.index) >= 1:
        row.loc[row[maf_compound_name_column] == comp_name, ['row_id', 'search_flag']] = '', '1'
        return row[:1]  # Only return the 1st row, otherwise the replace function in Pandas will "paste" all rows

    return row


def get_sample_details(study_id, user_token, study_location):
    start_time = time.time()
    isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                     skip_load_tables=True,
                                                     study_location=study_location)
    try:
        if isa_study:
            sample_file = isa_study.filename
            sample_df = read_tsv(os.path.join(study_location, sample_file))
    except FileNotFoundError:
        print_log("Error: Could not find ISA-Tab metadata files", mode='error')
        abort(500, message='Error: Could not find ISA-Tab metadata files')
    except Exception as e:
        print_log("Error: Parsing ISA-Tab metadata files " + str(e), mode='error')
        abort(500, message="Error: Parsing ISA-Tab metadata files" + str(e))

    print_log("Reading ISA-Tab sample file took %s seconds" % round(time.time() - start_time, 2))

    try:
        organism_pos = sample_df.columns.get_loc('Characteristics[Organism]')
    except KeyError:
        organism_pos = None

    try:
        organism_part_pos = sample_df.columns.get_loc('Characteristics[Organism part]')
    except KeyError:
        organism_part_pos = None

    try:
        variant_pos = sample_df.columns.get_loc('Characteristics[Variant]')
    except KeyError:
        variant_pos = None

    unique_org_count = 0
    all_orgs = []
    all_orgs_with_index = []
    for idx, sample in sample_df.iterrows():
        try:
            org_term = sample[organism_pos + 2]
            org = sample['Characteristics[Organism]']
            org = unidecode(org)
            if org_term != '':
                org = org + ' [' + convert_to_chebi_onto(org_term) + ']'
            if 'blank' in org.lower() or org == " []":
                org = ""
        except Exception as e:
            logging.exception(e)
            org = ""

        try:
            org_part_term = sample[organism_part_pos + 2]
            org_part = sample['Characteristics[Organism part]']
            org_part = unidecode(org_part)
            if org_part_term != '':
                org_part = org_part + ' [' + convert_to_chebi_onto(org_part_term) + ']'
            if 'blank' in org_part.lower() or org_part == " []":  # ToDo: Do not need blank or other controls
                org_part = ""
        except Exception as e:
            logging.exception(e)
            org_part = ""

        variant = ""
        variant_onto = ""
        try:
            variant = sample['Characteristics[Variant]']  # There may not always be an ontology for this, so two calls
            variant = unidecode(variant)
            variant_part_term = sample[variant_pos + 2]
            if variant_part_term != '':
                variant_onto = ' [' + convert_to_chebi_onto(variant_part_term) + ']'
                variant = variant + variant_onto
            if variant == ' []':
                variant = ""
        except Exception as e:
            # print_log("    -- WARNING: 'Characteristics[Variant]' not found in the sample sheet")
            variant = ""

        complete_org = org + "|" + org_part + "|" + variant

        if complete_org not in all_orgs:
            unique_org_count += 1
            all_orgs.append(complete_org)
            all_orgs_with_index.append(complete_org + '|' + str(unique_org_count))

    return all_orgs_with_index  # all_orgs


def convert_to_chebi_onto(onto_term):
    # Example: http://purl.bioontology.org/ontology/NCBITAXON/39414
    chebi_onto = onto_term.lower().replace("ncbitaxon/", "ncbitaxon:")
    if chebi_onto:
        chebi_onto = chebi_onto.rsplit('/', 1)[1]  # Only keep the final part of the URL
    chebi_onto = chebi_onto.replace("ncbitaxon:", "ncbi:").replace("_", ":").replace('txid', '').upper()
    chebi_onto = chebi_onto.replace('NCBI:', 'NCBI:txid')
    chebi_onto = chebi_onto.replace('NCBITAXON:', 'NCBI:txid')
    return chebi_onto


def populate_sample_rows(pubchem_df, study_id, user_token, study_location):
    all_organisms = get_sample_details(study_id, user_token, study_location)
    organism_len = len(all_organisms)
    newdf = pubchem_df.copy()
    newdf = newdf.replace(np.nan, '', regex=True)
    pubchem_len = len(pubchem_df)

    if organism_len > 1:  # Multiply rows by number of unique sample rows
        newdf = pd.DataFrame(np.repeat(pubchem_df.values, organism_len, axis=0))
        newdf.columns = pubchem_df.columns

    # For simplicy extend the list to the same length as the dataframe
    all_organisms = duplicate(all_organisms, pubchem_len)
    org_pos = newdf.columns.get_loc('ORGANISM')
    org_part_pos = newdf.columns.get_loc('ORGANISM_PART')
    strain_pos = newdf.columns.get_loc('STRAIN')
    for idx, row in newdf.iterrows():  # Loop and add the different unique sample rows
        if row[0] == "" and all_organisms and len(all_organisms) > idx:  # Only add if database is not known and sample file is valid.
            s_row = all_organisms[idx]
            org_parts = s_row.split('|')
            newdf.iloc[idx, [org_pos, org_part_pos, strain_pos]] = org_parts[0], org_parts[1], org_parts[2]
            # newdf.iloc[idx, org_part_pos] = org_parts[1]
            # newdf.iloc[idx, strain_pos] = org_parts[2]

            combination = org_parts[3]
            if organism_len > 1 and combination:
                newdf.iloc[idx, get_idx('combination')] = org_parts[3]

    newdf.drop_duplicates(keep='first', inplace=True)
    newdf.reset_index(drop=True, inplace=True)

    return newdf


def duplicate(my_list, n):
    new_list = []
    for i in range(n):
        new_list.extend(my_list)
    return new_list


def unique_list(l):
    ulist = []
    [ulist.append(x) for x in l if x not in ulist and x]
    return ulist


def get_cas_from_synonyms(synonyms):
    pattern = re.compile("CAS-\S")
    result = unique_list(synonyms.split(';'))
    for i in result:
        if pattern.match(i):
            return i[4:]


def search_and_update_maf(study_id: str, study_metadata_location: str, annotation_file_name: str, classyfire_search, user_token,
                          run_silently=None, update_study_maf=None, obfuscation_code=None, email: str=None, task_name: str=None):
    sdf_file_list = []
    exiting_pubchem_file = False
    sample_compound_name = 'aspirin'
    sample_compound_2 = '2,4,6-trinitrotoluene'
    dimedb_search_status = 'None'
    dime_db_api_active = True
    
    cactus_search_status = check_cactus_api(comp_name=sample_compound_name)
    opsin_search_status = check_opsin_api(comp_name=sample_compound_2)
    chemspider_search_status = check_chemspider_api()
    unichem_search_status = check_unichem_api()
    chebi_search_status = check_chebi_api()
    classyfire_search_status = check_classyfire_api()
    chebi_master_list_initialised = 'No'
    chebi_search_manager_initiated = 'No'
    curated_metabolite_table: CuratedMetaboliteTable = CuratedMetaboliteTable.get_instance()
    curated_metabolite_table.initialize_df()
    if curated_metabolite_table.initialized:
        chebi_master_list_initialised = 'Yes'
    else:
        chebi_master_list_initialised = 'No'
    
    if WsClient.default_search_manager:
        chebi_search_manager_initiated = 'Yes'
    
    first_start_time = time.time()
    # Please note that the original MAF must exist without the _pubchem.tsv extension!!
    original_maf_name = annotation_file_name.replace("_pubchem.tsv", ".tsv")
    settings = get_study_settings()
    anno_sub_folder_path = os.path.join(study_metadata_location, settings.internal_files_symbolic_link_name, anno_sub_folder)
    create_annotation_folder(anno_sub_folder_path)
    short_file_name = os.path.join(anno_sub_folder_path, annotation_file_name.replace('.tsv', ''))
    if annotation_file_name.endswith(pubchem_end):
        exiting_pubchem_file = True
        short_file_name = os.path.join(anno_sub_folder_path, annotation_file_name.replace(pubchem_end, ''))

    annotation_file_name = os.path.join(study_metadata_location, annotation_file_name)
    pd.options.mode.chained_assignment = None  # default='warn'

    standard_maf_columns = {database_identifier_column: get_idx(database_identifier_column),
                            "chemical_formula": get_idx('chemical_formula'),
                            "smiles": get_idx('smiles'), "inchi": get_idx('inchi')}

    glytoucan_file_df = read_glytoucan_file()
    maf_len = 0
    maf_df = None
    try:
        maf_df = read_tsv(annotation_file_name)
        maf_len = len(maf_df.index)
    except FileNotFoundError:
        abort(400, message="The file " + annotation_file_name + " was not found")
    # CHEBI-PIPELINE annotation folder creation remotely
    
    if obfuscation_code:  # So the curators can FTP new files into the private upload folder for the study
         ftp_private_annotation_folder = os.path.join(study_id.lower() + "-" + obfuscation_code, settings.internal_files_symbolic_link_name, anno_sub_folder)
         ftp_private_storage = StorageService.get_ftp_private_storage()
         create_annotation_folder_on_remote_storage(ftp_private_storage, ftp_private_annotation_folder)

    # First make sure the existing pubchem annotated spreadsheet is loaded
    pubchem_df = maf_df.copy()
    pubchem_df_headers = None

    if exiting_pubchem_file:  # The has already been split and this is an existing "pubchem" file
        new_maf_df = maf_df.copy()
        new_maf_len = len(new_maf_df.index)
        pubchem_df_headers = pd.DataFrame(list(new_maf_df))
    else:
        # Any rows to split?
        new_maf_df = split_rows(maf_df, annotation_file_name)
        new_maf_len = len(new_maf_df.index)

        if maf_len != new_maf_len:  # We did find | so we have to use the new dataframe
            maf_df = new_maf_df

        # Remove existing row values first, because that's what we do ;-)
        for column_name in standard_maf_columns:
            maf_df.iloc[:, standard_maf_columns[column_name]] = ""

        pubchem_df = create_pubchem_df(maf_df)
        pubchem_df = reindex_row_id(pubchem_df, pubchem_df_headers)
        pubchem_df_headers = pd.DataFrame(list(pubchem_df))

    row_idx = 0
    if exiting_pubchem_file:
        short_df = maf_df[[database_identifier_column, maf_compound_name_column, alt_name_column, search_flag,
                           final_cid_column_name, "row_id", final_inchi_column, csid_ik_column]]
        # Make sure we re-read the original MAF so that we don't add the extra PubChem columns
        maf_df = read_tsv(os.path.join(study_metadata_location, original_maf_name))
    else:
        short_df = maf_df[[database_identifier_column, maf_compound_name_column]]

    # Search using the compound name column
    for idx, row in short_df.iterrows():
        database_id = row[0]
        comp_name = safe_str(row[1])
        org_row_id = None
        original_comp_name = comp_name
        search = True
        changed = False
        alt_name = ''
        existing_row = None
        final_inchi_key = None
        final_inchi = None
        csid_ik = None
        final_cid = None
        db_acc = str(pubchem_df.iloc[idx, get_idx('DATABASE_ACCESSION', pubchem_df_headers)])
        if exiting_pubchem_file:
            if str(row[3]).rstrip('.0') == '1':  # This is the already searched flag in the spreadsheet
                search = False
            final_cid = row[4]
            org_row_id = row[5]
            alt_name = row[2]
            alt_name = str(alt_name)
            if len(alt_name) > 0:
                comp_name = alt_name
            final_inchi = row[6]
            csid_ik = row[7] if row[7] != '' else None
            pubchem_df.iloc[idx, get_idx('ID', pubchem_df_headers)] = "temp_" + str(org_row_id)
            pubchem_df.iloc[idx, get_idx('NAME', pubchem_df_headers)] = safe_str(row[1])
            if csid_ik:
                db_acc = db_acc + ';ChemSpider:' + str(csid_ik) + ';'
                db_acc = ';'.join(unique_list(db_acc.split(';')))

        if not exiting_pubchem_file:
            pubchem_df.iloc[row_idx, get_idx('row_id', pubchem_df_headers)] = row_idx + 1  # Row id
            pubchem_df.iloc[idx, get_idx('ID', pubchem_df_headers)] = "temp_" + str(row_idx + 1)
            pubchem_df.iloc[idx, get_idx('NAME', pubchem_df_headers)] = original_comp_name
            # if not database_id:
            #     pubchem_df.iloc[row_idx, get_idx('combination')] = row_idx + 1  # Cluster sort field

        if not run_silently:
            print_log(str(idx + 1) + ' of ' + str(new_maf_len) + ' : ' + comp_name)

        if final_cid and final_cid.endswith('.mol') and final_inchi:
            classyfire_search_id = classyfire(final_inchi)
            if classyfire_search_id:
                pubchem_df.iloc[row_idx, get_idx('classyfire_search_id', pubchem_df_headers)] = str(
                    classyfire_search_id)

        if search and comp_name and check_if_unknown(comp_name):
            if run_silently:
                print_log(str(idx + 1) + ' of ' + str(new_maf_len) + ' : ' + comp_name)
            if alt_name and comp_name == alt_name:
                print_log("    -- Using alt_name '" + alt_name + "'")

            # existing_row = get_existing_values(pubchem_df, original_comp_name)
            if existing_row and len(existing_row.index) > 0:
                print_log("    -- Updating row(s) for compound name '" + original_comp_name +
                          "' (alt name = '" + alt_name + "')")

                pubchem_df.update(pubchem_df[[maf_compound_name_column]].merge(existing_row, 'left'))
                # f not exiting_pubchem_file:
                pubchem_df.iloc[row_idx, get_idx('row_id',
                                                 pubchem_df_headers)] = row_idx + 1  # Update Row id again, not use the copied row
                # if not database_id:
                #     pubchem_df.iloc[row_idx, get_idx('combination')] = row_idx + 1  # Cluster sort field

                pubchem_df.iloc[row_idx, get_idx(alt_name_column,
                                                 pubchem_df_headers)] = alt_name  # Add in the original alt name again
                pubchem_df.iloc[row_idx, get_idx('search_type',
                                                 pubchem_df_headers)] = 'copy_existing_row'  # Search category/type for logging
                changed = True
            else:
                start_time = time.time()
                chebi_found = False
                comp_name = comp_name.strip()  # Remove leading/trailing spaces before we search
                comp_name = comp_name.replace("Î´", "delta").replace("?", "").replace("*", "")
                if '[' in comp_name:
                    comp_name = comp_name.replace("[U]", "").replace("[S]", "")
                    comp_name = re.sub(re.escape(r'[iso\d]'), '', comp_name)

                search_res = wsc.get_maf_search("name",
                                                comp_name)  # This is the standard MetaboLights aka Plugin search
                if not search_res:
                    search_res = wsc.get_maf_search("name", clean_comp_name(comp_name))

                if search_res and search_res['content']:
                    result = search_res['content'][0]
                    database_identifier = result["databaseId"]
                    if not database_identifier:
                        database_identifier = ""
                    chemical_formula = result["formula"]
                    smiles = result["smiles"]
                    inchi = result["inchi"]
                    name = result["name"]

                    new_formula = None

                    if chemical_formula and '-' in chemical_formula:  # MTBLS search adds the charge at the end
                        # Need to get the conjugate acid of this base compound
                        print_log("    -- Searching for conjugate acid of " + comp_name)
                        database_identifier, inchi, inchikey, name, smiles, new_formula, search_type = \
                            direct_chebi_search(final_inchi_key, comp_name,
                                                acid_chebi_id=database_identifier,
                                                search_type="get_conjugate_acid")

                    # if no formula or new_formula is returned, check the first "is_a" and report the formula from that
                    if database_identifier and not new_formula and not chemical_formula:
                        new_database_identifier, new_inchi, new_inchikey, new_name, new_smiles, chemical_formula, search_type = \
                            direct_chebi_search(final_inchi_key, comp_name,
                                                acid_chebi_id=database_identifier,
                                                search_type="is_a")
                    elif new_formula:
                        chemical_formula = new_formula

                    pubchem_df.iloc[row_idx, get_idx(database_identifier_column)] = database_identifier
                    pubchem_df.iloc[row_idx, get_idx('chemical_formula')] = chemical_formula
                    pubchem_df.iloc[row_idx, get_idx('smiles')] = smiles
                    pubchem_df.iloc[row_idx, get_idx('inchi')] = inchi
                    pubchem_df.iloc[row_idx, get_idx(
                        maf_compound_name_column)] = original_comp_name  # name / metabolite_identification from MAF
                    if not exiting_pubchem_file:
                        pubchem_df.iloc[row_idx, get_idx('row_id', pubchem_df_headers)] = ''
                        # if not database_id:
                        #     pubchem_df.iloc[row_idx, get_idx('combination')] = ''  # Sorting cluster
                    pubchem_df.iloc[row_idx, get_idx('search_flag', pubchem_df_headers)] = ''
                    pubchem_df.iloc[row_idx, get_idx(alt_name_column,
                                                     pubchem_df_headers)] = alt_name  # alt_name, for us to override the submitted name

                    if name:
                        pubchem_df.iloc[row_idx, get_idx('search_type',
                                                         pubchem_df_headers)] = 'plugin_search'  # Search category/type for logging
                        if database_identifier:
                            if database_identifier.startswith('CHEBI:'):
                                chebi_found = True
                                print_log("    -- Found ChEBI id " + database_identifier + " using MetaboLights search")
                        #   elif get_relevant_synonym(comp_name):  # Do we know the source, ie. KEGG, HMDB?
                        #         chebi_id, inchi, inchikey, name, smiles, chemical_formula, search_type = direct_chebi_search(
                        #            final_inchi_key, comp_name, search_type="external_db")
                        #     maf_df.iloc[row_idx, get_idx(database_identifier_column)] = database_identifier
                        # if chemical_formula:
                        #     maf_df.iloc[row_idx, get_idx('chemical_formula')] = chemical_formula
                        # if smiles:
                        #     maf_df.iloc[row_idx, get_idx('smiles')] = smiles
                        # if inchi:
                        #     maf_df.iloc[row_idx, get_idx('inchi')] = inchi

                if not chebi_found:  # We could not find this in ChEBI, let's try other sources

                    # First, see if we cat find a PubChem compound
                    pc_name, pc_inchi, pc_inchi_key, pc_smiles, pc_cid, pc_formula, pc_synonyms, \
                    where_found, first_synonym = pubchem_search(comp_name, search_type='name',
                                                                search_category='compound')

                    pubchem_df.iloc[row_idx, get_idx('search_type',
                                                     pubchem_df_headers)] = where_found  # Search category/type for logging
                    cactus_stdinchikey = cactus_search(comp_name, 'stdinchikey')
                    opsin_stdinchikey = opsin_search(comp_name, 'stdinchikey')
                    cactus_smiles = cactus_search(comp_name, 'smiles')
                    opsin_smiles = opsin_search(comp_name, 'smiles')
                    cactus_inchi = cactus_search(comp_name, 'stdinchi')
                    opsin_inchi = opsin_search(comp_name, 'stdinchi')
                    cactus_synonyms = cactus_search(comp_name, 'names')  # Synonyms

                    ik = cactus_stdinchikey
                    if pc_inchi_key:
                        ik = pc_inchi_key
                        print_log("    -- Searching ChemSpider using PubChem InChIKey, not Cactus")
                    csid = get_csid(ik)

                    pubchem_df.iloc[row_idx, get_idx('iupac_name', pubchem_df_headers)] = pc_name  # PubChem name

                    if final_cid and pc_cid != final_cid:
                        pc_cid = final_cid

                    if not final_cid:
                        final_cid = pc_cid
                    pubchem_df.iloc[row_idx, get_idx(final_cid_column_name,
                                                     pubchem_df_headers)] = final_cid  # Final PubChem CID (and other external id's manually added)
                    pubchem_df.iloc[row_idx, get_idx('pubchem_cid', pubchem_df_headers)] = pc_cid  # PubChem CID
                    if not final_cid:
                        final_cid = get_pubchem_cid_on_inchikey(cactus_stdinchikey, opsin_stdinchikey)
                        pubchem_df.iloc[row_idx, get_idx(final_cid_column_name,
                                                         pubchem_df_headers)] = final_cid  # Final PubChem CID should now be the cactus or opsin

                    # if no pc_name and/or pc_synonyms, then use the final_cid to query pubchem again
                    if final_cid and (not pc_name or not pc_synonyms):
                        pc_name, pc_inchi, pc_inchi_key, pc_smiles, pc_cid, pc_formula, pc_synonyms, \
                        where_found, first_synonym = pubchem_search(final_cid, search_type='cid', search_category='cid')
                        if pc_name:
                            pubchem_df.iloc[row_idx, get_idx('iupac_name', pubchem_df_headers)] = pc_name
                        if pc_synonyms:
                            pubchem_df.iloc[row_idx, get_idx('pubchem_synonyms', pubchem_df_headers)] = pc_synonyms

                    pubchem_df.iloc[row_idx, get_idx('pubchem_cid_ik', pubchem_df_headers)] = pc_cid  # PubChem CID
                    pubchem_df.iloc[
                        row_idx, get_idx('csid_ik', pubchem_df_headers)] = csid  # ChemSpider ID (CSID) from INCHI
                    final_smiles, source_found = get_ranked_values(pc_smiles, cactus_smiles, opsin_smiles,
                                                                   None)  # final smiles
                    pubchem_df.iloc[row_idx, get_idx('final_smiles', pubchem_df_headers)] = final_smiles
                    final_inchi, source_found = get_ranked_values(pc_inchi, cactus_inchi, opsin_inchi,
                                                                  None)  # final inchi
                    pubchem_df.iloc[row_idx, get_idx('final_inchi', pubchem_df_headers)] = final_inchi
                    final_inchi_key, source_found = get_ranked_values(pc_inchi_key, cactus_stdinchikey,
                                                                      opsin_stdinchikey, None)  # final inchikey
                    pubchem_df.iloc[row_idx, get_idx('final_inchi_key', pubchem_df_headers)] = final_inchi_key
                    print_log('checking for csid')
                    print_log(csid)
                    if final_inchi_key and csid == "":
                        print_log("    -- Searching ChemSpider using final_inchi_key")
                        csid = get_csid(final_inchi_key)
                        pubchem_df.iloc[
                            row_idx, get_idx('csid_ik', pubchem_df_headers)] = csid

                    if not pc_inchi:
                        # We don't have a PubChem compound, but we may find a uncurated substance record
                        print_log("    -- Searching for PubChem substance as we did not find a compound")
                        pc_name, pc_inchi, pc_inchi_key, pc_smiles, pc_cid, pc_formula, pc_synonyms, \
                        where_found, first_synonym = pubchem_search(comp_name, search_type='name',
                                                                    search_category='substance')

                        if pc_name:
                            pubchem_df.iloc[
                                row_idx, get_idx('iupac_name', pubchem_df_headers)] = pc_name  # PubChem substance name

                        if not final_cid and pc_cid:
                            pubchem_df.iloc[row_idx, get_idx(final_cid_column_name)] = final_cid
                            pubchem_df.iloc[
                                row_idx, get_idx('pubchem_cid_ik', pubchem_df_headers)] = pc_cid  # PubChem CID

                        if not final_inchi and pc_inchi:
                            pubchem_df.iloc[row_idx, get_idx('final_inchi', pubchem_df_headers)] = pc_inchi

                        if not cactus_stdinchikey and pc_inchi_key:
                            print_log("    -- Searching ChemSpider using PubChem Substance InChIKey")
                            csid = get_csid(pc_inchi_key)
                            pubchem_df.iloc[row_idx, get_idx('csid_ik', pubchem_df_headers)] = csid

                    pubchem_df.iloc[row_idx, get_idx('pubchem_smiles', pubchem_df_headers)] = pc_smiles  # pc_smiles
                    pubchem_df.iloc[
                        row_idx, get_idx('cactus_smiles', pubchem_df_headers)] = cactus_smiles  # cactus_smiles
                    pubchem_df.iloc[row_idx, get_idx('opsin_smiles', pubchem_df_headers)] = opsin_smiles  # opsin_smiles
                    pubchem_df.iloc[row_idx, get_idx('pubchem_inchi', pubchem_df_headers)] = pc_inchi  # PubChem inchi
                    pubchem_df.iloc[row_idx, get_idx('cactus_inchi', pubchem_df_headers)] = cactus_inchi  # Cactus inchi
                    pubchem_df.iloc[row_idx, get_idx('opsin_inchi', pubchem_df_headers)] = opsin_inchi  # Opsin inchi
                    pubchem_df.iloc[
                        row_idx, get_idx('pubchem_inchi_key', pubchem_df_headers)] = pc_inchi_key  # PubChem stdinchikey
                    pubchem_df.iloc[row_idx, get_idx('cactus_inchi_key',
                                                     pubchem_df_headers)] = cactus_stdinchikey  # cactus_stdinchikey
                    pubchem_df.iloc[row_idx, get_idx('opsin_inchi_key',
                                                     pubchem_df_headers)] = opsin_stdinchikey  # opsin_stdinchikey
                    pubchem_df.iloc[
                        row_idx, get_idx('pubchem_formula', pubchem_df_headers)] = pc_formula  # PubChem formula
                    pubchem_df.iloc[
                        row_idx, get_idx('pubchem_synonyms', pubchem_df_headers)] = pc_synonyms  # PubChem synonyms
                    pubchem_df.iloc[row_idx, get_idx('pubchem_first_synonym',
                                                     pubchem_df_headers)] = first_synonym  # PubChem first synonym, ie. the compound name
                    #if not cactus_synonyms:
                        #cactus_synonyms = alt_name

                    pubchem_df.iloc[row_idx, get_idx('cactus_synonyms',
                                                     pubchem_df_headers)] = cactus_synonyms  # add cactus_synonyms

                    if final_cid:
                        glytoucan_id = get_glytoucan_id(final_cid, glytoucan_file_df)
                        if glytoucan_id:
                            pubchem_df.iloc[row_idx, get_idx('glytoucan_id', pubchem_df_headers)] = glytoucan_id
                            # GlyToucan id for ChEBI/UniProt colab

                    pubchem_df.iloc[row_idx, get_idx(final_cid_column_name, pubchem_df_headers)] = final_cid
                    if csid:
                        db_acc = 'ChemSpider:' + csid + ';'
                    if pc_synonyms:
                        db_acc = db_acc + ';' + pc_synonyms + ';'
                    if cactus_synonyms:
                        if pc_synonyms is None:
                            db_acc = db_acc + ';' +  cactus_synonyms
                        else:
                            cactus_synonyms = get_valid_synonyms(cactus_synonyms, pc_synonyms)
                            if cactus_synonyms:
                                db_acc = db_acc + ';'+ cactus_synonyms
                    # Cactus and PubChem synonyms for SDF export
                    pubchem_df.iloc[
                        row_idx, get_idx('direct_parent', pubchem_df_headers)] = ''  # direct_parent from ClassyFire
                    pubchem_df.iloc[
                        row_idx, get_idx('source_metabolights', pubchem_df_headers)] = study_id  # MTBLS accession

                    # Now we may have more information, so let's try to search ChEBI again
                    # search_type = ''
                    if final_inchi_key and len(final_inchi_key) > 0:
                        chebi_id, inchi, inchikey, name, smiles, formula, search_type = direct_chebi_search(
                            final_inchi_key, comp_name, search_type="inchi")
                    elif get_relevant_synonym(comp_name):  # Do we know the source, ie. KEGG, HMDB?
                        chebi_id, inchi, inchikey, name, smiles, formula, search_type = direct_chebi_search(
                            final_inchi_key, comp_name, search_type="external_db")
                    else:
                        chebi_id, inchi, inchikey, name, smiles, formula, search_type = direct_chebi_search(
                            final_inchi_key, comp_name, search_type="synonym")

                    if chebi_id:
                        database_identifier = chebi_id
                        chemical_formula = formula

                        print_log("    -- Found '" + name + "', ChEBI id " + database_identifier +
                                  " based on direct search, type: " + search_type)
                        pubchem_df.iloc[row_idx, get_idx(database_identifier_column)] = database_identifier
                        pubchem_df.iloc[row_idx, get_idx('chemical_formula')] = chemical_formula
                        pubchem_df.iloc[row_idx, get_idx('smiles')] = smiles
                        pubchem_df.iloc[row_idx, get_idx('inchi')] = inchi
                        # 4 is name / metabolite_identification from MAF
                        pubchem_df.iloc[row_idx, get_idx('search_type',
                                                         pubchem_df_headers)] = search_type  # ChEBI search category/type for logging

                    else:
                        # Now, if we still don't have a ChEBI accession, download the structure (SDF) from PubChem
                        # and the classyFire SDF
                        sdf_file_list, classyfire_id = get_sdf(anno_sub_folder_path, str(final_cid), pc_name,
                                                               sdf_file_list, final_inchi, classyfire_search)
                        pubchem_df.iloc[row_idx, get_idx('classyfire_search_id', pubchem_df_headers)] = str(
                            classyfire_id)

                if not exiting_pubchem_file:
                    pubchem_df.iloc[row_idx, get_idx('row_id', pubchem_df_headers)] = row_idx + 1  # Row id
                    # if not database_id:
                    #     pubchem_df.iloc[row_idx, get_idx('combination')] = row_idx + 1  # Sorting cluster

                pubchem_df.iloc[row_idx, get_idx(search_flag, pubchem_df_headers)] = '1'  # Search flag
                print_log("    -- Search took %s seconds" % round(time.time() - start_time, 2))
                changed = True
        else:
            pubchem_df.iloc[row_idx, get_idx(search_flag,
                                             pubchem_df_headers)] = '1'  # Search flag set so we don't search for unknown again
            print_log("    -- Skipping. Already found or no database id/compound name to search for: '"
                      + database_id + "' '" + comp_name + "'", run_silently)

        pubchem_df.iloc[row_idx, get_idx(search_flag,
                                         pubchem_df_headers)] = '1'  # Search flag set so we don't search for unknown again

        if not org_row_id:
            pubchem_df.iloc[row_idx, get_idx('row_id',
                                             pubchem_df_headers)] = row_idx + 1  # Row id added if copy and no search results
        if db_acc:
            db_acc = ';'.join(unique_list(db_acc.split(';')))
            pubchem_df.iloc[row_idx, get_idx('DATABASE_ACCESSION', pubchem_df_headers)] = db_acc.rstrip(
                ';')

        final_inchi_key = pubchem_df.iloc[idx, get_idx('final_inchi_key', pubchem_df_headers)]
        if final_inchi_key and len(final_inchi_key) > 0:
            unichem_id = pubchem_df.iloc[row_idx, get_idx('unichem_id', pubchem_df_headers)]
            unichem_id_filtered = unichem_id
            if not unichem_id:
                unichem_id = processUniChemResponseAll(final_inchi_key)
                pubchem_df.iloc[row_idx, get_idx('unichem_id', pubchem_df_headers)] = unichem_id.rstrip(
                    ';')
                unichem_id_filtered =  processUniChemResponse(final_inchi_key)

            cas_id = pubchem_df.iloc[row_idx, get_idx('cas_id', pubchem_df_headers)]
            if not cas_id:
                cas_id = get_cas_id(final_inchi_key)
                if not cas_id :
                    pubchem_synonyms = pubchem_df.iloc[row_idx, get_idx('pubchem_synonyms', pubchem_df_headers)]
                    if pubchem_synonyms:
                        cas_id = get_cas_from_synonyms(pubchem_synonyms)
                    if not cas_id:
                        cactus_synonyms = pubchem_df.iloc[row_idx, get_idx('cactus_synonyms', pubchem_df_headers)]
                        if cactus_synonyms:
                            cas_id = get_cas_from_synonyms(cactus_synonyms)
                pubchem_df.iloc[row_idx, get_idx('cas_id', pubchem_df_headers)] = cas_id

            dime_id = pubchem_df.iloc[row_idx, get_idx('dime_id', pubchem_df_headers)]
            if not dime_id:
                if dime_db_api_active:
                    dimedb_search_status = check_dime_db_api()
                    if not dimedb_search_status:
                        print_log('Dime DB API failed, Trying 2nd time! ')
                        dimedb_search_status = check_dime_db_api()
                        if not dimedb_search_status:
                            print_log(' Trying DimeDB API 3rd time! ')
                            dimedb_search_status = check_dime_db_api(inchi_key='')
                            if not dimedb_search_status:
                                print_log(' Dime DB API is down, skipping for rest of the run. ')
                                dime_db_api_active = False
                            else:
                                dime_id = get_dime_db(final_inchi_key)
                                pubchem_df.iloc[row_idx, get_idx('dime_id', pubchem_df_headers)] = dime_id
                        else:
                            dime_id = get_dime_db(final_inchi_key)
                            pubchem_df.iloc[row_idx, get_idx('dime_id', pubchem_df_headers)] = dime_id
                    else:
                        dime_id = get_dime_db(final_inchi_key)
                        pubchem_df.iloc[row_idx, get_idx('dime_id', pubchem_df_headers)] = dime_id
                    
            update_db_acc = db_acc.strip(';')
            if update_db_acc:
                update_db_acc = update_db_acc.rstrip(';') + ';' + unichem_id_filtered.rstrip(';')
            else:
                update_db_acc = unichem_id_filtered.rstrip(';')

            if update_db_acc.rstrip(';'):
                update_db_acc = update_db_acc.rstrip(';') + ';' + dime_id.rstrip(';')
            else:
                update_db_acc = dime_id.rstrip(';')

            db_acc = update_db_acc.strip(';')
            db_acc = ';'.join(unique_list(db_acc.split(';')))
            pubchem_df.iloc[row_idx, get_idx('DATABASE_ACCESSION', pubchem_df_headers)] = db_acc.rstrip(
                ';')
            pubchem_df.iloc[row_idx, get_idx('CAS_NO', pubchem_df_headers)] = cas_id

        if changed and row_idx > 0 and row_idx % 20 == 0:  # Save every 20 rows
            pubchem_file = short_file_name + pubchem_end
            write_tsv(pubchem_df, pubchem_file)
            print_log('------------------------------------')
            print_log('Updating PubChem and annotated file. Record ' + str(idx + 1) + ' of ' + str(new_maf_len))
            print_log('------------------------------------')
        row_idx += 1

    pubchem_df = re_sort_pubchem_file(pubchem_df)

    # Update the submitted MAF in the chebi sub-folder, before adding species
    chebi_folder = anno_sub_folder_path
    
    update_original_maf(maf_df=maf_df, pubchem_df=pubchem_df,
                        original_maf_name=original_maf_name, study_location=chebi_folder,
                        update_study_maf=update_study_maf)

    if update_study_maf:  # Also update MAF in study folder
        pubchem_df = re_sort_pubchem_file(pubchem_df)
        update_original_maf(maf_df=maf_df, pubchem_df=pubchem_df,
                            original_maf_name=original_maf_name, study_location=study_metadata_location,
                            update_study_maf=update_study_maf)

    if not exiting_pubchem_file:
        pubchem_df = reindex_row_id(pubchem_df, pubchem_df_headers)
        # Add sample into to all rows, also duplicate rows for all samples
        pubchem_df = populate_sample_rows(pubchem_df, study_id, user_token, study_metadata_location)

    pubchem_file = short_file_name + pubchem_end
    # write_tsv(pubchem_df, pubchem_file)
    # pubchem_df = re_sort_pubchem_file(pubchem_df)

    annotated_study_location = anno_sub_folder_path
    
    classy_file_downoaded = update_sdf_file_info(pubchem_df, annotated_study_location, short_file_name + classyfire_end, classyfire_search,
                         study_id, pubchem_file, pubchem_df_headers)
    write_tsv(pubchem_df, pubchem_file)
    concatenate_sdf_files(pubchem_df, annotated_study_location, short_file_name + complete_end, run_silently)
    change_access_rights(study_metadata_location, anno_sub_folder_path)
    pubchem_df_len = str(len(pubchem_df))
    
    if dime_db_api_active:
        dimedb_search_status = 'Success'
    else:
        dimedb_search_status = 'Failure'
        
    print_log("Finishing Pipeline run!!!! ")
    print_log(f"dimedb_search_status : {str(dimedb_search_status)}")
    print_log(f"cactus_search_status : {str(cactus_search_status)}")
    print_log(f"opsin_search_status : {str(opsin_search_status)}")
    print_log(f"chemspider_search_status : {str(chemspider_search_status)}")
    print_log(f"unichem_search_status : {str(unichem_search_status)}")
    print_log(f"classyfire_search_status : {str(classyfire_search_status)}")
    print_log(f"Classyfire SDF file downloaded : {str(classy_file_downoaded)}")
    print_log(f"Chebi API status : {str(chebi_search_status)}")
    print_log(f"ChEBI pipeline Done. Overall it took %s seconds" % round(time.time() - first_start_time, 2))

    folder_name = get_settings().chebi.pipeline.chebi_annotation_sub_folder
    internal_files_path = os.path.join(study_metadata_location, settings.internal_files_symbolic_link_name)
    zip_file_folder_path  = os.path.dirname(anno_sub_folder_path)
    compressed_chebi_annotations_folder_path = os.path.join(zip_file_folder_path, f"{folder_name}.zip")
    try:
        if os.path.exists(compressed_chebi_annotations_folder_path):
            compressed_chebi_annotations_folder_archive_path = os.path.join(zip_file_folder_path, f"{folder_name}_old.zip")
            shutil.move(compressed_chebi_annotations_folder_path, compressed_chebi_annotations_folder_archive_path)
    except Exception as e:
        logger.error("chebi folder zip move error")
        print(f"An error occurred: {e}")
    try:             
        with zipfile.ZipFile(compressed_chebi_annotations_folder_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for foldername, subfolders, filenames in os.walk(anno_sub_folder_path):
                for filename in filenames:
                    file_path = os.path.join(foldername, filename)
                    rel_path = os.path.relpath(file_path, anno_sub_folder_path)
                    zipf.write(file_path, rel_path)
    except Exception as e:
        logger.error("chebi folder zip error")
        print(f"An error occurred: {e}")
            
    if obfuscation_code:

        ftp_private_folder = os.path.join(study_id.lower() + "-" + obfuscation_code)
        
        pipeline_settings = get_settings().chebi.pipeline
        target_ftp_path = os.path.join(get_settings().hpc_cluster.datamover.mounted_paths.cluster_private_ftp_root_path, ftp_private_folder, settings.internal_files_symbolic_link_name)
        include_list = [f"{pipeline_settings.chebi_annotation_sub_folder}", 
                        f"{pipeline_settings.chebi_annotation_sub_folder}.zip",
                        f"{pipeline_settings.chebi_annotation_sub_folder}/***"]
        commands=[f"mkdir -p {target_ftp_path}"]
        
        rsync_command = HpcRsyncWorker.build_rsync_command(
            f"{internal_files_path}/", f"{target_ftp_path}/", include_list=include_list, exclude_list=["*"], rsync_arguments="-auv"
        )
        commands.append(f"chmod -R 750 {target_ftp_path}")
        commands.append(rsync_command)
        command = " && ".join(commands)
        
        inputs = {"command": command, "email": email, "task_name": task_name}
        task = execute_bash_command.apply_async(kwargs=inputs, expires=60*5)
        print_log(f"ChEBI pipeline FTP rsync task is started. Task id: {task.id}")
        print_log(f"Command: {json.dumps(inputs, indent=4)}")
        # try:
        #     output = task.get(timeout=get_settings().hpc_cluster.configuration.task_get_timeout_in_seconds*3)
        #     print_log(f"ChEBI pipeline FTP rsync is completed.\n {output}")
        # except Exception as ex:
        #         print_log(f"ChEBI pipeline FTP rsync is not completed. {str(ex)}")
        
    return maf_len, pubchem_df_len, pubchem_file,dimedb_search_status,cactus_search_status,opsin_search_status,chemspider_search_status,unichem_search_status,classyfire_search_status,classy_file_downoaded,chebi_search_status,chebi_master_list_initialised, chebi_search_manager_initiated


def update_original_maf(maf_df=None, pubchem_df=None, original_maf_name=None, study_location=None,
                        update_study_maf=None):
    # pass in the "annotation_file_name" to update a copy of the original maf
    if original_maf_name and study_location:
        # Drop all duplicates in the new DF, added for unique organism/part
        # pubchem_df.drop_duplicates(keep='first', inplace=True)
        # ToDo. Rather than removing duplicates, only add rows with "combination" counter = (NaN, null, 0) or 1
        for column_name in [database_identifier_column, 'chemical_formula', 'smiles', 'inchi',
                            maf_compound_name_column]:
            maf_df[column_name] = pubchem_df[column_name]

        # Update the original MAF in the study folder
        write_tsv(maf_df, os.path.join(study_location, original_maf_name))


def change_access_rights(study_location, anno_sub_folder_path):
    chmode = 0o777
    chebi_folder = anno_sub_folder_path
    print_log("Changing access right")
    try:
        os.chmod(study_location, chmode)
        for m_name in glob.glob(os.path.join(study_location, "m_*.tsv")):
            os.chmod(os.path.join(chebi_folder, m_name), chmode)

        os.chmod(chebi_folder, chmode)
        for f_name in glob.glob(os.path.join(chebi_folder, "*")):
            os.chmod(os.path.join(chebi_folder, f_name), chmode)

    except Exception as e:
        print_log("Error : Changing access right not  for " + m_name)


def re_sort_pubchem_file(pubchem_df):
    pubchem_df.row_id = pd.to_numeric(pubchem_df.row_id, errors='coerce')
    pubchem_df = pubchem_df.sort_values(by='row_id', axis=0, ascending=True)
    return pubchem_df


def reindex_row_id(pubchem_df, pubchem_df_headers):
    for idx, row in pubchem_df.iterrows():
        pubchem_df.iloc[idx, get_idx('row_id', pubchem_df_headers)] = idx + 1  # Row id
    return pubchem_df


def create_annotation_folder_on_remote_storage(storage: Storage, folder_loc):
    print_log("Checking for ChEBI folder in FTP " + folder_loc)
    try:
        if not storage.remote.does_folder_exist(folder_loc):
            print_log(f"Creating ChEBI folder {folder_loc} on storage {storage.get_name()}")
        storage.remote.create_folder(folder_loc, acl=Acl.AUTHORIZED_READ_WRITE, exist_ok=True)
    except Exception as e:
        print_log(str(e))


def sync_annotation_folder_with_remote_storage(storage: Storage, ftp_folder):
    print_log("Syncing ChEBI annotation folder with FTP " + ftp_folder)
    try:
        storage.sync_from_local(source_local_folder=None, target_folder=ftp_folder, ignore_list=None, sync_chebi_annotation=True)
    except Exception as e:
        print_log(str(e))

def create_annotation_folder(folder_loc):
    print_log("Checking for ChEBI folder in study folder" + folder_loc)
    try:
        if not os.path.exists(folder_loc):
            print_log("Creating ChEBI folder " + folder_loc)
            os.makedirs(folder_loc, exist_ok=True)
    except Exception as e:
        print_log(str(e))


def update_sdf_file_info(pubchem_df, study_location, classyfire_file_name, classyfire_search,
                         study_id, pubchem_file_name, pubchem_df_headers):
    return_format = 'sdf'  # 'json' will require a new root element to separate the entries before merging
    classyfire_file_name = classyfire_file_name + '.' + return_format
    classyfire_df = get_classyfire_lookup_mapping()
    file_changed = False
    classy_file_downoaded = 'No'
    rowIdx = 0
    cluster_ids = []
    for idx, row in pubchem_df.iterrows():
        cid = row[final_cid_column_name]
        db_id = row[database_identifier_column]
        name = row[maf_compound_name_column]
        cf_id = row["classyfire_search_id"]
        organism = row['ORGANISM']
        organism_part = row['ORGANISM_PART']
        iupac_name = row['IUPAC_NAME']
        strain = row['STRAIN']
        reference = row['REFERENCE']
        direct_parent = row['RELATIONSHIP']
        comment = row['COMMENT']
        pmid = row['SOURCE_PMID']
        doi = row['SOURCE_DOI']
        row_id = row['row_id']
        chemspider = row['csid_ik']
        cactus_synonyms = row['cactus_synonyms']
        database_accession = row['DATABASE_ACCESSION']
        definition = row['DEFINITION']
        cas_id = row['cas_id']

        if cid and not db_id.startswith('CHEBI:'):
            cluster_ids.append(row_id)  # Keep count of the number of ORGANISM sections to add to ChEBI SDF file
            if cid.endswith('.mol'):
                fname = cid
            else:
                fname = cid + pubchem_sdf_extension

            full_file = os.path.join(study_location, fname)
            cluster_itr = len([p for p in cluster_ids if p == row_id])

            print_log(" -- Processing PubChem SDF file " + fname)

            if not os.path.isfile(full_file):
                try:
                    if "MTBLS" not in cid:
                        print_log(
                            '       -- PubChem file for CID ' + cid + ' is missing or final id has been added manually to the spreadsheet. Trying to download again')
                        pcp.download('SDF', full_file, cid)
                except Exception as e:
                    print_log(str(e))

            mtbls_sdf_file_name = os.path.join(study_location, 'mtbls_' + cid + pubchem_sdf_extension)
            if cluster_itr == 1:  # First time, remove some of the PubChem parameters
                mtbls_sdf_file_name = remove_pubchem_sdf_parameters(study_location, fname)

            # Now, get the classyFire queries, download sdf files
            classyfire_sdf_values,classy_sdf_downoaded = get_classyfire_results(cf_id, full_file, return_format,
                                                           classyfire_search, classyfire_df)
            if classy_sdf_downoaded:
                classy_file_downoaded = 'Yes'
                
            # merge data from Classyfire SDF into new PubChem SDF
            if classyfire_sdf_values and not row['direct_parent']:
                is_a = classyfire_sdf_values['is_a']
                direct_parent = classyfire_sdf_values['direct_parent']
                file_changed = True
                # direct_parent into "direct_parent". IS_A into "relationship"
                if direct_parent:
                    pubchem_df.iloc[rowIdx, get_idx('direct_parent',
                                                    pubchem_df_headers)] = direct_parent  # direct_parent from ClassyFire

                if is_a:
                    pubchem_df.iloc[rowIdx, get_idx('RELATIONSHIP', pubchem_df_headers)] = is_a

                add_classyfire_sdf_info(mtbls_sdf_file_name, relationships=is_a, definition=definition,
                                        name=name, iupack_name=iupac_name)
                print_log("       -- adding ancestors to SDF file " + fname)

            if chemspider:
                chemspider = 'ChemSpider:' + chemspider + ';'
                comment = comment

            # add ChemSpider (csid_ik) + any other approved synonyms (two columns) in the database acc for ChEBI SDF
            if database_accession:
                if chemspider and chemspider.strip(';') not in database_accession:
                    database_accession = database_accession + chemspider

                if cactus_synonyms and cactus_synonyms.strip(';') not in database_accession:
                    database_accession = database_accession + ";" + cactus_synonyms

            add_classyfire_sdf_info(mtbls_sdf_file_name, mtbls_accession=study_id, organism=organism,
                                    definition=definition,
                                    strain=strain, organism_part=organism_part, name=name, iupack_name=iupac_name,
                                    relationships=direct_parent, database_accession=database_accession,
                                    cluster_itr=cluster_itr, temp_id=row_id, comment=comment, reference=reference,
                                    pmid=pmid, doi=doi, cas_id=cas_id)

            if not os.path.isfile(full_file):
                if "MTBLS" not in cid:
                    print_log("       -- Will try to download SDF file for CID " + cid)
                    try:
                        pcp.download('SDF', full_file, cid, overwrite=True)  # try to pull down the sdf from PubChem
                    except Exception as e:
                        print_log("       -- Error: could not download SDF file for CID " + cid + ". " + str(e),
                                  mode='error')
        rowIdx = rowIdx + 1
    return classy_file_downoaded
    # if file_changed:
    # write_tsv(pubchem_df, pubchem_file_name)


def concatenate_sdf_files(pubchem_df, study_location, sdf_file_name, run_silently):
    print_log("Concatenating the PubChem and ClassyFire SDF information into the ChEBI submission SDF")

    final_cid_list = []
    no_hs_mol_list = []
    with open(sdf_file_name, 'w') as outfile:
        for idx, row in pubchem_df.iterrows():
            p_cid = row[final_cid_column_name]
            p_db_id = row[database_identifier_column]
            if p_cid and not p_db_id.startswith('CHEBI:') and p_cid not in final_cid_list:
                final_cid_list.append(p_cid)
                mtbls_sdf_file_name = os.path.join(study_location, 'mtbls_' + p_cid + pubchem_sdf_extension)
                if p_cid.endswith('.mol'):
                    mtbls_sdf_file_name = os.path.join(study_location, 'mtbls_' + p_cid)
                if os.path.isfile(mtbls_sdf_file_name):
                    try:
                        mole_str = ''
                        with open(mtbls_sdf_file_name, 'r', encoding="utf-8") as infile:
                            for line in infile:
                                line = unidecode(line)
                                if not line.startswith("#"):
                                    outfile.write(line)
                                    mole_str = mole_str + line
                                else:
                                    print_log("       -- Not adding: " + line.rstrip('\n'), run_silently)
                        no_hs_mol = remove_hs(molfile_str=mole_str)
                        no_hs_mol = no_hs_mol.rstrip()
                        no_hs_mol_list.append(no_hs_mol)
                    except Exception as e:
                        print_log("       -- Error, can not read SDF file (" + mtbls_sdf_file_name + ") " + str(e))
                else:
                    print_log("       -- Error: could not find SDF file: " + mtbls_sdf_file_name, mode='error')
        outfile.close()
    final_no_hs_mol = '\n'.join(no_hs_mol_list)
    no_hs_sdf_file = sdf_file_name[:-4] + "_removed_hs.sdf"
    with open(no_hs_sdf_file, 'w') as output:
            output.write(final_no_hs_mol)
    output.close()

def remove_hs(molfile_str):
    """Bespoke remove Hs function for MetaboLights team"""
    out_molfile = None
    try:
        mol = parse_molblock(molfile_str)
        Chem.FastFindRings(mol)
        mol = update_mol_valences(mol)
        indices = []
        for atom in mol.GetAtoms():
            if atom.GetAtomicNum() == 1 and not atom.GetIsotope():
                bnd = atom.GetBonds()[0]
                if not (
                    bnd.GetBondDir() in (Chem.BondDir.BEGINWEDGE, Chem.BondDir.BEGINDASH)
                ) and not (
                    bnd.HasProp("_MolFileBondStereo")
                    and bnd.GetUnsignedProp("_MolFileBondStereo") in (1, 6)
                ):
                    indices.append(atom.GetIdx())
        mol = Chem.RWMol(mol)
        for index in sorted(indices, reverse=True):
            mol.RemoveAtom(index)
        props = molfile_str.split("M  END")[1].strip()
        props = props if len(props) > 1 else None
        out_molfile = Chem.MolToMolBlock(mol)
        if props:
            out_molfile += props
    except Exception as e:
         print_log("-- Error, cannot remove hydrogen from SDF " + str(e), mode='error')
    return out_molfile

#Deprecated and Not used. Using local remove_hs instead.
def removeHydrogenCheml(sdf_file_name):
    fileSize = os.path.getsize(sdf_file_name)
    if fileSize <= 2000000:
        data = open(sdf_file_name, 'rb').read()
        res = requests.post('https://www.ebi.ac.uk/chembl/api/utils/removeHs', data=data)
        sdf_file = sdf_file_name[:-4] + "_removed_hs.sdf"
        res = res.text.replace("$$$$", '\n' + '$$$$')
        res = res.replace('\n\n\n', '\n\n')
        with open(sdf_file, 'w') as output:
            output.write(res)
    else:
        file_data = open(sdf_file_name, 'rb').read()
        fileText = ''
        count = 0
        final_file = ''
        molecule = str(file_data, 'utf-8').split('$$$$')
        mol_count = get_settings().chebi.pipeline.removed_hs_mol_count
        for mol in molecule:
            if count <= mol_count:
                fileText = fileText + mol + '$$$$'
                count += 1
            else:
                fileText = fileText + mol + '$$$$'
                if fileText[:1] == '\n':
                    fileText = fileText[1:]
                res = requests.post('https://www.ebi.ac.uk/chembl/api/utils/removeHs', data=fileText.encode('utf-8'))
                if res.status_code != 200:
                    print_log("removeHydrogen -- Error: could not remove Hydrogens" + str(res.status_code),
                              mode='error')
                    break
                final_file = final_file + res.text
                count = 0
                fileText = ''

        if fileText is not None:
            if fileText[:1] == '\n':
                fileText = fileText[1:]
            res = requests.post('https://www.ebi.ac.uk/chembl/api/utils/removeHs', data=fileText.encode('utf-8'))
            if res.status_code != 200:
                print_log("removeHydrogen -- Error: could not remove Hydrogens" + str(res.status_code), mode='error')
            final_file = final_file + res.text

        sdf_file = sdf_file_name[:-4] + "_removed_hs.sdf"
        final_file = final_file.replace("$$$$", '\n' + '$$$$')
        with open(sdf_file, 'w') as output:
            output.write(final_file)


def remove_pubchem_sdf_parameters(study_location, sdf_file_name):
    lines = []
    mtbls_sdf_file_name = os.path.join(study_location, 'mtbls_' + sdf_file_name)
    pubchem_sdf_file_name = os.path.join(study_location, sdf_file_name)
    template_start = 'M  END\n'
    template_body = '''> <ID>
TEMP_#template_temp_id#

> <NAME>
#template_name#

> <DEFINITION>
#template_definition#

> <IUPAC_NAME>
#template_iupack_name#

> <DATABASE_ACCESSION>
#template_database_accessions#

> <CAS_NO>
#template_cas#

> <REFERENCE>
#template_reference#

> <RELATIONSHIP>
#template_relationships#

> <COMMENT>
#template_comment#
$$$$
'''
    try:
        if os.path.isfile(pubchem_sdf_file_name):
            with open(pubchem_sdf_file_name, 'r+', encoding="utf-8") as infile:
                for line in infile:
                    lines.append(line)
                infile.close()

            with open(mtbls_sdf_file_name, 'w', encoding="utf-8") as outfile:
                for line in lines:
                    if line == template_start:
                        outfile.write(line)
                        outfile.write(template_body)
                        # outfile.write(get_template_sample_body(1))
                        outfile.close()
                        break
                    else:
                        outfile.write(line)
                outfile.close()
        else:
            print_log("Error, can not read PubChem SDF file " + pubchem_sdf_file_name, mode='error')

    except Exception as e:
        print_log("    -- Could not read from PubChem SDF file: " + sdf_file_name + ". Error: " + str(e), mode='error')

    return mtbls_sdf_file_name


def get_template_sample_body(itr):
    itr = str(itr)
    template = '''
> <ORGANISM#itr#>
#template_organism#

> <ORGANISM_PART#itr#>
#template_organism_part#

> <STRAIN#itr#>
#template_strain#

> <SOURCE_PMID#itr#>
#template_pmid#

> <SOURCE_DOI#itr#>
#template_doi#

> <SOURCE_ARTICLE#itr#>
#template_article#

> <SOURCE_METABOLIGHT#itr#>
#template_mtbls_accession#

$$$$'''
    return template.replace('#itr#', str(itr))


def add_classyfire_sdf_info(mtbls_pubchem_sdf_file, mtbls_accession=None, relationships=None, name=None,
                            definition=None, iupack_name=None, database_accession=None, organism=None,
                            organism_part=None, strain=None, cluster_itr=None, temp_id=None, comment=None,
                            pmid=None, article=None, reference=None, doi=None, cas_id=None):
    body_text = None
    if cluster_itr:
        body_text = get_template_sample_body(cluster_itr)

    if os.path.isfile(mtbls_pubchem_sdf_file):
        with open(mtbls_pubchem_sdf_file, 'r', encoding="utf-8") as infile:
            filedata = infile.read()
            infile.close()

            if body_text:
                filedata = filedata.replace('$$$$', body_text)

            if temp_id:
                filedata = filedata.replace('#template_temp_id#', str(temp_id))

            if name:
                filedata = filedata.replace('#template_name#', name)

            if definition:
                filedata = filedata.replace('#template_definition#', definition)

            if iupack_name:
                filedata = filedata.replace('#template_iupack_name#', iupack_name)

            if database_accession:
                database_accession = ';'.join(unique_list(database_accession.split(';')))
                filedata = filedata.replace('#template_database_accessions#', database_accession.rstrip(';'))

            if cas_id:
                filedata = filedata.replace('#template_cas#', cas_id.rstrip(';'))

            if relationships:
                filedata = filedata.replace('#template_relationships#', relationships.rstrip(';'))

            if organism:
                filedata = filedata.replace('#template_organism#', organism)

                if organism_part:
                    filedata = filedata.replace('#template_organism_part#', organism_part)

                if strain:
                    filedata = filedata.replace('#template_strain#', strain)

                if mtbls_accession:
                    filedata = filedata.replace('#template_mtbls_accession#', mtbls_accession)

                if article:
                    filedata = filedata.replace('#template_article#', article)

                if pmid:
                    filedata = filedata.replace('#template_pmid#', pmid)

                if doi:
                    filedata = filedata.replace('#template_doi#', doi)

            if comment:
                filedata = filedata.replace('#template_comment#', comment)

            if reference:
                filedata = filedata.replace('#template_reference#', reference)

        with open(mtbls_pubchem_sdf_file, 'w', encoding="utf-8") as outfile:
            outfile.write(filedata)
            outfile.close()
    else:
        print_log("File not found: " + mtbls_pubchem_sdf_file)


def remove_sdf_files(sdf_file_name, study_location, sdf_file_list):
    sdf_file = Path(sdf_file_name)
    sdf_path = None
    if sdf_file.is_file():
        for fname in sdf_file_list:
            try:
                sdf_path = Path(os.path.join(study_location, fname[0]))
                sdf_path.unlink()
            except Exception as e:
                print_log(" -- Error: Could not remove file: " + str(sdf_file) + ". Error " + str(e), mode='error')

def check_classyfire_api():
    status = 'Failure'
    try:
        url = get_settings().chebi.pipeline.search_services.classyfire_url
        print_log("Checking CLASSYFIRE API...   ", mode='info')
        url = url + '/queries/1'
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            status = 'Success'
    except Exception as e:
        print_log("CLASSYFIRE API check failed ..", mode='info')
    return status

def classyfire(inchi):
    print_log("    -- Starting querying ClassyFire")
    url = get_settings().chebi.pipeline.search_services.classyfire_url
    label = 'MetaboLights WS'
    query_id = None
    try:
        r = requests.post(url + '/queries.json',
                          data='{"label": "%s", "query_input": "%s", "query_type": "STRUCTURE"}' % (label, inchi),
                          headers={"Content-Type": "application/json"})
        r.raise_for_status()
        query_id = r.json()['id']
        print_log("    -- Got ClassyFire query id: " + str(query_id))
    except Exception as e:
        print_log("    -- Error querying ClassyFire: " + str(e), mode='error')
    return query_id


def get_classyfire_results(query_id, classyfire_file_name, return_format, classyfire_search, classyfire_df):
    all_ancestors = None
    classy_sdf_downoaded = False
    if classyfire_search and query_id and query_id != 'None':
        try:
            print_log("Searching Classyfire ..   ", mode='info')
            classyfire_file_name = classyfire_file_name.replace("_pubchem.sdf", "_classyfire.sdf")
            if classyfire_file_name.endswith('.mol'):
                classyfire_file_name = classyfire_file_name + "_classyfire.sdf"
            if not os.path.isfile(classyfire_file_name):
                # r = None
                start_time = time.time()
                print_log("       -- Getting ClassyFire SDF for query id: " + str(query_id))
                url = get_settings().chebi.pipeline.search_services.classyfire_url
                r = requests.get('%s/queries/%s.%s' % (url, query_id, return_format),
                                 headers={"Content-Type": "application/%s" % return_format})
                time.sleep(3)  # Give ClassyFire time to recover first ;-)
                r.raise_for_status()

                try:
                    if r and len(r.text) > 1:
                        text = r.text
                        with open(classyfire_file_name, 'w', encoding='utf-8') as cf_file:
                            cf_file.write(text)
                            cf_file.close()
                            classy_sdf_downoaded = True
                        print_log("       -- ClassyFire SDF download took %s seconds" % round(time.time() - start_time, 2))
                except Exception as e:
                    print_log("       -- ERROR: Could not read classyFire SDF file " + classyfire_file_name
                              + ". Error:" + str(e))
               
            else:
                print_log("       -- Already downloaded ClassyFire SDF for query id: " + str(query_id))
                
            if classyfire_search:
                all_ancestors = get_ancestors(classyfire_file_name, classyfire_df)

        except Exception as e:
            print_log("    -- ERROR: Could not get classyFire SDF for " + query_id + ". Error:" + str(e), mode='error')

    return all_ancestors,classy_sdf_downoaded


def get_ancestors(classyfire_file_name, classyfire_df):
    lines = []
    direct_parents = []
    try:
        with open(classyfire_file_name, 'r', encoding="utf-8") as infile:
            for line in infile:
                lines.append(line.rstrip('\n'))

        inchi_key = None
        parent_name = None
        is_a = None

        for idx, line in enumerate(lines):

            if line == "> <InChIKey>":
                inchi_key = lines[idx + 1]
            elif line == "> <Direct Parent>":
                parent_name = lines[idx + 1]
                if parent_name not in direct_parents:
                    direct_parents.append(parent_name)
            # elif line == "> <Alternative Parents>":

            if parent_name:
                row = classyfire_df.loc[classyfire_df['name'] == parent_name]
                p_name, is_a = row.iloc[0]  # Get the IS_A relationships

            if inchi_key and parent_name and is_a:
                return {"inchi_key": inchi_key.replace('InChiKey=', ''),
                        "direct_parent": parent_name, "c_sdf_file": classyfire_file_name, "is_a": is_a}

    except Exception as e:
        print_log("    -- Could not read from Classyfire SDF file: " + classyfire_file_name + ". Error: " + str(e))


def get_classyfire_lookup_mapping():
    assay_master_template = os.path.join(resource_folder, 'ClassyFire_Mapping_VLOOKUP.tsv')
    return read_tsv(assay_master_template)


def get_chebi_obo_file():
    print_log('loading ChEBI OBO file')
    obo_file = get_settings().chebi.pipeline.obo_file
    onto = pronto.Ontology(obo_file)
    return onto


def get_is_a(onto, chebi_compound):
    print_log('Get ChEBI parents')
    is_a_list = onto[chebi_compound].rparents()  # "Parents" are "is_a" relationships
    return is_a_list


def get_chebi_client():
    url = get_settings().chebi.service.connection.chebi_ws_wsdl
    wait_time = get_settings().chebi.pipeline.chebi_url_wait
    client = None
    retries = 20
    attempts = 0

    while not client and retries - attempts >= 0:
        try:
            client = Client(url)
        except Exception as e:
            print_log("    -- Could not set up ChEBI webservice call. " + str(e))
            print_log("      -- Trying to set up a new ChEBI webservice client in 5 minutes.")
            attempts += 1
            client = None
            time.sleep(wait_time)  # Wait as the resync of the ChEBI webservice in London can take up to 5 mins

    return client


def direct_chebi_search(final_inchi_key, comp_name, acid_chebi_id=None, search_type="inchi", loop_counter=False):
    chebi_id = ""
    inchi = ""
    inchikey = ""
    name = ""
    smiles = ""
    formula = ""
    change = None
    top_result = None
    lite_entity = None

    comp_name = clean_comp_name(comp_name)

    client: ChebiWsProxy = get_chebi_ws_proxy()
    if not client:
        print_log("    -- Could not set up any ChEBI webservice calls. ")
        abort(500, message='ERROR: Could not set up any direct ChEBI webservice calls, ChEBI WS may be down?')
        return chebi_id, inchi, inchikey, name, smiles, formula, search_type

    try:
        if search_type == "inchi" and final_inchi_key:
            print_log(
                "    -- Querying ChEBI web services for '" + comp_name + "' based on final InChIKey " + final_inchi_key)
            lite_entity = client.service.getLiteEntity(final_inchi_key, 'INCHI_INCHI_KEY', '10', 'ALL')
        elif search_type == "external_db" and comp_name:
            print_log("    -- Querying ChEBI web services for '" + comp_name + "' using external database id search")
            lite_entity = client.service.getLiteEntity(comp_name, 'DATABASE_LINK_REGISTRY_NUMBER_CITATION', '10', 'ALL')
        elif search_type == "synonym" and comp_name:
            print_log("    -- Querying ChEBI web services for '" + comp_name + "' using synonym search")
            lite_entity = client.service.getLiteEntity(comp_name, 'ALL_NAMES', '10', 'ALL')
        elif search_type == "get_conjugate_acid" and acid_chebi_id:
            lite_entity = client.service.getAllOntologyChildrenInPath(acid_chebi_id, 'is conjugate acid of', False)
        elif search_type == "is_a":
            lite_entity = client.service.getAllOntologyChildrenInPath(acid_chebi_id, 'is a', False)

        if lite_entity and lite_entity[0]:
            top_result = lite_entity[0]
        else:
            if final_inchi_key and len(final_inchi_key) > 0:
                comp_name = clean_comp_name(comp_name)
                print_log("    -- Querying ChEBI web services for " + comp_name + " using ChEBI name search")
                lite_entity = client.service.getLiteEntity(final_inchi_key, 'CHEBI_NAME', '10', 'ALL')
                if lite_entity:
                    top_result = lite_entity[0]

        if top_result:
            chebi_id = top_result.chebiId
            print_log('    -- Found ChEBI compound ' + chebi_id)
            complete_entity = client.service.getCompleteEntity(chebi_id)
            inchi = complete_entity.inchi
            inchikey = complete_entity.inchiKey
            name = complete_entity.chebiAsciiName
            smiles = complete_entity.smiles
            charge = complete_entity.charge
            if complete_entity.Formulae and complete_entity.Formulae[0]:
                formula = complete_entity.Formulae[0].data

    except Exception as e:
        print_log('    -- Error querying ChEBI. Error ' + str(e), mode='error')

    # if formula and formula.endswith('-') and not acid_chebi_id:
    if formula and charge and '-' in charge and not loop_counter:
        # Need to get the conjugate acid compound of this base compound
        # Only call if we do not have the acid_chebi_id, otherwise it may loop (maybe...)
        print_log("    -- Searching final time for conjugate acid of " + chebi_id)
        # Set the loop_counter to True to stop if trying yet again if there is no conjugate acid of this compound
        return direct_chebi_search(final_inchi_key, comp_name,
                                   acid_chebi_id=chebi_id, search_type="get_conjugate_acid", loop_counter=True)

    return chebi_id, inchi, inchikey, name, smiles, formula, search_type

def check_chemspider_api(inchikey='BSYNRYMUTXBXSQ-UHFFFAOYSA-N'):
    chemspider_search_status = 'Failure'
    csurl_base = 'http://www.chemspider.com/InChI.asmx/InChIKeyToCSID?inchi_key='
    try:
        url = csurl_base + inchikey
        print_log("Checking Chemspider API..   ", mode='info')
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            chemspider_search_status = 'Success'
    except Exception as e:
         print_log("Chem spider API is down ",mode = 'info')
    return chemspider_search_status

def get_csid(inchikey):
    csid = ""
    csurl_base = 'http://www.chemspider.com/InChI.asmx/InChIKeyToCSID?inchi_key='
    try:
        if inchikey:
            url1 = csurl_base + inchikey
            print_log("Searching Chemspider...   ", mode='info')
            resp1 = requests.get(url1, timeout=10)
            if resp1.status_code == 200:
                csid = resp1.text
                # Something like: '<?xml version="1.0" encoding="utf-8"?><string xmlns="http://www.chemspider.com/">4471938</string>'
                if csid:
                    try:
                        csid = csid.split('www.chemspider.com/">')[1]
                        csid = csid.replace('</string>', '')
                        print_log("    -- Found CSID " + csid + " using ChemSpider, inchikey: " + inchikey)
                        return csid
                    except IndexError:
                        print_log("    -- Could not find CSID in ChemSpider, inchikey: " + inchikey)
                        return ""
    except Exception as e:
         print_log("Chemspider API search failure.. ",mode = 'info')
    return csid


# def get_csid(inchikey):
#     csid = ""
#     csurl_base = unichem_url = get_settings().chebi.pipeline.
#
#     if inchikey:
#         url1 = csurl_base + 'SimpleSearch&searchOptions.QueryText=' + inchikey
#         resp1 = requests.get(url1)
#         if resp1.status_code == 200:
#             url2 = csurl_base + 'GetSearchResult&rid=' + resp1.text
#             resp2 = requests.get(url2)
#             if resp2.status_code == 200:
#                 csid = resp2.text
#                 csid = csid.replace('[', '').replace(']', '').split(',')[0]
#                 print_log("    -- Found CSID " + csid + " using ChemSpider, inchikey: " + inchikey)
#                 return csid
#     return csid


def get_pubchem_cid_on_inchikey(cactus_stdinchikey, opsin_stdinchikey):
    pc_cid = ''
    inchis = []

    if cactus_stdinchikey or opsin_stdinchikey:
        msg = "    -- Searching PubChem using "
        if cactus_stdinchikey:
            inchis = [cactus_stdinchikey]
            msg = msg + "Cactus '" + cactus_stdinchikey
        if opsin_stdinchikey and cactus_stdinchikey != opsin_stdinchikey:
            inchis = [cactus_stdinchikey, opsin_stdinchikey]
            msg = msg + "' and/or Opsin '" + opsin_stdinchikey

        msg = msg + "' InChIKey"

        if inchis:
            print_log(msg)
            for inchikey in inchis:
                if inchikey:
                    pc_name, pc_inchi, pc_inchi_key, pc_smiles, pc_cid, pc_formula, pc_synonyms, from_where, first_synonym = \
                        pubchem_search(inchikey, search_type='inchikey')
                    if pc_cid:
                        return pc_cid
    return pc_cid


def get_ranked_values(pubchem, cactus, opsin, chemspider):
    if pubchem:
        return pubchem, 'pubchem'
    elif cactus:
        return cactus, 'cactus'
    elif opsin:
        return opsin, 'opsin'
    elif chemspider:
        return chemspider, 'chemspider'
    else:
        return '', ''


def create_pubchem_df(maf_df):
    # These are simply the fixed spreadsheet column headers
    # Copy existing values from the MAF
    pubchem_df = maf_df[[database_identifier_column, 'chemical_formula', 'smiles', 'inchi', maf_compound_name_column]]
    # add the rest of the rows
    for idx, name in enumerate(spreadsheet_fields):
        if idx > 4:
            pubchem_df[name] = ''

    return pubchem_df

def check_chebi_api():
    status = 'Failure'
    try:
        url = get_settings().chebi.service.connection.chebi_ws_wsdl
        
        print_log("Checking CHEBI API...   ", mode='info')
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            status = 'Success'
    except Exception as e:
        print_log("Chebi API check failed ..", mode='info')
    return status

def check_opsin_api(comp_name):
    status = 'Failure'
    try:
        opsin_url = get_settings().chebi.pipeline.search_services.opsin_url
        url = opsin_url + comp_name + '.json'
        print_log("Checking OPSIN API...   ", mode='info')
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            status = 'Success'
    except Exception as e:
        print_log("OPSIN API check failed ..", mode='info')
    return status

def opsin_search(comp_name, req_type):
    result = ""
    try:
        opsin_url = get_settings().chebi.pipeline.search_services.opsin_url
        url = opsin_url + comp_name + '.json'
        print_log("Searching OPSIN ...   ", mode='info')
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            try:
                json_resp = resp.json()
                result = json_resp[req_type]
            except Exception as ex:
                logger.warning(f"Invalid result from {url}  {str(ex)}")
    except Exception as e:
        print_log("OPSIN Search failed ..", mode='info')
    return result

def check_cactus_api(comp_name, search_type='stdinchikey'):
    status = 'Failure'
    try:
        result = cirpy.resolve(comp_name, search_type)
        if result:
            status = 'Success'
    except Exception as e:
        print_log("    -- ERROR: Cactus API failed! " + str(e), mode='error')
    return status

def cactus_search(comp_name, search_type):
    result = None
    if comp_name is None:
        return None

    try:
        print_log("Searching Cactus ...   ", mode='info')
        result = cirpy.resolve(comp_name, search_type)
        synonyms = ""
    except Exception as e:
        print_log("    -- ERROR: Cactus search failed! " + str(e), mode='error')
        return result

    if result:
        if search_type == 'stdinchikey':
            return result.replace('InChIKey=', '')
        if search_type == 'names':
            for synonym in result:
                if get_relevant_synonym(synonym.strip()):
                    synonyms = synonyms + ';' + add_database_name_synonym(synonym)
            return synonyms.replace(";", "", 1)  # Remove the leading ";"

    return result


def add_database_name_synonym(synonym):
    if synonym.startswith('LM'):  # LipidMaps
        return "LIPID MAPS:" + synonym

    elif synonym.startswith('C') and is_correct_int(synonym[1:], 5):  # KEGG Compound
        return "KEGG COMPOUND:" + synonym

    elif synonym.startswith('D') and synonym[1:2] != '-' and is_correct_int(synonym[1:], 5):  # KEGG Drug
        return "KEGG DRUG:" + synonym

    elif synonym.startswith('G') and is_correct_int(synonym[1:], 5):  # KEGG Glycan
        return "KEGG GLYCAN:" + synonym

    elif synonym.startswith('R') and is_correct_int(synonym[1:], 5):  # KEGG Reaction
        return "KEGG REACTION:" + synonym

    elif synonym.startswith('C') and is_correct_int(synonym[1:], 8):
        return "KNApSAcK:" + synonym
    else:
        return synonym


def get_valid_synonyms(cactus_synonym, pc_synonyms):
    result = ""
    for i in cactus_synonym.split(";"):
        m = re.search(r"\d", i)
        if i[0:m.start()] not in pc_synonyms:
            result = result + i + ";"
    if result != "":
        return result[:-1]
    else:
        return None


def get_relevant_synonym(synonym):
    if synonym.startswith('CAS-'):
        synonym = synonym.replace('CAS-', '').replace('-', '')
        return is_correct_int(synonym, 6)

    elif synonym.startswith('HMDB'):
        return is_correct_int(synonym.replace('HMDB', ''), 7)

    elif synonym.startswith('LM'):  # LipidMaps
        synonym = synonym[4:]
        """
        https://www.lipidmaps.org/data/classification/LM_classification_exp.php
        Fatty Acyls [FA]
        Glycerolipids [GL]
        Glycerophospholipids [GP]
        Sphingolipids [SP]
        Sterol Lipids [ST]
        Prenol Lipids [PR]
        Saccharolipids [SL]
        Polyketides [PK]
        """
        return is_correct_int(synonym, 8)

    elif synonym.startswith('YMDB'):
        return is_correct_int(synonym.replace('YMDB', ''), 5)

    elif synonym.startswith('ECMDB'):
        return is_correct_int(synonym.replace('ECMDB', ''), 5)

    elif synonym.startswith('ECMDB'):
        return is_correct_int(synonym.replace('ECMDB', ''), 5)

    elif synonym.startswith('C'):  # KEGG Compound
        return is_correct_int(synonym.replace('C', ''), 5)

    elif synonym.startswith('D'):  # KEGG Drug
        return is_correct_int(synonym.replace('D', ''), 5)

    elif synonym.startswith('G'):  # KEGG Glycan
        return is_correct_int(synonym.replace('G', ''), 5)

    elif synonym.startswith('R'):  # KEGG Reaction
        return is_correct_int(synonym.replace('R', ''), 5)

    elif synonym.startswith('HSDB '):  # HSDB/TOXNET
        return is_correct_int(synonym.replace('HSDB ', ''), 4)

    else:
        return False


def is_correct_int(num, length):
    try:
        if len(num) == length:
            int(num)
            return True
    except Exception:
        return False
    return False


def pubchem_search(comp_name, search_type='name', search_category='compound'):
    iupac = ''
    inchi = ''
    inchi_key = ''
    smiles = ''
    cid = ''
    formula = ''
    synonyms = ''
    where_found = ''
    first_synonym = ''
    try:
        compound = None
        # For this to work on Mac, run: cd "/Applications/Python 3.6/"; sudo "./Install Certificates.command
        # or comment out the line below:
        # ssl._create_default_https_context = ssl._create_unverified_context  # If no root certificates installed
        try:
            print_log("    -- Searching PubChem " + search_category + " for '" + comp_name + "' using " + search_type)
            pubchem_compound = get_compounds(comp_name, namespace=search_type)
            if isinstance(pubchem_compound, list):
                compound = pubchem_compound[0]  # Pick the top one
            where_found = 'pubchem_compound_on_name'
            if pubchem_compound and search_category == 'cid':
                where_found = 'pubchem_compound_on_final_cid'

            if not pubchem_compound and search_category == 'substance':
                print_log("    -- Querying PubChem Substance '" + comp_name + "'")
                _cid = get_pubchem_substance(comp_name, 'cid')
                if _cid:
                    pubchem_compound = get_compounds(_cid, namespace='cid')
                    where_found = 'pubchem_substance_on_cid'
                    compound = pubchem_compound[0]  # Only read the first record from PubChem = preferred entry
                    # if compound and compound.iupac_name:
                    #     print_log("    -- Found PubChem substance '" + compound.iupac_name + "'")

            if compound and compound.iupac_name:
                print_log("    -- Found PubChem " + search_category + " '" + compound.iupac_name + "'")
        except IndexError:
            print_log(
                "    -- Could not find PubChem " + search_category + " for '" + comp_name + "'")  # Nothing was found
        except Exception as e:
            print_log(str(e))

        if compound:
            inchi = compound.inchi.strip().rstrip('\n')
            inchi_key = compound.inchikey.strip().rstrip('\n')
            smiles = compound.canonical_smiles.strip().rstrip('\n')
            iupac = compound.iupac_name.strip().rstrip('\n')
            # ToDo, generate from structure. Venkat has Marvin license
            iupac = iupac.replace('f', '').replace('{', '').replace('}', '')
            iupac = iupac.strip().rstrip('\n')
            cid = compound.cid
            cid = str(cid).strip().rstrip('\n')
            formula = compound.molecular_formula.strip().rstrip('\n')
            for idx, synonym in enumerate(compound.synonyms):
                first_synonym = None
                if idx == 0 and synonym.lower() != comp_name.lower():
                    # The first synonym is also the PubChem name,
                    # so always add this to the return set unless it's the same as the compound name we searched for
                    first_synonym = synonym
                elif idx == 1 and first_synonym is None and synonym.lower() != comp_name.lower():
                    # Pick the 2nd synonym if the first one is the same as the compounds we searched for
                    first_synonym = synonym
                if get_relevant_synonym(synonym):
                    synonym = add_database_name_synonym(synonym)
                    synonyms = synonyms + ';' + synonym.strip().rstrip('\n')

                if first_synonym:
                    print_log("    -- Found PubChem first synonym '" + first_synonym)

            if synonyms:
                synonyms = synonyms.replace(";", "", 1)  # Remove the leading ";"

            print_log("    -- Searching PubChem for '" + comp_name + "', got cid '" + str(cid) +
                      "' and iupac name '" + iupac + "'")
    except Exception as error:
        print_log("    -- Unable to search PubChem for '" + search_category + "' '" + comp_name +
                  "'. Error: " + str(error), mode='error')

    return iupac, inchi, inchi_key, smiles, cid, formula, synonyms, where_found, first_synonym


def get_sdf(anno_sub_folder_path, cid, iupac, sdf_file_list, final_inchi, classyfire_search):
    classyfire_id = ""
    file_name = ""
    
    if anno_sub_folder_path and cid:
        if not iupac or len(iupac) < 1:
            iupac = 'no name given'

        if cid.endswith(".mol"):  # We have added a manually created/downloaded mol file
            mol_file_name = os.path.join(anno_sub_folder_path, file_name)

            if os.path.isfile(mol_file_name):
                print_log("    -- Found manually created MOL structure " + str(cid))
                file_name = mol_file_name
            else:
                print_log("    -- ERROR: Could not find manually created MOl structure " + mol_file_name, mode='error')
        else:
            print_log("    -- Checking if we have SDF for CID " + str(cid))
            file_name = cid + pubchem_sdf_extension
            full_file = os.path.join(anno_sub_folder_path, file_name)

            if os.path.isfile(full_file):
                print_log("    -- Already have PubChem SDF for CID " + str(cid) + " for name: " + iupac)
            else:
                if "MTBLS" not in cid:
                    print_log("    -- Getting SDF for CID " + str(cid) + " for name: " + iupac)
                    pcp.download('SDF', full_file, cid, overwrite=True)

    if classyfire_search and final_inchi:
        print_log("    -- Getting SDF from ClassyFire for " + str(final_inchi))
        classyfire_id = classyfire(final_inchi)
    else:
        print_log("    -- Final InChI missing, can not query ClassyFire")

    sdf_file_list.append([file_name, classyfire_id])

    return sdf_file_list, classyfire_id


def read_glytoucan_file():
    glytoucan_file_df = None
    try:
        glytoucan_file_df = read_tsv(os.path.join(resource_folder, 'glytoucan.tsv'))
        glytoucan_file_df = glytoucan_file_df.drop_duplicates(subset='cid', keep="last")
    except Exception as e:
        print_log("ERROR: Could not read " + resource_folder + "/glytoucan.tsv file. " + str(e), mode='error')
    return glytoucan_file_df


def get_glytoucan_id(final_cid, df):
    glytoucan_id = ""
    try:
        result = df.loc[df['cid'] == 'CID' + final_cid]
        # glytoucan_id = result['glytoucan']
        if len(result.index) >= 1:
            glytoucan_id = result.iloc[0]['glytoucan']
    except Exception as e:
        print_log("    -- WARNING: Could not search glytoucan dataframe. " + str(e))
    return glytoucan_id



def check_unichem_api(inchi_key='BSYNRYMUTXBXSQ-UHFFFAOYSA-N'):
    status = 'Failed'
    unichem_url = get_settings().chebi.pipeline.search_services.unichem_url
    unichem_url = f"{unichem_url}/inchikey/{inchi_key}"
    try:
        resp = requests.get(unichem_url, timeout=10)
        if resp.status_code == 200:
            status = 'Success'
    except Exception as e:
        print_log(' UNICHEM API failed ', mode='info')
    return status

def processDbNames(name=None):
    if name:
        if name.startswith('KEGG'):
            name = name.upper()
        elif name == 'LipidMaps':
            name = name.uppper()
        elif name.startswith('Human Metabolome'):
            name = 'HMDB'
    return name

def processUniChemResponseAll(inchi_key):
    print_log(' Querying Unichem URL with inchi key - ' + inchi_key)
    unichem_url = get_settings().chebi.pipeline.search_services.unichem_url
    unichem_url = f"{unichem_url}/verbose_inchikey/{inchi_key}"      
    try:
        resp = requests.get(unichem_url, timeout=10)
        unichem_id = ''
        if resp.status_code == 200:
            json_resp = resp.json()
            for item in json_resp:
                label = item["name_label"]
                dbname = processDbNames(name=label)
                src_comp_id = item["src_compound_id"][0]
                unichem_id = unichem_id+dbname+":"+src_comp_id+";"
            
            unichem_id = unichem_id.rstrip(';')
            return unichem_id
    except Exception as e:
        print_log(f' UNICHEM API failed; exception - {e}', mode='error')
        print_log(f' UNICHEM failed URL - {unichem_url}', mode='info')
    return ''
            
def processUniChemResponse(inchi_key):
    print_log(' Querying Unichem URL with inchi key - ' + inchi_key)
    unichem_url = get_settings().chebi.pipeline.search_services.unichem_url
    unichem_url = f"{unichem_url}/inchikey/{inchi_key}"
    try:
        resp = requests.get(unichem_url, timeout=10)
        unichem_id = ''
        if resp.status_code == 200:
            json_resp = resp.json()
            response_dict = {item['src_id']: item for item in json_resp if item and 'src_id' in item}
            if len(response_dict) == 0:
                print_log(' Chem plus search for inchi key - ' + inchi_key + ' contains no data.')
                return unichem_id
            if '2' in response_dict:
                unichem_id = 'DrugBank:' + response_dict['2']['src_compound_id'] + ";"
            if '3' in response_dict:
                unichem_id = unichem_id + 'PDBeChem:' + response_dict['3']['src_compound_id'] + ";"
            if '6' in response_dict:
                kegg_id = response_dict['6']['src_compound_id']
                kegg_id = add_database_name_synonym(kegg_id)
                unichem_id = unichem_id + kegg_id + ";"
            if '18' in response_dict:
                unichem_id = unichem_id + 'HMDB:HMDB' + response_dict['18']['src_compound_id'][4:] + ";"
            if '25' in response_dict:
                unichem_id = unichem_id + 'LINCS:' + response_dict['25']['src_compound_id'] + ";"
            if '34' in response_dict:
                unichem_id = unichem_id + 'DrugCentral:' + response_dict['34']['src_compound_id'] + ";"
            unichem_id = unichem_id.rstrip(';')
            return unichem_id
    except Exception as e:
        print_log(' UNICHEM API failed; exception ', mode='info')
        print_log(f' UNICHEM failed URL - {unichem_url}', mode='info')
    return ''

def get_cas_id(inchi_key):
    print_log(' calling Chem plus for inchi key - ' + inchi_key)
    chem_plus_url = get_settings().chebi.pipeline.search_services.chem_plus_url
    chem_plus_url = chem_plus_url.replace("INCHI_KEY", inchi_key)
    cas_id = ''
    # resp = requests.get(chem_plus_url)
    # if resp.status_code == 200:
    #     try:
    #         json_resp = resp.json()
    #         if 'rn' in json_resp['results'][0]['summary']:
    #             cas_id = json_resp['results'][0]['summary']['rn']
    #     except Exception as ex:
    #         logger.warning(f"Invalid result from {chem_plus_url} for input: {inchi_key}  {str(ex)}")

    return cas_id

def check_dime_db_api(inchi_key='BSYNRYMUTXBXSQ-UHFFFAOYSA-N'):
    print_log('Checking DimeDB API for inchi key...!')
    dime_url = get_settings().chebi.pipeline.search_services.dime_url
    dime_url = dime_url.replace("INCHI_KEY", inchi_key)
    dimedb_search_status = False
    try:
        resp = requests.get(dime_url, timeout=10)
        if resp.status_code == 200:
            dimedb_search_status = True
    except Exception as e:
        print_log(' DimeDB API is not reachable!')
    return dimedb_search_status


def get_dime_db(inchi_key):
    print_log(' Querying DIME DB for inchi key - ' + inchi_key)
    dime_url = get_settings().chebi.pipeline.search_services.dime_url
    dime_url = dime_url.replace("INCHI_KEY", inchi_key)
    dime_db_ids = ''
    try:
        resp = requests.get(dime_url, timeout=10)
        print_log('Response code ' + str(resp.status_code))
        if resp.status_code == 200:
            try:
                json_resp = resp.json()
                response_dict = json_resp['_items'][0]['External Sources']

                if 'BioCyc' in response_dict and response_dict['BioCyc']:
                    dime_db_ids = 'MetaCyc:' + response_dict['BioCyc'] + ";"
                if 'PubChem' in response_dict and response_dict['PubChem']:
                    dime_db_ids = dime_db_ids + 'PubChem:' + response_dict['PubChem'] + ";"
                if 'KEGG Compound' in response_dict and response_dict['KEGG Compound']:
                    dime_db_ids = dime_db_ids + 'KEGG COMPOUND:' + response_dict['KEGG Compound'] + ";"
                # Discussed with Chebi Team - No need to add this but add Wikipedia
                # if 'Wikidata' in response_dict and response_dict['Wikidata']:
                # dime_db_ids = dime_db_ids + 'Wikidata:' + response_dict['Wikidata'] + ";"
                if 'Wikipedia' in response_dict and response_dict['Wikipedia']:
                    dime_db_ids = dime_db_ids + 'Wikipedia:' + response_dict['Wikipedia'] + ";"
                if 'Chemspider' in response_dict and response_dict['Chemspider']:
                    dime_db_ids = dime_db_ids + 'ChemSpider:' + response_dict['Chemspider'] + ";"
                if 'ChEBI' in response_dict and response_dict['ChEBI']:
                    dime_db_ids = dime_db_ids + 'ChEBI:' + response_dict['ChEBI'] + ";"
                if 'HMDB Accession' in response_dict and response_dict['HMDB Accession']:
                    hmdb = response_dict['HMDB Accession']
                    if len(hmdb) != 11:
                        hmdb = '00' + hmdb[4:]
                    dime_db_ids = dime_db_ids + 'HMDB:HMDB' + hmdb + ";"

                dime_db_ids = dime_db_ids.rstrip(';')
            except Exception as ex:
                logger.warning(f"Invalid result from {dime_url} for input: {inchi_key}  {str(ex)}")
    except Exception as e:
        print_log(' DimeDB API is Error !')
                
    return dime_db_ids


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1]:         
        study_id = sys.argv[1]

    user_token = None
    if len(sys.argv) > 2 and sys.argv[2]:
        user_token = sys.argv[2]

    annotation_file_name = None
    if len(sys.argv) > 3 and sys.argv[3]:
        annotation_file_name = sys.argv[3]

    run_silently = False
    if len(sys.argv) > 4 and sys.argv[4]:
        run_silently = True if sys.argv[4].lower() == "true" else False

    classyfire_search = False
    if len(sys.argv) > 5 and sys.argv[5]:
        classyfire_search = True if sys.argv[5].lower() == "true" else False

    update_study_maf = False
    if len(sys.argv) > 6 and sys.argv[6]:
        classyfire_supdate_study_mafearch = True if sys.argv[5].lower() == "true" else False
        
    run_chebi_pipeline(study_id, user_token, annotation_file_name, run_silently=run_silently, classyfire_search=classyfire_search, update_study_maf=update_study_maf)