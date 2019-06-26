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

import logging, pandas as pd, os
import numpy as np
import requests
import cirpy
import time
import pubchempy as pcp
import ctfile
import ssl
import pronto
from subprocess import *
from flask import current_app as app
from zeep import Client
from pathlib import Path
from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from pubchempy import get_compounds
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import read_tsv, write_tsv, get_assay_file_list
from app.ws.isaApiClient import IsaApiClient
from app.ws.study_files import get_all_files_from_filesystem

logger = logging.getLogger('wslog')

# MetaboLights (Java-Based) WebService client
wsc = WsClient()
iac = IsaApiClient()

pubchem_end = "_pubchem.tsv"
complete_end = "_complete.sdf"
classyfire_end = "_classyfire"
anno_sub_folder = "chebi_pipeline_annotations"
final_cid_column_name = "final_external_id"
unknown_list = "unknown", "un-known", "n/a", "un_known", "not known", "not-known", "not_known", "unidentified", \
               "not identified", "unmatched"
resource_folder = "./resources/"
search_flag = 'search_flag'

maf_compound_name_column = "metabolite_identification"
alt_name_column = 'alt_name'
database_identifier_column = "database_identifier"


def split_rows(maf_df, annotation_file=None):
    # Split rows with pipe-lines "|"
    pipe_found = False
    new_maf = maf_df
    with open(annotation_file, 'r') as f:
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
            print_log("MAF splitter failed!")
            logger.error(str(e))
            return maf_df

    return new_maf


def split_maf_df(maf_file_name):
    try:
        args = [resource_folder + 'maf-splitter.jar', maf_file_name]  # Any number of args to be passed to the jar file
        stdout, stderr = jarWrapper(*args)
    except Exception as e:
        print_log("    -- Could not split MAF file. Error: " + str(e) + stderr)

    split_file = maf_file_name + "_SPLIT"
    try:
        new_df = read_tsv(split_file)
        os.remove(split_file)
    except Exception as e:
        print_log("Could not read and/or remove 'split' file " + split_file + ". " + str(e))

    return new_df


def jarWrapper(*args):
    process = Popen(['java', '-jar']+list(args), stdout=PIPE, stderr=PIPE)
    process.wait()
    stdout, stderr = process.communicate()
    return stdout, stderr


def print_log(message):
    print(str(message))
    logger.info(str(message))


def check_maf_for_pipes(study_location, annotation_file_name):
    annotation_file = os.path.join(study_location, annotation_file_name)
    try:
        maf_df = read_tsv(annotation_file)
    except FileNotFoundError:
        abort(400, "The file " + annotation_file + " was not found")
    maf_len = len(maf_df.index)

    # Any rows to split?
    new_maf_df = split_rows(maf_df, annotation_file)
    new_maf_len = len(new_maf_df.index)

    file_name = os.path.join(anno_sub_folder, annotation_file + '.split')
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

    return True


def clean_comp_name(comp_name):
    remove_chars = ['/', ' ', '(', ')', ')', ',', ':', ';', '\\', '-', '\'', '"']

    for c in remove_chars:
        comp_name = comp_name.replace(c, '')
    return comp_name


def search_and_update_maf(study_location, annotation_file_name, classyfire_search):
    sdf_file_list = []
    exiting_pubchem_file = False
    short_file_name = os.path.join(study_location + os.sep + anno_sub_folder + os.sep,
                                   annotation_file_name.replace('.tsv', ''))
    if annotation_file_name.endswith(pubchem_end):
        exiting_pubchem_file = True
        short_file_name = os.path.join(study_location + os.sep + anno_sub_folder + os.sep,
                                       annotation_file_name.replace(pubchem_end, ''))

    annotation_file_name = os.path.join(study_location, annotation_file_name)
    pd.options.mode.chained_assignment = None  # default='warn'

    standard_maf_columns = {database_identifier_column: 0, "chemical_formula": 1, "smiles": 2, "inchi": 3}

    try:
        maf_df = read_tsv(annotation_file_name)
    except FileNotFoundError:
        abort(400, "The file " + annotation_file_name + " was not found")
    maf_len = len(maf_df.index)

    create_annotation_folder(study_location + os.sep + anno_sub_folder)

    # First make sure the existing pubchem annotated spreadsheet is loaded
    pubchem_df = maf_df.copy()

    if exiting_pubchem_file:  # The has already been split and this is an existing "pubchem" file
        new_maf_df = maf_df.copy()
        new_maf_len = len(new_maf_df.index)
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

    row_idx = 0
    if exiting_pubchem_file:
        short_df = maf_df[[database_identifier_column, maf_compound_name_column, alt_name_column, search_flag, final_cid_column_name]]
    else:
        short_df = maf_df[[database_identifier_column, maf_compound_name_column, alt_name_column]]

    # Search using the compound name column
    for idx, row in short_df.iterrows():
        database_id = row[0]
        comp_name = row[1]
        original_comp_name = comp_name
        alt_name = row[2]
        search = True
        chebi_id = None

        alt_name = str(alt_name)
        if len(alt_name) > 0:
            comp_name = alt_name
            print_log("    -- Using alt_name '" + alt_name + "'")

        final_cid = None
        if exiting_pubchem_file:
            if str(row[3]).rstrip('.0') == '1':  # This is the already searched flag in the spreadsheet
                search = False
            final_cid = row[4]
        print_log(str(idx + 1) + ' of ' + str(new_maf_len) + ' : ' + comp_name)

        pubchem_df.iloc[row_idx, 5] = row_idx + 1  # Row id

        if search and comp_name and check_if_unknown(comp_name):
            # So if have a name, but no ChEBI id, try to search for it  # ToDo, use final_cid

            start_time = time.time()
            chebi_found = False
            comp_name = comp_name.strip()  # Remove leading/trailing spaces before we search
            comp_name = comp_name.replace('Î´', 'delta')

            search_res = wsc.get_maf_search("name", comp_name)  # This is the standard MetaboLights aka Plugin search
            if not search_res:
                search_res = wsc.get_maf_search("name", clean_comp_name(comp_name))

            if search_res and search_res['content']:
                result = search_res['content'][0]
                database_identifier = result["databaseId"]
                chemical_formula = result["formula"]
                smiles = result["smiles"]
                inchi = result["inchi"]
                name = result["name"]

                pubchem_df.iloc[row_idx, 0] = database_identifier
                pubchem_df.iloc[row_idx, 1] = chemical_formula
                pubchem_df.iloc[row_idx, 2] = smiles
                pubchem_df.iloc[row_idx, 3] = inchi
                pubchem_df.iloc[row_idx, 4] = original_comp_name  # 4 is name / metabolite_identification from MAF
                pubchem_df.iloc[row_idx, 5] = ''  # Row id
                pubchem_df.iloc[row_idx, 6] = ''  # Search flag
                pubchem_df.iloc[row_idx, 7] = alt_name  # alt_name, for us to override the submitted name

                if name:
                    if database_identifier:
                        if database_identifier.startswith('CHEBI:'):
                            chebi_found = True
                            print_log("    -- Found ChEBI id " + database_identifier + " in the MetaboLights search")
                        maf_df.iloc[row_idx, int(standard_maf_columns[database_identifier_column])] = database_identifier
                    if chemical_formula:
                        maf_df.iloc[row_idx, int(standard_maf_columns['chemical_formula'])] = chemical_formula
                    if smiles:
                        maf_df.iloc[row_idx, int(standard_maf_columns['smiles'])] = smiles
                    if inchi:
                        maf_df.iloc[row_idx, int(standard_maf_columns['inchi'])] = inchi

            if not chebi_found:  # We could not find this in ChEBI, let's try other sources
                pc_name, pc_inchi, pc_inchi_key, pc_smiles, pc_cid, pc_formula, pc_synonyms, pc_structure = \
                    pubchem_search(comp_name, 'name')

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

                pubchem_df.iloc[row_idx, 8] = pc_name  # PubChem name

                if not final_cid:
                    final_cid = pc_cid
                pubchem_df.iloc[row_idx, 9] = final_cid   # Final PubChem CID (and other external id's manually added)
                pubchem_df.iloc[row_idx, 10] = pc_cid   # PubChem CID
                if not final_cid:
                    final_cid = get_pubchem_cid_on_inchikey(cactus_stdinchikey, opsin_stdinchikey)
                    pubchem_df.iloc[row_idx, 9] = final_cid  # Final PubChem CID should now be the cactus or opsin
                pubchem_df.iloc[row_idx, 11] = pc_cid    # PubChem CID

                pubchem_df.iloc[row_idx, 12] = csid      # ChemSpider ID (CSID) from INCHI
                pubchem_df.iloc[row_idx, 13] = get_ranked_values(pc_smiles, cactus_smiles, opsin_smiles, None)  # final smiles
                final_inchi = get_ranked_values(pc_inchi, cactus_inchi, opsin_inchi, None)  # final inchi
                pubchem_df.iloc[row_idx, 14] = final_inchi
                final_inchi_key = get_ranked_values(pc_inchi_key, cactus_stdinchikey, opsin_stdinchikey, None)  # final inchikey
                pubchem_df.iloc[row_idx, 15] = final_inchi_key
                pubchem_df.iloc[row_idx, 16] = pc_smiles            # pc_smiles
                pubchem_df.iloc[row_idx, 17] = cactus_smiles        # cactus_smiles
                pubchem_df.iloc[row_idx, 18] = opsin_smiles         # opsin_smiles
                pubchem_df.iloc[row_idx, 19] = pc_inchi             # PubChem inchi
                pubchem_df.iloc[row_idx, 20] = cactus_inchi         # Cactus inchi
                pubchem_df.iloc[row_idx, 21] = opsin_inchi          # Opsin inchi
                pubchem_df.iloc[row_idx, 22] = pc_inchi_key         # PubChem stdinchikey
                pubchem_df.iloc[row_idx, 23] = cactus_stdinchikey   # cactus_stdinchikey
                pubchem_df.iloc[row_idx, 24] = opsin_stdinchikey    # opsin_stdinchikey
                pubchem_df.iloc[row_idx, 25] = pc_formula           # PubChem formula
                pubchem_df.iloc[row_idx, 26] = pc_synonyms          # PubChem synonyms
                pubchem_df.iloc[row_idx, 27] = cactus_synonyms      # Cactus synonyms
                # pubchem_df.iloc[row_idx, 28] = organism             # Organism from study sample sheet
                # pubchem_df.iloc[row_idx, 29] = organism_part        # Organism part from study sample sheet
                # pubchem_df.iloc[row_idx, 30] = direct_parent  #
                # pubchem_df.iloc[row_idx, 31] = alternate_parent  #
                # pubchem_df.iloc[row_idx, 32] = mtbls_acc  #
                # pubchem_df.iloc[row_idx, 33] = classyfire_search_id  #

                # Now we may have more information, so let's try to search ChEBI again

                if final_inchi_key and len(final_inchi_key) > 0:
                    chebi_id, inchi, inchikey, name, smiles, formula = direct_chebi_search(final_inchi_key, comp_name)
                elif comp_name:
                    chebi_id, inchi, inchikey, name, smiles, formula = direct_chebi_search(
                        final_inchi_key, comp_name, "external_db")
                else:
                    chebi_id, inchi, inchikey, name, smiles, formula = direct_chebi_search(
                        final_inchi_key, comp_name, "external_db")  # ToDo, not really required

                if chebi_id:
                    database_identifier = chebi_id
                    chemical_formula = formula
                    smiles = smiles
                    inchi = inchi
                    name = name

                    print_log('    -- Found ChEBI id ' + database_identifier + ' based on final InChIKey')
                    pubchem_df.iloc[row_idx, 0] = database_identifier
                    pubchem_df.iloc[row_idx, 1] = chemical_formula
                    pubchem_df.iloc[row_idx, 2] = smiles
                    pubchem_df.iloc[row_idx, 3] = inchi
                    # 4 is name / metabolite_identification from MAF

                    if name:  # Add to the annotated file as well
                        if database_identifier:
                            maf_df.iloc[row_idx, int(standard_maf_columns[database_identifier_column])] = database_identifier
                        if chemical_formula:
                            maf_df.iloc[row_idx, int(standard_maf_columns['chemical_formula'])] = chemical_formula
                        if smiles:
                            maf_df.iloc[row_idx, int(standard_maf_columns['smiles'])] = smiles
                        if inchi:
                            maf_df.iloc[row_idx, int(standard_maf_columns['inchi'])] = inchi

                else:
                    # Now, if we still don't have a ChEBI accession, download the structure (SDF) from PubChem
                    # and the classyFire SDF
                    sdf_file_list, classyfire_id = get_sdf(study_location, str(final_cid).rstrip('.0'), pc_name,
                                                           sdf_file_list, final_inchi, classyfire_search)
                    #  pubchem_df.iloc[row_idx, 32] = classyfire_id

            pubchem_df.iloc[row_idx, 5] = row_idx + 1  # Row id
            pubchem_df.iloc[row_idx, 6] = '1'  # Search flag
            print_log("    -- Search took %s seconds" % round(time.time() - start_time, 2))
        else:
            pubchem_df.iloc[row_idx, 6] = '1'  # Search flag set so we don't search for unknown again
            print_log("    -- Skipping. Already found or no database id/compound name to search for: "
                      + database_id + " " + comp_name)

        if row_idx > 0 and row_idx % 10 == 0:  # Save every 10 rows
            pubchem_file = short_file_name + pubchem_end
            write_tsv(pubchem_df, pubchem_file)

        row_idx += 1

    write_tsv(maf_df, short_file_name + "_annotated.tsv")
    pubchem_file = short_file_name + pubchem_end
    write_tsv(pubchem_df, pubchem_file)

    # if sdf_file_list:
    concatenate_sdf_files(pubchem_df, study_location + os.sep + anno_sub_folder + os.sep,
                          short_file_name + complete_end, short_file_name + classyfire_end, classyfire_search)

    return maf_df, maf_len, new_maf_df, new_maf_len, pubchem_file


def create_annotation_folder(folder_loc):
    print_log("Checking for ChEBI folder " + folder_loc)
    try:
        if not os.path.exists(folder_loc):
            print_log("Creating ChEBI folder " + folder_loc)
            os.makedirs(folder_loc)
    except Exception as e:
        print_log(str(e))


def concatenate_sdf_files(pubchem_df, study_location, sdf_file_name, classyfire_file_name, classyfire_search):
    return_format = 'sdf'  # 'json' will require a new root element to separate the entries before merging
    classyfire_file_name = classyfire_file_name + '.' + return_format

    # ToDo, only write files if we have downloaded SDF files
    # Create a new concatenated SDF file
    with open(sdf_file_name, 'w') as outfile:
        # SDF file list = [file name, classyFire process id]
        # short_df = pubchem_df[["pubchem_cid", "classyfire_search_id"]]
        short_df = pubchem_df[["pubchem_cid", database_identifier_column]]
        for idx, row in short_df.iterrows():
            cf_id = None
            cid = row["pubchem_cid"]
            cid = str(cid).rstrip('.0')
            db_id = row[database_identifier_column]
            if len(db_id) == 0:
                db_id = None

            if cid and not db_id:
                fname = cid + '.sdf'
                full_file = os.path.join(study_location, fname)
                if os.path.isfile(full_file):
                    try:
                        with open(full_file) as infile:
                            for line in infile:
                                outfile.write(line)
                    except Exception as e:
                        print_log("       -- Warning, can not read SDF file (" + full_file + ")")
                else:
                    print_log("    -- Will try to downbload SDF file for CID " + cid)
                    pcp.download('SDF', full_file, cid, overwrite=True)
                    # ToDo, try to pull down the sdf from PubChem

            # Now, get the classyFire queries, add to classyfire_file_name and get ancestors
            get_classyfire_results(cf_id, classyfire_file_name, return_format, classyfire_search)

        outfile.close()

        all_ancestors = None
        if classyfire_search:
            all_ancestors = get_ancestors(classyfire_file_name)

        if all_ancestors:
            print_log("    -- Adding ancestors to SDF file")

        # If we have a real new SDF file, remove the smaller sdf files
        # remove_sdf_files(sdf_file_name, study_location, sdf_file_list)


def remove_sdf_files(sdf_file_name, study_location, sdf_file_list):
    sdf_file = Path(sdf_file_name)
    if sdf_file.is_file():
        for fname in sdf_file_list:
            try:
                sdf_path = Path(os.path.join(study_location, fname[0]))
                sdf_path.unlink()
            except Exception as e:
                logger.error('Could not remove file' + sdf_path)
                logger.exception(str(e))


def classyfire(inchi):
    print_log("    -- Querying ClassyFire")
    url = app.config.get('CLASSYFIRE_ULR')
    label = 'MetaboLights WS'
    query_id = None
    try:
        r = requests.post(url + '/queries.json',
                          data='{"label": "%s", "query_input": "%s", "query_type": "STRUCTURE"}' % (label, inchi),
                          headers={"Content-Type": "application/json"})
        r.raise_for_status()
        query_id = r.json()['id']
    except Exception as e:
        logger.error(str(e))
        print_log("    -- Error querying ClassyFire: " + str(e))
    return query_id


def get_classyfire_results(query_id, classyfire_file_name, return_format, classyfire_search):

    if classyfire_search and query_id:
        try:
            url = app.config.get('CLASSYFIRE_ULR')
            r = requests.get('%s/queries/%s.%s' % (url, query_id, return_format),
                             headers={"Content-Type": "application/%s" % return_format})
            time.sleep(2)  # Give ClassyFire time to recover first ;-)
            r.raise_for_status()

            if len(r.text) > 1:
                with open(classyfire_file_name, "a") as cf_file:
                    cf_file.write(r.text)
        except Exception as e:
            logger.error("Could not get classyfire result for " + query_id)
            logger.error(str(e))


def load_chebi_classyfire_mapping():
    mapping_file = app.config.get('CLASSYFIRE_MAPPING')
    print_log('loading ChEBI mapping file ' + mapping_file)
    return read_tsv(mapping_file)


def get_ancestors(classyfire_file_name):
    # get mappings from ClassyFire names (in classyfire_file_name) to ChEBI
    # with open(classyfire_file_name, 'r') as infile:
    #     mols = ctfile.load(infile)

    lines = []
    inchi_key = ""
    parent_name = ""
    direct_parents = []
    is_a_list = []
    all_ancestors = []
    classyfire_df = get_classyfire_lookup_mapping()
    with open(classyfire_file_name, 'r') as infile:
        for line in infile:
            lines.append(line.rstrip('\n'))

    for idx, line in enumerate(lines):

        if line == "> <InChIKey>":
            inchi_key = lines[idx+1]
        elif line == "> <Direct Parent>":
            parent_name = lines[idx+1]
            if parent_name not in direct_parents:
                direct_parents.append(parent_name)
        elif line == "> <Alternative Parents>":
            row = classyfire_df.loc[classyfire_df['name'] == parent_name]
            is_a = row['map_to']  # Get the IS_A relationships
            all_ancestors.append({"inchi_key": inchi_key.replace('InChiKey=', ''),
                                  "direct_parent": parent_name,
                                  "is_a": is_a})

    # onto = get_chebi_obo_file()
    # for compound_name in direct_parents:
    #     chebi_compound = get_chebi_mapping(compound_name)
    #     is_a_list = get_is_a(onto, chebi_compound)

    return all_ancestors


def get_classyfire_lookup_mapping():
    assay_master_template = './resources/ClassyFire_Mapping_VLOOKUP.tsv'
    return read_tsv(assay_master_template)


def get_chebi_obo_file():
    print_log('loading ChEBI OBO file')
    obo_file = app.config.get('OBO_FILE')
    onto = pronto.Ontology(obo_file)
    return onto


def get_chebi_mapping(compound_name):
    print_log('Reading ClassyFire to ChEBI mapping file')
    mapping_file = load_chebi_classyfire_mapping()

    return mapping_file


def get_is_a(onto, chebi_compound):
    print_log('Get ChEBI parents')
    is_a_list = onto[chebi_compound].rparents()  # "Parents" are "is_a" relationships
    return is_a_list


def direct_chebi_search(final_inchi, comp_name, search_type="inchi"):
    chebi_id = ""
    inchi = ""
    inchikey = ""
    name = ""
    smiles = ""
    formula = ""
    url = app.config.get('CHEBI_URL')
    top_result = None

    try:
        client = Client(url)
    except Exception as e:
        print_log("    -- Could not set up ChEBI webservice calls. " + str(e))
        return chebi_id, inchi, inchikey, name, smiles, formula

    try:
        if search_type == "inchi" and final_inchi:
            print_log("    -- Querying ChEBI web services for " + comp_name + " based on final InChIKey " + final_inchi)
            lite_entity = client.service.getLiteEntity(final_inchi, 'INCHI_INCHI_KEY', '10', 'ALL')
        elif search_type == "external_db" and comp_name:
            print_log("    -- Querying ChEBI web services for " + comp_name + " using external database id search")
            lite_entity = client.service.getLiteEntity(comp_name, 'DATABASE_LINK_REGISTRY_NUMBER_CITATION', '10', 'ALL')

        if lite_entity and lite_entity[0]:
            top_result = lite_entity[0]
        else:
            if final_inchi and len(final_inchi) > 0:
                comp_name = clean_comp_name(comp_name)
                print_log("    -- Querying ChEBI web services for " + comp_name)
                lite_entity = client.service.getLiteEntity(final_inchi, 'CHEBI_NAME', '10', 'ALL')
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
            if complete_entity.Formulae and complete_entity.Formulae[0]:
                formula = complete_entity.Formulae[0].data

    except Exception as e:
        logger.error("ChEBI Search error: " + str(e))
        print_log('    -- Error querying ChEBI')
    return chebi_id, inchi, inchikey, name, smiles, formula


def get_csid(inchikey):
    csid = ""
    csurl_base = app.config.get('CHEMSPIDER_URL')

    if inchikey:
        url1 = csurl_base + 'SimpleSearch&searchOptions.QueryText=' + inchikey
        resp1 = requests.get(url1)
        if resp1.status_code == 200:
            url2 = csurl_base + 'GetSearchResult&rid=' + resp1.text
            resp2 = requests.get(url2)
            if resp2.status_code == 200:
                csid = resp2.text
                csid = csid.replace('[', '').replace(']', '').split(',')[0]
                print_log("    -- Found CSID using ChemSpider: " + inchikey)
                return csid
    return csid


def get_pubchem_cid_on_inchikey(inchikey1, inchikey2):
    pc_cid = ''
    for inchikey in [inchikey1, inchikey2]:
        if inchikey:
            pc_name, pc_inchi, pc_inchi_key, pc_smiles, pc_cid, pc_formula, pc_synonyms, pc_structure = \
                pubchem_search(inchikey, search_type='inchikey')
            if pc_cid:
                return pc_cid
    return pc_cid


def get_ranked_values(pubchem, cactus, opsin, chemspider):
    if pubchem:
        return pubchem
    elif cactus:
        return cactus
    elif opsin:
        return opsin
    elif chemspider:
        return chemspider
    else:
        return ""


def create_pubchem_df(maf_df):
    # These are simply the fixed spreadsheet column headers
    pubchem_df = maf_df[[database_identifier_column, 'chemical_formula', 'smiles', 'inchi', maf_compound_name_column]]
    pubchem_df['row_id'] = ''                   # 5
    pubchem_df[search_flag] = ''                # 6
    pubchem_df[alt_name_column] = ''            # 7
    pubchem_df['iupac_name'] = ''               # 8
    pubchem_df[final_cid_column_name] = ''      # 9
    pubchem_df['pubchem_cid'] = ''              # 10
    pubchem_df['pubchem_cid_ik'] = ''           # 11 PubChem CID from InChIKey search (Cactus, OBSIN)
    pubchem_df['csid_ik'] = ''                  # 12 ChemSpider ID (CSID) from INCHIKEY
    pubchem_df['final_smiles'] = ''             # 13
    pubchem_df['final_inchi'] = ''              # 14
    pubchem_df['final_inchi_key'] = ''          # 15
    pubchem_df['pubchem_smiles'] = ''           # 16
    pubchem_df['cactus_smiles'] = ''            # 17
    pubchem_df['opsin_smiles'] = ''             # 18
    pubchem_df['pubchem_inchi'] = ''            # 19
    pubchem_df['cactus_inchi'] = ''             # 20
    pubchem_df['opsin_inchi'] = ''              # 21
    pubchem_df['pubchem_inchi_key'] = ''        # 22
    pubchem_df['cactus_inchi_key'] = ''         # 23
    pubchem_df['opsin_inchi_key'] = ''          # 24
    pubchem_df['pubchem_formula'] = ''          # 25
    pubchem_df['pubchem_synonyms'] = ''         # 26
    pubchem_df['cactus_synonyms'] = ''          # 27
    pubchem_df['organism'] = ''                 # 28
    pubchem_df['organism_part'] = ''            # 29
    pubchem_df['direct_parent'] = ''            # 30
    pubchem_df['alternate_parent'] = ''         # 31
    pubchem_df['mtbls_acc'] = ''                # 32
    pubchem_df['classyfire_search_id'] = ''     # 33

    return pubchem_df


def opsin_search(comp_name, req_type):
    result = ""
    opsin_url = app.config.get('OPSIN_URL')
    url = opsin_url + comp_name + '.json'
    resp = requests.get(url)
    if resp.status_code == 200:
        json_resp = resp.json()
        result = json_resp[req_type]
    return result


def cactus_search(comp_name, search_type):
    result = None

    if comp_name is None:
        return None

    try:
        result = cirpy.resolve(comp_name, search_type)
        synonyms = ""
    except:
        print_log("    -- ERROR: Cactus search failed!")
        return result

    if result:
        if search_type == 'stdinchikey':
            return result.replace('InChIKey=', '')
        if search_type == 'names':
            for synonym in result:
                if get_relevant_synonym(synonym.strip()):
                    synonyms = synonyms + ';' + synonym
            return synonyms.replace(";", "", 1)  # Remove the leading ";"

    return result


def get_relevant_synonym(synonym):

    if synonym.startswith('CAS-'):
        synonym = synonym.replace('CAS-', '').replace('-', '')
        return is_correct_int(synonym, 6)

    elif synonym.startswith('HMDB'):
        return is_correct_int(synonym.replace('HMDB', ''), 7)

    elif synonym.startswith('LM'):  # LipidMaps
        synonym = synonym[4:]
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
    except:
        return False
    return False


def pubchem_search(comp_name, search_type='name'):
    iupac = ''
    inchi = ''
    inchi_key = ''
    smiles = ''
    cid = ''
    formula = ''
    synonyms = ''
    structure = ''
    print_log("    -- Searching PubChem for compound '" + comp_name + "'")
    try:
        compound = None
        # For this to work on Mac, run: cd "/Applications/Python 3.6/"; sudo "./Install Certificates.command
        # or comment out the line below:
        ssl._create_default_https_context = ssl._create_unverified_context  # If no root certificates installed
        try:
            pubchem_compound = get_compounds(comp_name, namespace=search_type)
            compound = pubchem_compound[0]  # Only read the first record from PubChem = preferred entry
            print_log("    -- Found PubChem compound '" + compound.iupac_name + "'")
        except IndexError:
            print_log('    -- Could not find PubChem compound for ' + comp_name)   # Nothing was found

        if compound:
            inchi = compound.inchi.strip().rstrip('\n')
            inchi_key = compound.inchikey.strip().rstrip('\n')
            smiles = compound.canonical_smiles.strip().rstrip('\n')
            iupac = compound.iupac_name.strip().rstrip('\n')  # ToDo, more than one newline encoding??
            # ToDo, generate from structure. Venkat has Marvin license
            iupac = iupac.replace('f', '').replace('{', '').replace('}', '')
            iupac = iupac.strip().rstrip('\n')
            cid = compound.cid
            cid = str(cid).strip().rstrip('\n')
            formula = compound.molecular_formula.strip().rstrip('\n')
            for synonym in compound.synonyms:
                if get_relevant_synonym(synonym):
                    synonyms = synonyms + ';' + synonym.strip().rstrip('\n')

            if synonyms:
                synonyms = synonyms.replace(";", "", 1)  # Remove the leading ";"

            print_log("    -- Searching PubChem for '" + comp_name + "', got cid '" + str(cid) +
                      "' and iupac name '" + iupac + "'")
    except Exception as error:
        logger.error("Unable to search PubChem for compound " + comp_name)
        logger.error(error)

    return iupac, inchi, inchi_key, smiles, cid, formula, synonyms, structure


def get_sdf(study_location, cid, iupac, sdf_file_list, final_inchi, classyfire_search):
    classyfire_id = None
    if study_location and cid:
        if not iupac or len(iupac) < 1:
            iupac = 'no name given'
        classyfire_id = ''
        print_log("    -- Getting SDF for CID " + str(cid) + " for name: " + iupac)
        file_name = cid + '.sdf'
        pcp.download('SDF', study_location + os.sep + anno_sub_folder + os.sep + file_name, cid, overwrite=True)

        if classyfire_search and final_inchi:
            classyfire_id = classyfire(final_inchi)

        sdf_file_list.append([file_name, classyfire_id])

    return sdf_file_list, classyfire_id


class SplitMaf(Resource):
    @swagger.operation(
        summary="MAF pipeline splitter (curator only)",
        nickname="Add rows based on pipeline splitting",
        notes="Split a given Metabolite Annotation File based on pipelines in cells. "
              "A new MAF will be created with extension '.split'. "
              "If no annotation_file_name is given, all MAF in the study is processed",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "annotation_file_name",
                "description": "Metabolite Annotation File name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The Metabolite Annotation File (MAF) is returned"
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def post(self, study_id):
        http_base_location = app.config.get('WS_APP_BASE_LINK')
        http_file_location = http_base_location + os.sep + study_id + os.sep + 'files'

        # param validation
        if study_id is None:
            abort(404, 'Please provide valid parameter for study identifier')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not is_curator:
            abort(403)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('annotation_file_name', help="Metabolite Annotation File", location="args")
        args = parser.parse_args()
        annotation_file_name = args['annotation_file_name'].strip()

        if annotation_file_name is None:
            # Loop through all m_*_v2_maf.tsv files
            study_files, upload_files, upload_diff, upload_location = \
                get_all_files_from_filesystem(
                    study_id, obfuscation_code, study_location, directory=None, include_raw_data=False,
                    assay_file_list=get_assay_file_list(study_location))
            maf_count = 0
            maf_changed = 0
            for file in study_files:
                file_name = file['file']
                if file_name.startswith('m_') and file_name.endswith('_v2_maf.tsv'):
                    maf_count += 1
                    maf_df, maf_len, new_maf_df, new_maf_len, split_file_name = \
                        check_maf_for_pipes(study_location, file_name)
                    if maf_len != new_maf_len:
                        maf_changed += 1
        else:
            maf_df, maf_len, new_maf_df, new_maf_len, split_file_name = \
                check_maf_for_pipes(study_location, annotation_file_name)

            return {"maf_rows": maf_len, "new_maf_rows": new_maf_len,
                    "file_name": http_file_location + split_file_name.split(study_id)[1]}

        return {"success": str(maf_count) + " MAF files checked for pipelines, " +
                           str(maf_changed) + " files needed updating."}


class ChEBIPipeLine(Resource):
    @swagger.operation(
        summary="Search external resources using compound names in MAF (curator only)",
        nickname="Search compound names",
        notes="Search and populate a given Metabolite Annotation File based on the 'metabolite_identification' column. "
              "New MAF files will be created with extensions '_annotated.tsv' and '_pubchem.tsv'. These form part of "
              "the ChEBI submission pipeline. If no annotation_file_name is given, all MAF in the study are processed",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "annotation_file_name",
                "description": "Metabolite Annotation File name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "classyfire_search",
                "description": "Search ClassyFire?.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The Metabolite Annotation File (MAF) is returned"
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def post(self, study_id):

        http_base_location = app.config.get('WS_APP_BASE_LINK')
        http_file_location = http_base_location + os.sep + study_id + os.sep + 'files'
        # param validation
        if study_id is None:
            abort(404, 'Please provide valid parameter for study identifier')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # Search ClassyFire?
        classyfire_search = False
        if "classyfire_search" in request.headers and \
                request.headers["classyfire_search"].lower() == 'true':
            classyfire_search = True

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not is_curator:
            abort(403)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('annotation_file_name', help="Metabolite Annotation File", location="args")
        args = parser.parse_args()
        annotation_file_name = args['annotation_file_name'].strip()

        if annotation_file_name is None:
            # Loop through all m_*_v2_maf.tsv files
            study_files, upload_files, upload_diff, upload_location = \
                get_all_files_from_filesystem(
                    study_id, obfuscation_code, study_location, directory=None, include_raw_data=False)
            maf_count = 0
            maf_changed = 0
            for file in study_files:
                file_name = file['file']
                if file_name.startswith('m_') and file_name.endswith('_v2_maf.tsv'):
                    maf_count += 1
                    maf_df, maf_len, new_maf_df, new_maf_len, pubchem_file = \
                        search_and_update_maf(study_location, file_name, classyfire_search)
                    if maf_len != new_maf_len:
                        maf_changed += 1
        else:
            maf_df, maf_len, new_maf_df, new_maf_len, pubchem_file = \
                search_and_update_maf(study_location, annotation_file_name, classyfire_search)
            return {"in_maf_rows": maf_len, "out_maf_rows": new_maf_len,
                    "pubchem_file": http_file_location + pubchem_file.split(study_id)[1]}

        return {"success": str(maf_count) + " MAF files checked for pipelines, " +
                           str(maf_changed) + " files needed updating."}


class CheckCompounds(Resource):
    @swagger.operation(
        summary="Search external resources using compound names",
        nickname="Search compound names",
        notes="Search various resources based on compound names",
        parameters=[
            {
                "name": "compound_names",
                "description": 'Compound names, one per line',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The Metabolite Annotation File (MAF) is returned"
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self):

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions('MTBLS1', user_token)
        if not read_access:
            abort(403)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('compound_names', help='compound_names')
        args = parser.parse_args()
        compound_names = args['compound_names']

        return {"success": compound_names}
