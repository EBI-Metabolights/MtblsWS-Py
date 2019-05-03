import logging, pandas as pd, os
import numpy as np
import requests
import cirpy
import time
import pubchempy as pcp
import ctfile
import sdf
import ssl
import pronto
from pronto import Relationship
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


def split_rows(maf_df):
    # Split rows with pipe-lines "|"
    new_maf = pd.DataFrame(explode(explode(explode(explode(explode(maf_df.values, 0), 1), 2), 3), 4), columns=maf_df.columns)
    return new_maf


def explode(v, i, sep='|'):
    v = v.astype(str)
    n, m = v.shape
    a = v[:, i]
    bslc = np.r_[0:i, i + 1:m]
    asrt = np.append(i, bslc).argsort()
    b = v[:, bslc]
    a = np.core.defchararray.split(a, sep)
    A = np.concatenate(a)[:, None]
    counts = [len(x) for x in a.tolist()]
    rpt = np.arange(n).repeat(counts)
    return np.concatenate([A, b[rpt]], axis=1)[:, asrt]


def check_maf_for_pipes(study_location, annotation_file_name):
    annotation_file = os.path.join(study_location, annotation_file_name)
    try:
        maf_df = read_tsv(annotation_file)
    except FileNotFoundError:
        abort(400, "The file " + annotation_file + " was not found")
    maf_len = len(maf_df.index)

    # Any rows to split?
    new_maf_df = split_rows(maf_df)
    new_maf_len = len(new_maf_df.index)

    file_name = os.path.join(anno_sub_folder, annotation_file + '.split')
    if maf_len != new_maf_len:  # We did find |, so we create a new MAF
        write_tsv(new_maf_df, file_name)

    return maf_df, maf_len, new_maf_df, new_maf_len, file_name


def search_and_update_maf(study_location, annotation_file_name):
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

    standard_maf_columns = {"database_identifier": 0, "chemical_formula": 1, "smiles": 2, "inchi": 3}
    maf_compound_name_column = "metabolite_identification"

    try:
        maf_df = read_tsv(annotation_file_name)
    except FileNotFoundError:
        abort(400, "The file " + annotation_file_name + " was not found")
    maf_len = len(maf_df.index)

    # First make sure the existing pubchem annotated spreadsheet is loaded
    pubchem_df = maf_df.copy()

    if exiting_pubchem_file:  # The has already been split and this is an existing "pubchem" file
        new_maf_df = maf_df.copy()
        new_maf_len = len(new_maf_df.index)
    else:
        # Any rows to split?
        new_maf_df = split_rows(maf_df)
        new_maf_len = len(new_maf_df.index)

        if maf_len != new_maf_len:  # We did find | so we have to use the new dataframe
            maf_df = new_maf_df

        # Remove existing row values first, because that's what we do ;-)
        for column_name in standard_maf_columns:
            maf_df.iloc[:, standard_maf_columns[column_name]] = ""

        pubchem_df = create_pubchem_df(maf_df)

    row_idx = 0
    short_df = maf_df[["database_identifier", maf_compound_name_column]]

    # Search using the compound name column
    for idx, row in short_df.iterrows():
        database_id = row[0]
        comp_name = row[1]
        print(str(idx+1) + ' of ' + str(new_maf_len) + ' : ' + comp_name)
        if not database_id and comp_name:
            start_time = time.time()
            chebi_found = False
            comp_name = comp_name.rstrip()  # Remove trailing spaces
            # comp_name = comp_name.replace('Î´', 'δ')
            # '\u03b4', UnicodeEncodeError: 'latin-1' codec can't encode character '\u03b4' in position 8: ordinal not in range(256)
    #        comp_name = comp_name.encode('ascii', 'ignore')  # Make sure it's only searching using ASCII encoding

            if '/' in comp_name:  # Not a real name
                comp_name = comp_name.replace('/', ' ')

            search_res = wsc.get_maf_search("name", comp_name)  # This is the standard MetaboLights aka Plugin search
            if search_res['content']:
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
                # 4 is name / metabolite_identification from MAF

                if name:
                    if database_identifier:
                        if database_identifier.startswith('CHEBI:'):
                            chebi_found = True
                            logger.info("    -- Found ChEBI id " + database_identifier + " based on name")
                            print("    -- Found ChEBI id " + database_identifier + " based on name")
                        maf_df.iloc[row_idx, int(standard_maf_columns['database_identifier'])] = database_identifier
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
                csid = get_csid(ik)

                pubchem_df.iloc[row_idx, 5] = pc_name  # 5 PubChem name
                pubchem_df.iloc[row_idx, 6] = pc_cid   # 6 PubChem CID

                if not pc_cid:
                    pc_cid = get_pubchem_cid_on_inchikey(cactus_stdinchikey, opsin_stdinchikey)
                pubchem_df.iloc[row_idx, 7] = pc_cid  # 7 PubChem CID, if none get from InChIKey search (Cactus, OBSIN)
                pubchem_df.iloc[row_idx, 8] = csid  # 8 ChemSpider ID (CSID) from INCHI
                pubchem_df.iloc[row_idx, 9] = get_ranked_values(pc_smiles, cactus_smiles, opsin_smiles, None)  # 9 final smiles
                final_inchi = get_ranked_values(pc_inchi, cactus_inchi, opsin_inchi, None)  # 10 final inchi
                pubchem_df.iloc[row_idx, 10] = final_inchi
                final_inchi_key = get_ranked_values(pc_inchi_key, cactus_stdinchikey, opsin_stdinchikey, None)  # 11 final inchikey
                pubchem_df.iloc[row_idx, 11] = final_inchi_key
                pubchem_df.iloc[row_idx, 12] = pc_smiles  # 12 pc_smiles
                pubchem_df.iloc[row_idx, 13] = cactus_smiles   # 13 cactus_smiles
                pubchem_df.iloc[row_idx, 14] = opsin_smiles  # 14 opsin_smiles
                pubchem_df.iloc[row_idx, 15] = pc_inchi  # 15 PubChem inchi
                pubchem_df.iloc[row_idx, 16] = cactus_inchi  # 16 Cacus inchi
                pubchem_df.iloc[row_idx, 17] = opsin_inchi   # 17 Opsin inchi
                pubchem_df.iloc[row_idx, 18] = pc_inchi_key  # 18 PubChem stdinchikey
                pubchem_df.iloc[row_idx, 19] = cactus_stdinchikey  # 19 cactus_stdinchikey
                pubchem_df.iloc[row_idx, 20] = opsin_stdinchikey   # 20 opsin_stdinchikey
                pubchem_df.iloc[row_idx, 21] = pc_formula   # 21 PubChem formula
                pubchem_df.iloc[row_idx, 22] = pc_synonyms  # 22 PubChem synonyms
                pubchem_df.iloc[row_idx, 23] = cactus_synonyms  # 23 Cactus synonyms
                pubchem_df.iloc[row_idx, 24] = pc_structure  # 24 PubChem structure (SDF)

                # Now we have more information, so let's try to search ChEBI again

                if final_inchi_key and len(final_inchi_key) > 0:
                    chebi_id, inchi, inchikey, name, smiles, formula = direct_chebi_search(final_inchi_key, comp_name)
                    if chebi_id:
                        database_identifier = chebi_id
                        chemical_formula = formula
                        smiles = smiles
                        inchi = inchi
                        name = name

                        logger.info("    -- Found ChEBI id " + database_identifier + " based on final InChIKey")
                        print('    -- Found ChEBI id ' + database_identifier + ' based on final InChIKey')
                        pubchem_df.iloc[row_idx, 0] = database_identifier
                        pubchem_df.iloc[row_idx, 1] = chemical_formula
                        pubchem_df.iloc[row_idx, 2] = smiles
                        pubchem_df.iloc[row_idx, 3] = inchi
                        # 4 is name / metabolite_identification from MAF

                        if name:  # Add to the annotated file as well
                            if database_identifier:
                                maf_df.iloc[row_idx, int(standard_maf_columns['database_identifier'])] = database_identifier
                            if chemical_formula:
                                maf_df.iloc[row_idx, int(standard_maf_columns['chemical_formula'])] = chemical_formula
                            if smiles:
                                maf_df.iloc[row_idx, int(standard_maf_columns['smiles'])] = smiles
                            if inchi:
                                maf_df.iloc[row_idx, int(standard_maf_columns['inchi'])] = inchi

                    else:
                        # Now, if we still don't have a ChEBI accession, download the structure (SDF) from PubChem
                        # and the classyFire SDF
                        create_annotation_folder(study_location + os.sep + anno_sub_folder)
                        sdf_file_list = get_sdf(study_location, pc_cid, pc_name, sdf_file_list, final_inchi)

            logger.info("    -- Search took %s seconds" % round(time.time() - start_time, 2))
            print("    -- Search took %s seconds" % round(time.time() - start_time, 2))
        else:
            print("    -- Skipping. Found database id  " + database_id)

        row_idx += 1

    write_tsv(maf_df, short_file_name + "_annotated.tsv")
    pubchem_file = short_file_name + pubchem_end
    write_tsv(pubchem_df, pubchem_file)
    if sdf_file_list:
        concatenate_sdf_files(sdf_file_list, study_location + os.sep + anno_sub_folder + os.sep,
                              short_file_name + complete_end, short_file_name + classyfire_end)

    return maf_df, maf_len, new_maf_df, new_maf_len, pubchem_file


def create_annotation_folder(folder_loc):
    if not os.path.exists(folder_loc):
        os.makedirs(folder_loc)


def concatenate_sdf_files(sdf_file_list, study_location, sdf_file_name, classyfire_file_name):
    return_format = 'sdf'  # 'json' will require a new root element to separate the entries before merging
    classyfire_file_name = classyfire_file_name + '.' + return_format

    # Create a new concatenated SDF file
    with open(sdf_file_name, 'w') as outfile:
        # SDF file list = [file name, classyFire process id]
        for entry in sdf_file_list:
            fname, cf_id = entry
        # for fname in sdf_file_list:
            # First remove the hydrogens. OpenBabel 'Delete hydrogens (make implicit)'
            # remove_hydrogens(fname)
            with open(os.path.join(study_location, fname)) as infile:
                for line in infile:
                    outfile.write(line)
            # Now, get the classyFire queries, add to classyfire_file_name and get ancestors
            get_classyfire_results(cf_id, classyfire_file_name, return_format)

        get_ancestors(classyfire_file_name)

        # If we have a real new SDF file, remove the smaller sdf files
        # remove_sdf_files(sdf_file_name, study_location, sdf_file_list)


# def remove_hydrogens(sdf_file):
#     for mol in pybel.readfile("sdf", sdf_file):
#         mol.removeh()
#
#     if mol:
#         with open(sdf_file, 'w') as outfile:
#             outfile.write(mol)


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
    print("    -- Querying ClassyFire")
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
        print("    -- Error querying ClassyFire: " + str(e))
    return query_id


def get_classyfire_results(query_id, classyfire_file_name, return_format):

    if query_id:
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
    print('loading ChEBI mapping file ' + mapping_file)
    return read_tsv(mapping_file)


def get_ancestors(classyfire_file_name):
    # get mappings from ClassyFire names (in classyfire_file_name) to ChEBI
    # with open(classyfire_file_name, 'r') as infile:
    #     mols = ctfile.load(infile)

    lines = []
    direct_parents = []
    alt_parents = []
    is_a_list = []
    with open(classyfire_file_name, 'r') as infile:
        for line in infile:
            lines.append(line.rstrip('\n'))

    for idx, line in enumerate(lines):
        if line == "> <Direct Parent>":
            direct_parents.append(lines[idx+1])
        elif line == "> <Alternative Parents>":
            alt_parents.append(lines[idx + 1])

    onto = get_chebi_obo_file()
    for compound_name in direct_parents:
        chebi_compound = get_chebi_mapping(compound_name)
        is_a_list = get_is_a(onto, chebi_compound)

    return is_a_list


def get_chebi_obo_file():
    print('loading ChEBI OBO file')
    obo_file = app.config.get('OBO_FILE')
    onto = pronto.Ontology(obo_file)
    return onto


def get_chebi_mapping(compound_name):
    print('Reading ClassyFire to ChEBI mapping file')
    mapping_file = load_chebi_classyfire_mapping()

    return mapping_file


def get_is_a(onto, chebi_compound):
    print('Get ChEBI parents')
    is_a_list = onto[chebi_compound].rparents()
    return is_a_list


def direct_chebi_search(final_inchi, comp_name):
    chebi_id = ""
    inchi = ""
    inchikey = ""
    name = ""
    smiles = ""
    formula = ""
    logger.info("    -- Querying ChEBI web services for " + comp_name + " based on final InChIKey " + final_inchi)
    print("    -- Querying ChEBI web services for " + comp_name + " based on final InChIKey " + final_inchi)
    url = app.config.get('CHEBI_URL')
    client = Client(url)
    try:
        lite_entity = client.service.getLiteEntity(final_inchi, 'INCHI_INCHI_KEY', '10', 'ALL')
        top_result = lite_entity[0]
        chebi_id = top_result.chebiId

        complete_entity = client.service.getCompleteEntity(chebi_id)
        inchi = complete_entity.inchi
        inchikey = complete_entity.inchiKey
        name = complete_entity.chebiAsciiName
        smiles = complete_entity.smiles
        formula = complete_entity.Formulae[0].data

    except Exception as e:
        logger.error(str(e))
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
    pubchem_df = maf_df[['database_identifier', 'chemical_formula', 'smiles', 'inchi', 'metabolite_identification']]
    pubchem_df['iupac_name'] = ''       # 5
    pubchem_df['pubchem_cid'] = ''      # 6
    pubchem_df['pubchem_cid_ik'] = ''   # 7  PubChem CID from InChIKey search (Cactus, OBSIN)
    pubchem_df['csid_ik'] = ''          # 8  ChemSpider ID (CSID) from INCHIKEY

    pubchem_df['final_smiles'] = ''     # 9
    pubchem_df['final_inchi'] = ''      # 10
    pubchem_df['final_inchi_key'] = ''  # 11

    pubchem_df['pubchem_smiles'] = ''   # 12
    pubchem_df['cactus_smiles'] = ''    # 13
    pubchem_df['opsin_smiles'] = ''     # 14

    pubchem_df['pubchem_inchi'] = ''    # 15
    pubchem_df['cactus_inchi'] = ''     # 16
    pubchem_df['opsin_inchi'] = ''      # 17

    pubchem_df['pubchem_inchi_key'] = ''    # 18
    pubchem_df['cactus_inchi_key'] = ''     # 19
    pubchem_df['opsin_inchi_key'] = ''      # 20

    pubchem_df['pubchem_formula'] = ''      # 21
    pubchem_df['pubchem_synonyms'] = ''  # 22
    pubchem_df['cactus_synonyms'] = ''  # 23

    pubchem_df['structure'] = ''  # 24

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


def cactus_search(comp_name, type):
    result = cirpy.resolve(comp_name, type)
    synonyms = ""
    if result:
        if type == 'stdinchikey':
            return result.replace('InChIKey=', '')
        if type == 'names':
            for synonym in result:
                if get_relevant_synonym(synonym):
                    synonyms = synonyms + ';' + synonym
            return synonyms

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

    try:
        compound = None
        # For this to work on Mac, run: cd "/Applications/Python 3.6/"; sudo "./Install Certificates.command
        # or comment out the line below:
        ssl._create_default_https_context = ssl._create_unverified_context  # If no root certificates installed
        try:
            pubchem_compound = get_compounds(comp_name, namespace=search_type)
            compound = pubchem_compound[0]  # Only read the first record from PubChem = preferred entry
        except IndexError:
            logger.info('Could not find PubChem compound for ' + comp_name)   # Nothing was found

        if compound:
            inchi = compound.inchi
            inchi_key = compound.inchikey
            smiles = compound.canonical_smiles
            iupac = compound.iupac_name
            iupac = iupac.replace('f', '').replace('{', '').replace('}', '')
            cid = compound.cid
            formula = compound.molecular_formula
            for synonym in compound.synonyms:
                if get_relevant_synonym(synonym):
                    synonyms = synonyms + ';' + synonym

            if synonyms:
                synonyms = synonyms.replace(";", "", 1)  # Remove the leading ";"

            logger.debug('Searching PubChem for "' + comp_name + '", got cid "' + str(cid) +
                         '" and iupac name "' + iupac + '"')
    except Exception as error:
        logger.error("Unable to search PubChem for compound " + comp_name)
        logger.error(error)

    return iupac, inchi, inchi_key, smiles, cid, formula, synonyms, structure


def get_sdf(study_location, cid, iupac, sdf_file_list, final_inchi):
    if study_location and cid:
        if not iupac or len(iupac) < 1:
            iupac = 'no name given'
        classifyre_id = ''
        logger.info("    -- Getting SDF for CID " + str(cid) + " for name: " + iupac)
        print("    -- Getting SDF for CID " + str(cid) + " for name: " + iupac)
        file_name = str(cid) + '.sdf'
        pcp.download('SDF', study_location + os.sep + anno_sub_folder + os.sep + file_name, cid, overwrite=True)

        if final_inchi:
            classifyre_id = classyfire(final_inchi)

        sdf_file_list.append([file_name, classifyre_id])

    return sdf_file_list


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
        annotation_file_name = args['annotation_file_name']

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


class SearchNamesMaf(Resource):
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
        annotation_file_name = args['annotation_file_name']

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
                    maf_df, maf_len, new_maf_df, new_maf_len, pubchem_file = search_and_update_maf(study_location, file_name)
                    if maf_len != new_maf_len:
                        maf_changed += 1
        else:
            maf_df, maf_len, new_maf_df, new_maf_len, pubchem_file = search_and_update_maf(study_location, annotation_file_name)
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
