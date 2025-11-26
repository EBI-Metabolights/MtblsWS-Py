#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Feb-13
#  Modified by:   kenneth
#
#  Copyright 2020 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import glob
import hashlib
import json
import logging
import os
import random
import re
import shutil
from typing import Dict, List, Set, Tuple

import numpy as np
import pandas as pd
from isatools.model import Assay, Investigation, OntologyAnnotation, Study
from lxml import etree
from mzml2isa.parsing import convert as isa_convert
from pandas import DataFrame

from app.config import get_settings
from app.utils import current_time
from app.ws.db_connection import update_release_date
from app.ws.isaApiClient import IsaApiClient
from app.ws.settings.utils import get_study_settings
from app.ws.utils import (
    copy_file,
    get_absolute_path,
    get_year_plus_one,
    read_tsv,
    write_tsv,
)

"""
Utils

Misc of utils
"""

logger = logging.getLogger("wslog")
iac = IsaApiClient()


def validate_mzml_files(study_id, study_path):
    status, result = True, "All mzML files validated in both study and upload folder"
    settings = get_study_settings()
    parent = os.path.join(
        settings.mounted_paths.study_internal_files_root_path,
        study_id,
        "metabolon_pipeline",
    )

    os.makedirs(parent, exist_ok=True)
    validated_files = os.path.join(parent, "validation_result.json")
    validation_results = {}
    try:
        if os.path.exists(validated_files):
            with open(validated_files, "r") as f:
                validation_results = json.load(f)
            logger.info(
                "Loading previous mzML file validation result from %s", validated_files
            )
    except Exception as ex:
        logger.error("Error while reading %s: %s", validated_files, str(ex))
        validation_results = {}
    if not validation_results:
        validation_results = {}
    settings = get_settings()
    study_folder = study_path
    xsd_path = settings.file_resources.mzml_xsd_schema_file_path
    xmlschema_doc = etree.parse(xsd_path)
    xmlschema = etree.XMLSchema(xmlschema_doc)
    # Getting xsd schema for validation
    invalid_files = {}
    if os.path.isdir(study_folder):  # Only check if the folder exists
        top_file_set = set(
            glob.iglob(os.path.join(study_folder, "*.mzML"))
        )  # Are there mzML files there?
        # if len(top_file_list) == 0:  # No files, check sub-folders
        #     logger.info('Could not find any mzML files, checking any sub-folders')
        subfolder_file_set = set(
            glob.iglob(os.path.join(study_folder, "**/*.mzML"), recursive=True)
        )
        all_mzml_files_set = subfolder_file_set.union(top_file_set)
        all_mzml_files = list(all_mzml_files_set)
        all_mzml_files.sort()
        if len(all_mzml_files) == 0:
            message = f"No mzML files within study folder: {study_folder}"
            logger.error(message)
            return False, message
        for idx, file in enumerate(all_mzml_files):
            valid = validation_results.get(file)
            if valid:
                logger.info("Skipping mzML file " + file)
                continue
            try:
                logger.info("Validating mzML file " + file)
                status, result = validate_xml(xml=file, xmlschema=xmlschema)
                logger.info(f"{file}: validated")
                validation_results[file] = status
                if not status:
                    invalid_files[file] = result
            except Exception as e:
                invalid_files[file] = str(e)
                logger.error(f"{file}: failed: {invalid_files[file]}")
                # return False, f"Error while validating file {file}: {str(e)}"
            finally:
                with open(validated_files, "w") as f:
                    json.dump(validation_results, f)
    else:
        message = f"Study folder does not exist: {study_folder}"
        logger.error(message)
        return False, message
    if invalid_files:
        return False, json.dumps(invalid_files)
    return True, ""


def validate_mzml_file(file_path: str):
    status, result = True, "All mzML files validated in both study and upload folder"
    settings = get_settings()
    xsd_path = settings.file_resources.mzml_xsd_schema_file_path
    xmlschema_doc = etree.parse(xsd_path)
    xmlschema = etree.XMLSchema(xmlschema_doc)
    # Getting xsd schema for validation

    try:
        logger.info("Validating mzML file " + file_path)
        status, result = validate_xml(xml=file_path, xmlschema=xmlschema)
        return status, result["Error"]
    except Exception as e:
        return False, f"Error while validating file {file_path}: {str(e)}"


def validate_xml(xml=None, xmlschema=None) -> Tuple[bool, dict[str, str]]:
    # parse xml
    try:
        doc = etree.parse(xml)
    except IOError:
        print("Schema validation error. File read error: " + xml)
        return False, {"Error": "Can not read the file " + xml}
    except etree.XMLSyntaxError:
        print("Schema validation error. Invalid XML, schema validation failed: " + xml)

        return False, {"Error": "File " + xml + " is not a valid XML file"}
    except Exception as ex:
        return False, {"Error": str(ex)}
    # validate against schema
    try:
        xmlschema.assertValid(doc)
        # print('XML valid, schema validation ok: ' + xml)
        return True, "File " + xml + " is a valid XML file"
    except etree.DocumentInvalid:
        print("Schema validation error. " + xml)
        return False, {"Error": "Can not validate the file " + xml}


def to_isa_tab(study_id, input_folder, output_folder):
    try:
        isa_convert(input_folder, output_folder, study_id, jobs=1, verbose=True)
    except Exception as e:
        print(
            "Could not convert mzML to ISA-Tab study " + study_id + ". " + input_folder
        )
        return (
            False,
            "Could not convert mzML to ISA-Tab study "
            + study_id
            + ": "
            + input_folder
            + ". "
            + str(e),
        )

    return True, "ISA-Tab files generated for study " + study_id


def create_temp_dir_in_study_folder(parent_folder: str) -> str:
    date = current_time().strftime("%m/%d/%Y, %H:%M:%S")
    rand = random.randint(1000, 9999999)
    folder_name = f"{date}-{str(rand)}"
    random_folder_name = hashlib.sha256(bytes(folder_name, "utf-8")).hexdigest()
    path = os.path.join(parent_folder, random_folder_name)
    os.makedirs(path, exist_ok=True)

    return path


def collect_all_mzml_files(study_location: str, study_id: str) -> List[str]:
    settings = get_study_settings()
    temp_folder = os.path.join(
        settings.mounted_paths.study_internal_files_root_path,
        study_id,
        "metabolon_pipeline",
    )
    # working_path = create_temp_dir_in_study_folder(parent_folder=temp_folder)
    working_path = temp_folder
    files_folder = study_location
    mzml_files = {}
    all_files = []
    if os.path.exists(files_folder) and os.path.isdir(
        files_folder
    ):  # Only check if the folder exists
        files = glob.iglob(
            os.path.join(files_folder, "*.mzML")
        )  # Are there mzML files there?
        for file in files:
            base_name = os.path.basename(file)
            if base_name not in mzml_files:
                mzml_files[base_name] = file
                all_files.append(file)
        files = glob.iglob(
            os.path.join(files_folder, "**/*.mzML"), recursive=True
        )  # Are there mzML files there?
        for file in files:
            base_name = os.path.basename(file)
            if base_name not in mzml_files:
                mzml_files[base_name] = file
                all_files.append(file)
    return split_files_to_subfolders(
        all_files, files_folder, working_path, max_file_size=10
    )


def split_files_to_subfolders(
    mzml_files: List[str],
    files_folder,
    working_path: str,
    min_file_size: int = 10,
    max_file_size: int = 10,
):
    files = sorted([x for x in mzml_files])
    all_sub_paths: List[Dict[str, List[str]]] = {}
    sub_folder_items: List[str] = []
    sub_folder_path = None
    previous_dir = None
    for file in files:
        current_dir = os.path.dirname(file)
        if len(sub_folder_items) >= max_file_size:
            sub_folder_items = []
        if current_dir != previous_dir:
            sub_folder_items = []
        previous_dir = current_dir

        if not sub_folder_items:
            sub_folder_name = f"MZML_{(len(all_sub_paths) + 1):04}"
            sub_folder_path = os.path.join(working_path, sub_folder_name)
            files_sub_folder_path = os.path.join(sub_folder_path)
            if os.path.exists(sub_folder_path):
                linked_files = list(
                    glob.iglob(os.path.join(files_sub_folder_path, "*.mzML"))
                )
                for item in linked_files:
                    if os.path.islink(item):
                        os.unlink(item)
                    elif os.path.isfile(item):
                        os.remove(item)
                    elif os.path.isdir(item):
                        shutil.rmtree(item, ignore_errors=True)

            os.makedirs(files_sub_folder_path, exist_ok=True)

            all_sub_paths[sub_folder_path] = sub_folder_items

        file_basename = os.path.basename(file)
        target_path = os.path.join(files_sub_folder_path, file_basename)
        os.symlink(file, target_path, target_is_directory=False)
        sub_folder_items.append(file)
    logger.info(f"Splitted folders {len(all_sub_paths)}")
    return list(all_sub_paths.keys())


def convert_to_isa(study_location: str, study_id: str) -> Tuple[bool, str]:
    try:
        subfolders: List[str] = collect_all_mzml_files(study_location, study_id)

        for sub_path in subfolders:
            file_path = os.path.join(sub_path, "i_Investigation.txt")
            input_files_path = os.path.join(sub_path)
            if not os.path.exists(file_path):
                status, message = to_isa_tab("", input_files_path, sub_path)
                if not status:
                    return status, message
        return True, ""
    except Exception as exc:
        logger.error(str(exc))
        return False, str(exc)


ss_id_pattern = re.compile(r"(.*)SSID_Data.*\.csv", re.IGNORECASE)


def check_input_files(study_id: str, study_location: str):
    logger.info("Study file location: %s", study_location)
    samples_files_result = glob.iglob(os.path.join(study_location, "*SSID_Data*.csv"))
    samples_files = [x for x in samples_files_result]
    samples_files.sort()
    sample_ids: Set[str] = set()

    for sample_file in samples_files:
        basename = os.path.basename(sample_file)
        match = ss_id_pattern.match(basename)
        match_result = match.groups()[0].strip() if match else ""
        if match_result:
            sample_ids.add(match_result)
    logger.info("SSID Files: ", ", ".join(samples_files))
    if len(sample_ids) == 0:
        return False, "There is no *SSID_Data*.csv file."
    else:
        search_pattern = "*Peak_Area*.xlsx"
        for sample_id in sample_ids:
            search_pattern = f"*{sample_id}*Peak_Area*.xlsx"
            peak_table_paths = list(
                glob.iglob(os.path.join(study_location, search_pattern))
            )

            if len(peak_table_paths) > 1:
                return (
                    False,
                    f"There are multiple Peak Area Table xlsx for {sample_id}.",
                )
            elif len(peak_table_paths) == 0:
                return False, f"There is no Peak Area Table xlsx table for {sample_id}."

    for sample_id in sample_ids:
        if len(sample_ids) > 1:
            peak_tables_search = glob.iglob(
                os.path.join(study_location, f"*{sample_id}*Peak_Area*.xlsx")
            )
        else:
            peak_tables_search = glob.iglob(
                os.path.join(study_location, "*Peak_Area*.xlsx")
            )
        peak_table_paths: List[str] = [x for x in peak_tables_search]
        peak_table_paths.sort()
        if len(peak_table_paths) > 1:
            return False, "There are multiple Peak Area Table xlsx table."
        if len(peak_table_paths) == 0:
            return False, f"There is no Peak Area Table xlsx table for {sample_id}."

    for peak_table_file_path in peak_table_paths:
        try:
            validate_peak_table(peak_table_file_path)
        except Exception as ex:
            return (
                False,
                f"Peak table validation failed for {peak_table_file_path}: {str(ex)}",
            )
    return True, ""


def create_isa_files(
    study_id, study_location, target_location: str = None
) -> Tuple[bool, str]:
    settings = get_study_settings()
    if not target_location:
        target_location = study_location

    temp_folder = os.path.join(
        settings.mounted_paths.study_internal_files_root_path,
        study_id,
        "metabolon_pipeline",
    )
    assay_files_result = glob.iglob(os.path.join(temp_folder, "MZML_*/a_*.txt"))
    assay_files = [x for x in assay_files_result]
    assay_files.sort()
    merged_assay_df: DataFrame = None
    for assay in assay_files:
        assay_df = read_tsv(assay)
        if merged_assay_df is None:
            merged_assay_df = assay_df
        else:
            merged_assay_df = pd.concat([merged_assay_df, assay_df])
    assay_file_names: Dict[str, List[str]] = {}

    files = glob.iglob(
        os.path.join(study_location, "*.mzML")
    )  # Are there mzML files there?
    files_list = [f for f in files]
    files = glob.iglob(os.path.join(study_location, "**/*.mzML"), recursive=True)
    files_list.extend([f for f in files])
    file_paths = DataFrame.from_records(
        [
            {
                "File Name": os.path.basename(f),
                "File Path": f.replace(f"{study_location}/", ""),
            }
            for f in files_list
        ]
    )

    samples_files_result = glob.iglob(os.path.join(study_location, "*SSID_Data*.csv"))
    samples_files = [x for x in samples_files_result]
    samples_files.sort()
    sample_csv_map: Dict[str, DataFrame] = {}

    for sample_file in samples_files:
        basename = os.path.basename(sample_file)
        sampe_df = pd.read_csv(sample_file, sep=",")
        sampe_df = sampe_df.replace(np.nan, "", regex=True)  # Remove NaN
        match = ss_id_pattern.match(basename)
        match_result = match.groups()[0].strip() if match else ""
        sample_csv_map[match_result.strip()] = sampe_df

    sample_id_sample_name_map: Dict[str, DataFrame] = {}

    for sample_id in sample_csv_map:
        if sample_id not in assay_file_names:
            assay_file_names[sample_id] = []
        for column_name in sample_csv_map[sample_id].columns:
            if column_name == "SAMPLE_NAME":
                continue
            rows = sample_csv_map[sample_id][[column_name, "SAMPLE_NAME"]].copy()
            rows.rename(columns={column_name: "SOURCE_NAME"}, inplace=True)
            if sample_id not in sample_id_sample_name_map:
                sample_id_sample_name_map[sample_id] = rows
            else:
                sample_id_sample_name_map[sample_id] = pd.concat(
                    [sample_id_sample_name_map[sample_id], rows], ignore_index=True
                )

            assay_file_name = create_assay_file(
                study_id,
                target_location,
                merged_assay_df,
                sample_csv_map,
                sample_id,
                column_name,
                file_paths,
            )
            assay_file_names[sample_id].append(assay_file_name)

    all_sample_names = None
    for sample_id in sample_id_sample_name_map:
        if all_sample_names is None:
            all_sample_names = sample_id_sample_name_map[sample_id].copy()
        else:
            all_sample_names = pd.concat(
                [all_sample_names, sample_id_sample_name_map[sample_id]],
                ignore_index=True,
            )

    sample_name_client_id_df = None
    add_sample_id = True if len(sample_id_sample_name_map) > 1 else False
    for sample_id in sample_id_sample_name_map:
        if len(sample_id_sample_name_map) > 1:
            peak_tables_search = glob.iglob(
                os.path.join(study_location, f"*{sample_id}*Peak_Area*.xlsx")
            )
        else:
            peak_tables_search = glob.iglob(
                os.path.join(study_location, "*Peak_Area*.xlsx")
            )
        peak_table_paths: List[str] = [x for x in peak_tables_search]
        peak_table_paths.sort()
        if len(peak_table_paths) > 1:
            logger.warning(
                f"There are multiple Peak Area Table xlsx table. Only {peak_table_paths[0]} will be used."
            )
        if len(peak_table_paths) > 0:
            # select only one Peak Area Table
            df = create_maf_file(
                study_id, target_location, sample_id, peak_table_paths[0], add_sample_id
            )
            if sample_name_client_id_df is None:
                sample_name_client_id_df = df
            else:
                sample_name_client_id_df = pd.concat(
                    [sample_name_client_id_df, df], ignore_index=True
                )

    create_sample_file(
        study_id,
        target_location,
        temp_folder,
        all_sample_names,
        sample_name_client_id_df,
    )

    create_investigation_file(study_id, target_location, assay_file_names)
    return True, "ISA files are created successfully."


def validate_peak_table(peak_table_file_path: str):
    metabolon_template_path = (
        get_settings().file_resources.study_partner_metabolon_template_path
    )
    annotation_file_template_path = os.path.join(
        metabolon_template_path,
        "m_metabolite_profiling_mass_spectrometry_v2_maf.tsv",
    )
    annotation_file_template = get_absolute_path(annotation_file_template_path)
    maf_template: DataFrame = read_tsv(annotation_file_template)

    maf_template = maf_template[0:0]
    main_table: DataFrame = pd.read_excel(
        peak_table_file_path, engine="openpyxl", dtype=str, index_col=None, header=None
    )
    main_table.dropna(axis=0, how="all", inplace=True)
    main_table.replace(np.nan, "", regex=True, inplace=True)
    temp_df = main_table[:2]

    parent_sample_row_index = -1
    for col in temp_df.columns:
        for i in range(2):
            if "SAMPLE" in temp_df.iloc[i][col].upper():
                parent_sample_row_index = i
                break

    first_data_row = -1
    for i in range(10):
        if len(main_table) > i and (
            "BIOCHEMICAL" == main_table.iloc[i, 0]
            or "CHEMICAL_NAME" == main_table.iloc[i, 0]
        ):
            first_data_row = i
            break

    if first_data_row < 0:
        raise Exception("BIOCHEMICAL or CHEMICAL_NAME column does not exist.")

    if parent_sample_row_index < 0:
        raise Exception("PARENT SAMPLE NAME column does not exist in first 2 rows.")


def create_maf_file(
    study_id: str,
    study_location: str,
    sample_id: str,
    peak_table_file_path: str,
    use_sample_id_in_filename: bool = False,
):
    pd.options.mode.copy_on_write = True
    metabolon_template_path = (
        get_settings().file_resources.study_partner_metabolon_template_path
    )
    maf_template: DataFrame = read_tsv(
        os.path.join(
            metabolon_template_path,
            "m_metabolite_profiling_mass_spectrometry_v2_maf.tsv",
        )
    )

    maf_template = maf_template[0:0]
    main_table: DataFrame = pd.read_excel(
        peak_table_file_path, engine="openpyxl", dtype=str, index_col=None, header=None
    )
    main_table.dropna(axis=0, how="all", inplace=True)
    main_table.replace(np.nan, "", regex=True, inplace=True)
    temp_df = main_table[:2]

    parent_sample_column_index = -1
    client_id_row_index = -1
    parent_sample_row_index = -1
    for col in temp_df.columns:
        for i in range(2):
            if "CLIENT" in temp_df.iloc[i][col].upper():
                client_id_row_index = i
                break
        for i in range(2):
            if "SAMPLE" in temp_df.iloc[i][col].upper():
                parent_sample_row_index = i
                break
        if parent_sample_row_index > -1 or client_id_row_index > -1:
            parent_sample_column_index = temp_df.columns.get_loc(col)
            break
    first_data_row = -1
    metbolite_identification_column_name = "BIOCHEMICAL"
    for i in range(10):
        if len(main_table) > i and (
            "BIOCHEMICAL" == main_table.iloc[i, 0]
            or "CHEMICAL_NAME" == main_table.iloc[i, 0]
        ):
            if "CHEMICAL_NAME" == main_table.iloc[i, 0]:
                metbolite_identification_column_name = "CHEMICAL_NAME"
            first_data_row = i
            break
    if first_data_row < 0:
        raise Exception("BIOCHEMICAL or CHEMICAL_NAME column does not  exist.")

    if parent_sample_row_index < 0:
        raise Exception("PARENT SAMPLE NAME column does not  exist.")

    df_header: DataFrame = DataFrame()
    df_header["PARENT_SAMPLE_NAME"] = temp_df.iloc[
        parent_sample_row_index, parent_sample_column_index + 1 :
    ]

    if client_id_row_index >= 0:
        df_header["CLIENT_ID"] = temp_df.iloc[
            client_id_row_index, parent_sample_column_index + 1 :
        ]
    else:
        df_header["CLIENT_ID"] = temp_df.iloc[
            parent_sample_row_index, parent_sample_column_index + 1 :
        ]

    sample_name_client_id_df = df_header

    maf_header: DataFrame = main_table.iloc[first_data_row][
        : parent_sample_column_index + 1
    ]
    sample_headers = main_table.iloc[parent_sample_row_index][
        parent_sample_column_index + 1 :
    ]
    maf_header = pd.concat([maf_header, sample_headers], ignore_index=True)

    maf_header_update_params = {i: maf_header.iloc[i] for i in range(len(maf_header))}

    if client_id_row_index >= 0:
        table = main_table[first_data_row:]
        table.iloc[0, :] = temp_df.iloc[client_id_row_index, :]
    else:
        table = main_table[first_data_row + 1 :]

    table.rename(columns=maf_header_update_params, inplace=True)

    maf_template[list(table.columns)] = table

    maf_template["metabolite_identification"] = maf_template[
        metbolite_identification_column_name
    ]
    maf_template.drop(
        columns=[metbolite_identification_column_name], axis=1, inplace=True
    )

    maf_file_name_prefix = (
        f"m_{study_id}_{sample_id}" if use_sample_id_in_filename else f"m_{study_id}"
    )
    maf_file_name = (
        f"{maf_file_name_prefix}_metabolite_profiling_mass_spectrometry_v2_maf.tsv"
    )
    maf_file_name_path = os.path.join(study_location, maf_file_name)
    maf_template.to_csv(
        maf_file_name_path, sep="\t", encoding="utf-8", index=False, header=True
    )

    return sample_name_client_id_df


def create_sample_file(
    study_id, study_location, temp_folder, all_sample_names, sample_name_client_id_df
):
    sample_files_result = glob.iglob(os.path.join(temp_folder, "MZML_*/s_*.txt"))
    sample_files = [x for x in sample_files_result]
    sample_files.sort()
    merged_df: DataFrame = None
    for sample in sample_files:
        sample_df = read_tsv(sample)
        if merged_df is None:
            merged_df = sample_df
        else:
            merged_df = pd.concat([merged_df, sample_df], ignore_index=True)
    merged_data = all_sample_names[["SOURCE_NAME", "SAMPLE_NAME"]].merge(
        merged_df,
        right_on="Sample Name",
        left_on="SOURCE_NAME",
        how="right",
        sort=False,
    )
    merged_data = merged_data.merge(
        sample_name_client_id_df,
        right_on="PARENT_SAMPLE_NAME",
        left_on="SAMPLE_NAME",
        how="left",
        sort=False,
    )

    merged_data["Sample Name"] = merged_data["SAMPLE_NAME"]
    merged_data["Source Name"] = merged_data["CLIENT_ID"]
    merged_data.drop(
        columns=["SAMPLE_NAME", "SOURCE_NAME", "CLIENT_ID", "PARENT_SAMPLE_NAME"],
        inplace=True,
        axis=1,
    )
    merged_data.drop_duplicates(subset=["Sample Name"], inplace=True)

    final_sample_file_data = pd.DataFrame()
    final_sample_file_data["Source Name"] = merged_data["Source Name"]
    final_sample_file_data["Characteristics[Organism]"] = ""
    final_sample_file_data["Term Source REF"] = ""
    final_sample_file_data["Term Accession Number"] = ""

    final_sample_file_data["Characteristics[Organism part]"] = ""
    final_sample_file_data["Term Source REF.1"] = ""
    final_sample_file_data["Term Accession Number.1"] = ""

    final_sample_file_data["Characteristics[Variant]"] = ""
    final_sample_file_data["Term Source REF.2"] = ""
    final_sample_file_data["Term Accession Number.2"] = ""

    final_sample_file_data["Characteristics[Sample type]"] = "experimental sample"
    final_sample_file_data["Term Source REF.3"] = "CHMO"
    final_sample_file_data["Term Accession Number.3"] = (
        "http://purl.obolibrary.org/obo/CHMO_0002746"
    )
    final_sample_file_data["Characteristics[Cell type]"] = ""
    final_sample_file_data["Term Source REF.3"] = ""
    final_sample_file_data["Term Accession Number.3"] = ""

    final_sample_file_data["Characteristics[Disease]"] = ""
    final_sample_file_data["Term Source REF.4"] = ""
    final_sample_file_data["Term Accession Number.4"] = ""
    final_sample_file_data["Protocol REF"] = "Sample collection"
    final_sample_file_data["Sample Name"] = merged_data["Sample Name"]

    write_tsv(final_sample_file_data, os.path.join(study_location, f"s_{study_id}.txt"))


assay_method_params = {
    "METHOD1": {
        "prefix": "POS_1",
        "column_model": "ACQUITY UPLC BEH C18 (1.7 µm, 2.1 mm x 100 mm; Waters)",
        "column_type": "reverse phase",
    },
    "METHOD2": {
        "prefix": "POS_2",
        "column_model": "ACQUITY UPLC BEH C18 (1.7 µm, 2.1 mm x 100 mm; Waters)",
        "column_type": "reverse phase",
    },
    "METHOD3": {
        "prefix": "NEG_1",
        "column_model": "ACQUITY UPLC BEH C18 (1.7 µm, 2.1 mm x 100 mm; Waters)",
        "column_type": "reverse phase",
    },
    "METHOD4": {
        "prefix": "NEG_2",
        "column_model": "ACQUITY UPLC BEH Amide (1.7 µm, 2.1 mm x 150 mm; Waters)",
        "column_type": "HILIC",
    },
}


def create_assay_file(
    study_id: str,
    study_location: str,
    merged_assay_df: DataFrame,
    sample_csv_map: Dict[str, DataFrame],
    sample_id: str,
    sample_csv_column_name: str,
    file_paths: DataFrame,
):
    names = sample_csv_map[sample_id][sample_csv_column_name]
    method_name = sample_csv_column_name.replace(" ", "").upper()
    sample_col = "Sample Name"
    splitted_assay_df = merged_assay_df.loc[merged_assay_df[sample_col].isin(names)]

    splitted_assay_df.loc[:, "Parameter Value[Chromatography Instrument]"] = (
        "Waters ACQUITY UPLC system"
    )
    idx = splitted_assay_df.columns.get_loc(
        "Parameter Value[Chromatography Instrument]"
    )
    splitted_assay_df.loc[:, splitted_assay_df.columns[idx + 1]] = "MTBLS"
    splitted_assay_df.loc[:, splitted_assay_df.columns[idx + 2]] = (
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000877"
    )

    splitted_assay_df.loc[:, "Parameter Value[Column model]"] = assay_method_params[
        method_name
    ]["column_model"]
    splitted_assay_df.loc[:, "Parameter Value[Column type]"] = assay_method_params[
        method_name
    ]["column_type"]

    # splitted_assay_df.loc[:, "Derived Spectral Data File"] = f"FILES/{sample_csv_column_name}/" + splitted_assay_df.loc[:, "Derived Spectral Data File"]
    splitted_assay_df = splitted_assay_df.merge(
        file_paths,
        right_on="File Name",
        left_on="Derived Spectral Data File",
        how="left",
        sort=False,
    )
    splitted_assay_df["Derived Spectral Data File"] = splitted_assay_df["File Path"]
    splitted_assay_df = splitted_assay_df.drop(columns=["File Name", "File Path"])
    prefix = (
        f"{study_id}_{sample_id}_{method_name}_{assay_method_params[method_name]['prefix']}"
        if len(sample_csv_map) > 1
        else f"{study_id}_{method_name}_{assay_method_params[method_name]['prefix']}"
    )
    file_name = f"a_{prefix}_metabolite_profiling_mass_spectrometry.txt"
    path = os.path.join(study_location, file_name)
    splitted_assay_df = sample_csv_map[sample_id][
        [sample_csv_column_name, "SAMPLE_NAME"]
    ].merge(
        splitted_assay_df,
        right_on="Sample Name",
        left_on=sample_csv_column_name,
        how="left",
        sort=False,
    )
    splitted_assay_df.loc[:, "Sample Name"] = splitted_assay_df.loc[:, "SAMPLE_NAME"]
    splitted_assay_df.loc[:, "MS Assay Name"] = splitted_assay_df.loc[:, "SAMPLE_NAME"]
    splitted_assay_df = splitted_assay_df.drop(
        columns=["SAMPLE_NAME", sample_csv_column_name]
    )
    maf_file_name_prefix = (
        f"m_{study_id}_{sample_id}" if len(sample_csv_map) > 1 else f"m_{study_id}"
    )
    maf_file_name = (
        f"{maf_file_name_prefix}_metabolite_profiling_mass_spectrometry_v2_maf.tsv"
    )
    splitted_assay_df.loc[:, "Metabolite Assignment File"] = maf_file_name

    write_tsv(splitted_assay_df, path)
    return file_name


def create_investigation_file(study_id, study_location, assay_file_names: List[str]):
    settings = get_study_settings()
    status, message = True, "Copied Metabolon template into study " + study_id
    invest_file = settings.investigation_file_name

    # Get the correct location of the Metabolon template study
    template_study_location = (
        get_settings().file_resources.study_partner_metabolon_template_path
    )
    template_study_location = os.path.join(template_study_location, invest_file)
    dest_file = os.path.join(study_location, invest_file)

    try:
        copy_file(template_study_location, dest_file)
    except:
        return False, "Could not copy Metabolon template into study " + study_id

    try:
        # Updating the ISA-Tab investigation file with the correct study id
        isa_study_input, isa_inv_input, std_path = iac.get_isa_study(
            study_id=study_id,
            api_key=None,
            skip_load_tables=True,
            study_location=str(study_location),
        )
        isa_study: Study = isa_study_input
        isa_inv: Investigation = isa_inv_input
        # Adding the study identifier
        isa_study.identifier = study_id
        isa_inv.identifier = study_id
        isa_study.filename = f"s_{study_id}.txt"
        source_references = {x.name: x for x in isa_inv.ontology_source_references}
        measurement_type = OntologyAnnotation(
            term="metabolite profiling",
            term_accession="http://purl.obolibrary.org/obo/OBI_0000366",
            term_source=source_references["OBI"],
        )
        technology_type = OntologyAnnotation(
            term="mass spectrometry assay",
            term_accession="http://purl.obolibrary.org/obo/OBI_0000470",
            term_source=source_references["OBI"],
        )
        technology_platform = "Q Exactive"
        # Set publication date to one year in the future
        plus_one_year = get_year_plus_one(isa_format=True)
        date_now = get_year_plus_one(todays_date=True, isa_format=True)

        isa_study.assays.clear()
        for sample_id in assay_file_names:
            for assay_file_name in assay_file_names[sample_id]:
                new_assay = Assay()
                new_assay.measurement_type = measurement_type
                new_assay.technology_type = technology_type
                new_assay.technology_platform = technology_platform
                new_assay.filename = assay_file_name
                isa_study.assays.append(new_assay)

        isa_inv.submission_date = date_now
        isa_study.submission_date = date_now

        try:
            result, _ = update_release_date(study_id, plus_one_year)
            if result:
                isa_inv.public_release_date = plus_one_year
                isa_study.public_release_date = plus_one_year
            # message = message + '. ' + api_version + '. ' + mzml2isa_version
        except Exception as e:
            logger.info(
                "Could not updated database study: " + study_id + ". Error: " + str(e)
            )

        # Updated the files with the study accession
        try:
            iac.write_isa_study(
                inv_obj=isa_inv,
                api_key=None,
                std_path=study_location,
                save_investigation_copy=False,
                save_samples_copy=False,
                save_assays_copy=False,
            )
        except Exception as e:
            logger.info("Could not write the study: " + study_id + ". Error: " + str(e))

    except Exception as e:
        return (
            False,
            "Could not update Metabolon template for study "
            + study_id
            + ". Error: "
            + str(e),
        )

    return status, message


# if __name__ == '__main__':
#     study_id = "MTBLS2307"
#     settings = get_settings()
#     study_root_path = pathlib.Path(settings.study.mounted_paths.study_metadata_files_root_path)
#     target_root_path = pathlib.Path(settings.study.mounted_paths.study_internal_files_root_path)
#     study_location = study_root_path / study_id
#     # target_location = target_root_path / study_id / "metabolon_pipeline"
#     target_location = study_root_path / study_id
#     # convert_to_isa(study_location=str(study_location), study_id=study_id)
#     # create_isa_files(study_id=study_id, study_location=str(study_location), target_location=target_location)
#     # target_location = target_root_path / study_id / "metabolon_pipeline/MZML_0015"
#     # status, message = to_isa_tab("", str(target_location), str(target_location))
#     status, report = validate_mzml_files(study_id)
#     print(status)
