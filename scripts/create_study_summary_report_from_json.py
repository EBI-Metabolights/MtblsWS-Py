import glob
import json
import os
import re


study_index_folder = '/nfs/www-prod/web_hx2/cm/metabolights/prod/studies/stage/private/INDEXED_ALL_STUDIES/test2'
json_files = glob.glob(os.path.join(study_index_folder, 'MTBLS*.json'))
def study_id_compare(key: str):
    if key:
        val = os.path.basename(key)
        val = val.replace("MTBLS", '')
        if val.isnumeric():
            return int(val)
    return 0
SEARCH_PATTERNS = {"raw_files": [r'^\d+~raw.+ data file(.\d+)?'], 
                    "derived_files": [r'^\d+~derived.+ data file(.\d+)?'], 
                    "supplementary_data_files": [r'^\d+~normalization.+ file(.\d+)?', r'^\d+~.+ decay data file(.\d+)?', r'^\d+~.+ parameter data file(.\d+)?'],
                    "metabolites_files": [r'^\d+~.+ assignment file(.\d+)?']}

json_files.sort(key=study_id_compare)
counter = 0
total = len(json_files)
report_path = "./_study_summary_report.tsv"

def update_extension_set(assay, file_indices, file_extensions):
    if not file_indices:
        return
    if "assayTable" in assay and assay["assayTable"] and "data" in assay["assayTable"] and assay["assayTable"]["data"]:
        for item in assay["assayTable"]["data"]:
            for index in file_indices:
                if index < len(item) and item[index]:
                    name_parts = os.path.basename(item[index]).lstrip(".").split('.')
                    extension = ""
                    if len(name_parts) > 2:
                        extension = f".{'.'.join(name_parts[-2:])}"
                    elif len(name_parts) > 1:
                        extension = f".{name_parts[-1]}"
                    else:
                        extension = "<without extension>"
                    file_extensions.add(extension)

with open(report_path, "w") as report:
    header = []
    header.append("id")
    header.append("studyIdentifier")
    header.append("country")
    header.append("studyStatus")
    header.append("submissionMonth")
    header.append("submissionYear")
    header.append("releaseMonth")
    header.append("releaseYear")
    header.append("organismNames")
    header.append("organismParts")
    header.append("descriptions")
    header.append("measurements")
    header.append("technologies")
    header.append("platforms")
    header.append("rawFileExtensions")
    header.append("derivedFileExtensions")
    row = '\t'.join(header)
    report.write(f"{row}\n")           
    for json_file in json_files:
        counter += 1
        accession_number = os.path.basename(json_file).replace('.json', '')
        actions = []
        with open(json_file, "r") as f:
        
                print(f'{counter:04}/{total:04} - {json_file}')
                json_data = json.load(f)
                report_item = []
                report_item.append(str(json_data["id"]))
                report_item.append(str(json_data["studyIdentifier"]))
                report_item.append(str(json_data["studyStatus"]))
                report_item.append(str(json_data["derivedData"]["country"]) if json_data["derivedData"] else "")
                report_item.append(str(json_data["derivedData"]['submissionMonth']) if json_data["derivedData"] else "")
                report_item.append(str(json_data["derivedData"]['submissionYear']) if json_data["derivedData"] else "")
                report_item.append(str(json_data["derivedData"]['releaseMonth']) if json_data["derivedData"] else "")
                report_item.append(str(json_data["derivedData"]['releaseYear']) if json_data["derivedData"] else "")
                report_item.append(str(json_data["derivedData"]['organismNames']) if json_data["derivedData"] else "")
                report_item.append(str(json_data["derivedData"]['organismParts']) if json_data["derivedData"] else "")
                
                descriptions = set(item["description"] for item in json_data["descriptors"] if item and item["description"])
                measurements = set(item["measurement"] for item in json_data["assays"] if item and item["measurement"])
                technologies = set(item["technology"] for item in json_data["assays"] if item and item["technology"])
                platforms = set(item["platform"] for item in json_data["assays"] if item and item["platform"])
                
                
                
                report_item.append("::".join(descriptions))
                report_item.append("::".join(measurements))
                report_item.append("::".join(technologies))
                report_item.append("::".join(platforms))
                
                raw_file_extensions= set()
                derived_file_extensions = set()
                for assay in json_data["assays"]:
                    raw_file_indices = []
                    derived_file_indices = []
                    
                    if "assayTable" in assay and assay["assayTable"] and "fields" in assay["assayTable"] and assay["assayTable"]["fields"]:
                        for key in assay["assayTable"]["fields"]:
                            for pattern in SEARCH_PATTERNS["raw_files"]:
                                if re.match(pattern, key):
                                    field_match = assay["assayTable"]["fields"][key]
                                    if "index" in field_match and field_match["index"]:
                                        raw_file_indices.append(int(field_match["index"]))
                            for pattern in SEARCH_PATTERNS["derived_files"]:        
                                if re.match(pattern, key):
                                    field_match = assay["assayTable"]["fields"][key]
                                    if "index" in field_match and field_match["index"]:
                                        derived_file_indices.append(int(field_match["index"]))

                    update_extension_set(assay, raw_file_indices, raw_file_extensions)
                    update_extension_set(assay, derived_file_indices, derived_file_extensions)
                
                
                report_item.append("::".join(raw_file_extensions))
                report_item.append("::".join(derived_file_extensions))
                row = '\t'.join(report_item)
                report.write(f"{row}\n")
        
        pass