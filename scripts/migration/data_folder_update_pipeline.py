import datetime
import glob
import hashlib
import json
import os
import pathlib
import re
from typing import Callable, Dict, List, OrderedDict, Set, Tuple
import pandas as pd

from pydantic import BaseModel
from unidecode import unidecode

from app.config import get_settings
from app.study_folder_utils import FileDescriptor, get_all_study_metadata_and_data_files
from app.tasks.utils import UTC_SIMPLE_DATE_FORMAT
from app.ws.db.schemes import Study
from app.ws.db.types import StudyStatus
from app.ws.folder_maintenance import StudyFolderMaintenanceTask
from app.ws.redis.redis import get_redis_server
from app.ws.study.study_service import StudyService
from scripts.migration.utils import get_studies
from metabolights.isatab import Reader
from metabolights.isatab.reader import InvestigationFileReader, IsaTableFileReader, InvestigationFileReaderResult, IsaTableFileReaderResult
from metabolights.models.parser.enums import ParserMessageType
from metabolights.models.parser.common import ParserMessage
from metabolights.models.isa.investigation_file import Study, Assay
from metabolights.models.isa.common import IsaTableColumn

class FileDetail(BaseModel):
    file_path: str = ""
    size: str = ""
    size_in_bytes: int = 0
    modified_utc: str = ""
    modified_timestamp: float = 0
    previous_file_path: str = ""
    sha256: str = ""

class FileCollection(BaseModel):
    metadata_files: Dict[str, List[FileDetail]] = {}
    data_files: Dict[str, List[FileDetail]] = {}
    supplementary_files: Dict[str, List[FileDetail]] = {}
    
class StudyFolder(BaseModel):
    study_id: str = ""
    update: str = ""
    metadata_files: Dict[str, List[FileDetail]] = {}
    data_files: Dict[str, List[FileDetail]] = {}
    unreferenced_files: FileCollection = FileCollection()
    
class Action(BaseModel):
    action_name: str = ""
    action_input: str = ""
    action_output: str = ""
    description: str = ""
    
class FileState(BaseModel):
    key: str = ""
    value: str = ""
    def __hash__(self) -> int:
        return hash(f"{self.key}:{self.value}")

    
def load_sha_file(filepath: str) -> Dict[str, str]:
    sha_dict :Dict[str, str] = {}
    with open(filepath, "r") as f:
        while True:    
            line = f.readline()
            if not line:
                break
            val = line.split("\t")
            sha_dict[val[0]] = val[2].strip()
    return sha_dict
        # return pd.read_csv(f, sep="\t", header=None, index_col=0)
sha256_dict: Dict[str, str] = load_sha_file("sha_256_values.tsv")

KB_FACTOR = 1024.0 ** 1
MB_FACTOR = 1024.0 ** 2
GB_FACTOR = 1024.0 ** 3

def get_file_size_string(size: float):
    if size > GB_FACTOR:
        size_in_gb = size / GB_FACTOR
        return "%.2f" % round(size_in_gb, 2) + "GiB"
    elif size > MB_FACTOR:
        size_in_mb = size / MB_FACTOR
        return "%.2f" % round(size_in_mb, 2) + "MiB"
    else:
        size_in_mb = size / KB_FACTOR
        return "%.2f" % round(size_in_mb, 2) + "KiB"

def sha256sum(filepath: str):
    if not filepath or not os.path.exists(filepath):
        return hashlib.sha256("".encode()).hexdigest()

    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()
    
class DataFolderUpdatePipeline():

    split_main_bucket_indices = {"_NEG": 1, "_POS": 2, "_ALT": 3}

    
    acqu_file_column_name = "Acquisition Parameter Data File"
    fid_file_column_name = "Free Induction Decay Data File"
    raw_file_column_name = "Raw Spectral Data File"
    derived_file_column_name = "Derived Spectral Data File"
    maf_file_column_name = "Metabolite Assignment File"     
    


    def __init__(self, study_id: str, files: Dict[str, FileDescriptor], sha256_dict: Dict[str, str]=None, summary_output_folder: str="data_folder_updates") -> None:
        filtered_map = {x:files[x]  for x in files if not files[x].is_dir or files[x].is_stop_folder or files[x].is_empty}
        self._all_files = files
        self.file_descriptors: Dict[str, FileDescriptor] = {}
        for item in filtered_map:
            parent = os.path.dirname(item)
            if parent in filtered_map and filtered_map[parent].is_stop_folder:
                continue
            self.file_descriptors[item] = filtered_map[item]
        self.new_folder_content: Dict[str, str]= { x: x for x in self.file_descriptors}
        self.actions: Dict[str, List[Action]]= { x: [] for x in self.file_descriptors}

        self.study_id = study_id
        self.study_folder: StudyFolder = StudyFolder(study_id=study_id)
        self.current_file_details: Dict[str, FileDetail] = {}
        self.deleted_paths: Dict[str, List[Action]] = {}
        self.new_zip_file_previous_file_map: Dict[str, str] = { x: x for x in self.file_descriptors}     
        self.files_converted_to_zip: Dict[str, str] = { x: x for x in self.file_descriptors}
        self.files_uncompressed: Dict[str, str] = {}
        self.summary_output_folder = summary_output_folder
        self.sha256_dict = sha256_dict
        self.data_file_assay_map: Dict[str, List[str]] = {}
        self.maf_file_assay_map: Dict[str, List[str]] = {}

    def print_actions(self, global_file_fd=None):
        rows = []
        for file in self.actions:
            count = 0
            padding = len(str(len(self.actions[file])))
            for action in self.actions[file]:
                count += 1
                rows.append(f"{self.study_id}\t{file}\t{str(count).zfill(padding)}\t{action.action_name}\t{action.action_input}\t{action.action_output}\t{action.description}\n")

        rows_deleted_actions = []
        for file in self.deleted_paths:
            rows_deleted_actions.append(f"{self.study_id}\t{file}\t{'1'}\t{self.deleted_paths[file][-1].action_name}\t\t\t\n")
            
        rows.extend(rows_deleted_actions)
        rows.sort(key=lambda x: x.lower())
        content = "".join(rows)
        action_file_path = f"{self.summary_output_folder}/{self.study_id}_actions.tsv"
        
        with open(action_file_path, "w") as f:
            f.write(f"STUDY_ID\tFILE_PATH\tORDER\tACTION\tINPUT\tOUTPUT\tDESCRIPTION\n")
            f.write(content)
            
            if global_file_fd:
                if content:
                    global_file_fd.write(content)
                    global_file_fd.flush()

                
    def print_mapping(self, global_file_fd=None):
        if len(self.new_folder_content) > 0:
            with open(f"{self.summary_output_folder}/{self.study_id}_mapping.tsv", "w") as f:
                f.write(f"STUDY_ID\tLAST_FILE_PATH\tCURRENT_FILE_PATH\tACTION_REQUIRED\tIS_DIRECTORY\tRAW_DATA_FOLDER\n")
                rows = []
                for file in self.new_folder_content:
                    action_required = not self.new_folder_content[file] == file
                    is_dir = self.file_descriptors[file].is_dir
                    is_stop_folder = self.file_descriptors[file].is_stop_folder
                    file_to_print = file.replace("\r","__<RETURN CHAR>__")
                    file_to_print = file_to_print.replace("\n","__<NEW_LINE CHAR>__")
                    file_to_print = file_to_print.replace("\t","__<TAB CHAR>__")
                    rows.append(f"{self.study_id}\t{self.new_folder_content[file]}\t{file}\t{action_required}\t{is_dir}\t{is_stop_folder}\n")
                rows.sort(key=lambda x: x.lower())
                content = "".join(rows)
                f.write(content)
                if global_file_fd and content:
                    global_file_fd.write(content)
                    global_file_fd.flush()
                
    def print_data_folder_summary(self, missing_sha_f):
        missing_sha_list = []
        content = self.study_folder.model_dump()

        with open(f"{self.summary_output_folder}/{self.study_id}_content.json", "w") as f:
            json.dump(content, f, indent=4)
        with open(f"{self.summary_output_folder}/{self.study_id}_content.tsv", "w") as f:
            f.write(f"STUDY_ID\tCATEGORY\tREFERENCED\tFILE_PATH\tSIZE\tMODIFIED_TIME[UTC]\tSHA256\tMODIFIED_TIMESTAMP\tSIZE[BYTE]\tPREVIOUS_FILE_PATH\n")
            rows = []
            deleted_file_map = {}
            for file in self.deleted_paths:
                zip_file = self.deleted_paths[file][-1].action_input
                if zip_file not in deleted_file_map:
                    deleted_file_map[zip_file] = []
                deleted_file_map[zip_file].append(file)
            metadata_category_names = ["ISA Investigation File", "ISA Sample File", "ISA Assay File", self.maf_file_column_name]
            for metadata_files, referenced in [(self.study_folder.metadata_files, True), 
                                               (self.study_folder.unreferenced_files.metadata_files, False)]:
                for category in metadata_category_names:
                    self.write_category(metadata_files,
                                        category_name=category, referenced=referenced, 
                                        deleted_file_map=deleted_file_map, rows=rows)
                
                
            data_column_names = [self.raw_file_column_name, self.acqu_file_column_name, self.fid_file_column_name, self.derived_file_column_name]

            for data_files, referenced in [(self.study_folder.data_files, True), 
                                               (self.study_folder.unreferenced_files.data_files, False)]:    
                for column_name in data_column_names:
                    self.write_category(data_files,
                                        category_name=column_name, referenced=True, 
                                        deleted_file_map=deleted_file_map, rows=rows)
                    
            for metadata_files in [self.study_folder.metadata_files,
                                               self.study_folder.unreferenced_files.metadata_files]:
                self.write_category(metadata_files,
                                    category_name="Supplementary File", referenced=False, 
                                    deleted_file_map=deleted_file_map, rows=rows)
                
            self.write_category(self.study_folder.unreferenced_files.supplementary_files,
                                category_name="Supplementary File", referenced=False, 
                                deleted_file_map=deleted_file_map, rows=rows)
            
            content = "".join(rows)
            f.write(content)
            if missing_sha_list:
                missing_sha_f.writelines(missing_sha_list)
                missing_sha_f.flush()

    def write_category(self, category_map: str, category_name: str, referenced: bool, 
                       deleted_file_map: Dict[str, List[Action]], rows: List[str]):
        if category_name in category_map:
            files = category_map[category_name]
            files.sort(key=lambda x: x.file_path)
            for file in files:
                self.write_content_row(file_detail=file, category=category_name, referenced=referenced, 
                                        deleted_file_map=deleted_file_map, rows=rows)
            
    def write_content_row(self, file_detail: FileDetail, category: str, referenced: bool, deleted_file_map: Dict[str, List[Action]], rows: List[str]):
        key = file_detail.previous_file_path if file_detail.previous_file_path else file_detail.file_path
        previous_file_to_print = ""
        if file_detail.previous_file_path:
            previous_file_to_print = re.sub(r"[\x00-\x1F]", "<__CTRL__>", file_detail.previous_file_path)
        new_key = key
        if key in self.files_converted_to_zip:
            new_key = self.files_converted_to_zip[key]
        if key in self.files_uncompressed:
            new_key = self.files_uncompressed[key]
        detail = self.current_file_details[new_key]
        size_str = detail.size
        size_in_bytes = detail.size_in_bytes
        modified = detail.modified_utc
        modified_timestamp = detail.modified_timestamp
        hash_value = detail.sha256
        file_name = detail.file_path
        rows.append(f"{self.study_id}\t{category}\t{referenced}\t{file_name}\t{size_str}\t{modified}\t{hash_value}\t{modified_timestamp}\t{size_in_bytes}\t{previous_file_to_print}\n")
                
    def print_duplicated_files(self):
        unique_basenames = {}
        files = self.file_descriptors
        for file in files:
            relative_path = files[file].relative_path
            basename = os.path.basename(relative_path)
            if not files[file].is_dir or files[file].is_stop_folder:
                if basename not in unique_basenames:
                    unique_basenames[basename] = []
                unique_basenames[basename].append(file)
        non_unique_files = {x: unique_basenames[x] for x in unique_basenames if len(unique_basenames[x]) > 1}
        counter = 0
        if len(non_unique_files) > 0:
            results = []
            with open(f"{self.summary_output_folder}/{self.study_id}_duplicate_files.tsv", "w") as f:
                results.append(f"STUDY_ID\tNO\tREASON\tFILE\tMODIFIED_TIME\tFILE_SIZE\n")
                for item in non_unique_files:
                    filesize_check_map = {}
                    for non_unique_item in non_unique_files[item]:
                        file_size = files[non_unique_item].file_size
                        if file_size not in filesize_check_map:
                            filesize_check_map[file_size] = []
                        filesize_check_map[file_size].append(non_unique_item)
                    for key in filesize_check_map:
                        if len(filesize_check_map[key]) > 1:
                            counter += 1
                            results = []
                            for item in filesize_check_map[key]:
                                modified = datetime.datetime.fromtimestamp(files[item].modified_time, tz=datetime.timezone.utc).strftime('%Y-%m-%d-%H:%M')
                                results.append(f"{self.study_id}\t\SAME_SIZE\t{counter:03}\t{item}\t{modified}\t{files[item].file_size}\n")
                            results.sort()
                            f.write("".join(results)) 
        
            
    def update_state(self, updates: Dict[str, str], task_name: str):
        sorted_keys = [x for x in updates]
        sorted_keys.sort(key=lambda x: x.lower() if x else "")
        for key in sorted_keys:
            action = Action(action_name=task_name, action_input=self.new_folder_content[key], action_output=updates[key])
            if updates[key]:
                if key not in self.actions:
                    self.actions[key] = []
                self.actions[key].append(action)
                self.new_folder_content[key] = updates[key]
            else:
                self.deleted_paths[key] = self.actions[key] if self.actions[key] else []
                self.deleted_paths[key].append(action)
                del self.actions[key]
                del self.new_folder_content[key]
        
        return self
    
    def get_error_message(self, message: ParserMessage) -> str:
        if message.type in (ParserMessageType.ERROR, ParserMessageType.CRITICAL):
            return True
        return False
    
    def load_metadata_file_references(self, study_metadata_path: str):
        i_file_name = "i_Investigation.txt"
        s_file_name: str = ""
        a_file_names: Set[str] = set()
        
        derived_file_names: Set[str] = set()
        raw_file_names: Set[str] = set()
        fid_file_names: Set[str] = set()
        acqu_file_names: Set[str] = set()
        m_file_names: Set[str] = set()  
        
        raw_derived_file_map: Dict[str, Set[str]] = {
            self.fid_file_column_name: fid_file_names,
            self.raw_file_column_name: raw_file_names,
            self.derived_file_column_name: derived_file_names,
            self.acqu_file_column_name: acqu_file_names
        }
        
        data_map: Dict[str, Set[str]] = {
            self.maf_file_column_name: m_file_names
        }
        data_map.update(raw_derived_file_map)
        
        i_file_path = pathlib.Path(os.path.join(study_metadata_path, i_file_name))

        i_reader: InvestigationFileReader = Reader.get_investigation_file_reader()
        result: InvestigationFileReaderResult = i_reader.read(i_file_path)
        error_list = [str(x) for x in result.parser_report.messages if self.get_error_message(x)]
        
        if i_file_name not in self.actions:
            self.actions[i_file_name] = []
        if error_list:
            errors = '; '.join(error_list)
            self.actions[i_file_name].append(Action(action_name="FIX", description=f"Fix parse errors in i_Investigation.txt. {errors}"))
        else:
            studies = result.investigation.studies
            if not studies or not studies[0] or not studies[0].file_name or len(studies) > 1:
                self.actions[i_file_name].append(Action(action_name="FIX", description=f"Define one study in i_Investigation.txt file."))
            else:
                study: Study = result.investigation.studies[0]
                if not study.file_name:
                    self.actions[i_file_name].append(Action(action_name="FIX", description=f"Define a sample file in i_Investigation.txt file."))
                else:
                    s_file_name = study.file_name
                    if s_file_name not in self.new_folder_content:
                        self.actions[s_file_name] = []
                        self.actions[i_file_name].append(Action(action_name="FIX", description=f"{s_file_name} does not exist."))
                    elif s_file_name != f"s_{self.study_id}.txt":
                        self.actions[s_file_name].append(Action(action_name="RENAME", action_input=s_file_name, action_output=f"s_{self.study_id}.txt"))
                        self.actions[i_file_name].append(Action(action_name="UPDATE_CONTENT", action_input=s_file_name, action_output=f"s_{self.study_id}.txt"))
                    
                    if study.study_assays.assays:
                        for assay in study.study_assays.assays:
                            if assay.file_name in a_file_names:
                                self.actions[i_file_name].append(Action(action_name="FIX", description=f"Multiple assay name {assay.file_name} in i_Investigation.txt file."))
                            if not assay.file_name or not assay.file_name.replace('"', "").replace("'", "").strip():
                                self.actions[i_file_name].append(Action(action_name="FIX", description=f"There is an empty assay file name in i_Investigation.txt file."))
                            else:
                                a_file_names.add(assay.file_name)
        a_reader = Reader.get_assay_file_reader()
        nonexist_files = set()
        for a_file in a_file_names:
            if a_file not in self.new_folder_content:
                self.actions[a_file] = []
                self.actions[a_file].append(Action(action_name="FIX", description=f"Assay file {a_file} defined in i_Investigation.txt does not exist."))
                continue
            if not a_file.startswith(f"a_{self.study_id}"):
                new_filename  = self.sanitise_metadata_filename(self.study_id, a_file)
                self.actions[a_file].append(Action(action_name="RENAME", action_input=a_file, action_output=new_filename))
                self.actions[i_file_name].append(Action(action_name="UPDATE_CONTENT", action_input=a_file, action_output=new_filename, description="Update assay file name"))
                                
            a_file_path = os.path.join(study_metadata_path, a_file)
            result: IsaTableFileReaderResult = a_reader.get_headers(a_file_path)
            error_list = [str(x) for x in result.parser_report.messages if self.get_error_message(x)]
            if error_list:
                errors = '; '.join(error_list)
                self.actions[a_file].append(Action(action_name="FIX", description=f"Fix parse errors in assay file {a_file}. {errors}"))
                continue
            
            columns: List[str] = result.isa_table_file.table.columns
            data_file_column_names = [x for x in columns if " Data File" in x]
            
            maf_file_columns = [x for x in columns if self.maf_file_column_name in x]
            if len(maf_file_columns) != 1 or (maf_file_columns and not maf_file_columns[0]):
                self.actions[a_file].append(Action(action_name="FIX", description=f"Invalid number of {self.maf_file_column_name} in assay file {a_file}"))
                continue
            maf_column_name = maf_file_columns[0]
            column_names = data_file_column_names.copy()
            column_names.append(maf_column_name)
            
            file_result = a_reader.get_rows(a_file_path, limit=None, selected_columns=column_names)
            table = file_result.isa_table_file.table
            for column in table.columns:
                selected_set: Set[str] = None
                for item in data_map:
                    if column.startswith(item):
                        selected_set = data_map[item]
                        break
                for file in set(table.data[column]):
                    if not file or file in nonexist_files:
                        continue
                    if (file not in self.new_folder_content and file not in self.files_converted_to_zip) and file not in nonexist_files:
                        base, ext = os.path.splitext(file)
                        found = False
                        if ext in self.non_standard_compressed_file_extensions:
                            if base  in self.new_folder_content or base in self.files_converted_to_zip:
                                found = True
                            else:
                                base2, ext2 = os.path.splitext(base)
                                if f"{ext2}{ext}" in self.non_standard_compressed_file_double_extensions and (base2  in self.new_folder_content or base2 in self.files_converted_to_zip): 
                                    found = True
                        if not found:
                            self.actions[a_file].append(Action(action_name="FIX", action_input=file, action_output=file, description=f"Referenced file '{file}' does not exist on study folder."))
                            nonexist_files.add(file)
                            continue
                    if file not in self.data_file_assay_map:
                        self.data_file_assay_map[file] = set()
                    self.data_file_assay_map[file].add(a_file)
                    
                    if column == maf_column_name:
                        if file not in self.maf_file_assay_map:
                            self.maf_file_assay_map[file] = set()
                        self.maf_file_assay_map[file].add(a_file)
                                         
                        
                selected_set.update(table.data[column])
                # selected_set.difference_update(nonexist_files)
        for item in data_map:
            data_map[item].discard(None)
            data_map[item].discard("")
            data_map[item].difference_update(nonexist_files)
            

        referenced_files: Set[str] = set()
        referenced_files.add(i_file_name)
        referenced_files.add(s_file_name)
        
        self.append_file_detail(self.study_folder.metadata_files, "ISA Investigation File", i_file_name)
        self.append_file_detail(self.study_folder.metadata_files, "ISA Sample File", s_file_name)
        for a_file in a_file_names:
            self.append_file_detail(self.study_folder.metadata_files, "ISA Assay File", a_file)
            referenced_files.add(a_file)
        for m_file in m_file_names:
            self.append_file_detail(self.study_folder.metadata_files, maf_column_name, m_file)
            referenced_files.add(m_file)

        raw_derived_file_map[self.derived_file_column_name].difference_update(raw_derived_file_map[self.raw_file_column_name])
        raw_derived_file_map[self.fid_file_column_name].difference_update(raw_derived_file_map[self.acqu_file_column_name])
        for category in raw_derived_file_map:
            for file in raw_derived_file_map[category]: 
                self.append_file_detail(self.study_folder.data_files, category, file)
                referenced_files.add(file)
                
        for item in self.new_folder_content:
            if item in referenced_files or (self.new_zip_file_previous_file_map[item] in referenced_files):
                continue
            file_category = self.classify_file(self.new_folder_content[item])
            if file_category.startswith("ISA ") or file_category == self.maf_file_column_name:
                self.append_file_detail(self.study_folder.unreferenced_files.metadata_files, file_category, item)
            elif " Data File" in file_category:
                self.append_file_detail(self.study_folder.unreferenced_files.data_files, file_category, item)
            else:
                self.append_file_detail(self.study_folder.unreferenced_files.supplementary_files, file_category, item )

        for m_file in self.maf_file_assay_map:
            if not m_file.startswith(f"m_{self.study_id}"):
                new_filename  = self.sanitise_metadata_filename(self.study_id, m_file, prefix="m_")
                self.actions[m_file].append(Action(action_name="RENAME", action_input=m_file, action_output=new_filename))
                for a_file in self.maf_file_assay_map[m_file]:
                    self.actions[a_file].append(Action(action_name="UPDATE_CONTENT", action_input=m_file, action_output=new_filename, description="Update MAF file name"))
    
        for assay_map in (self.data_file_assay_map, self.maf_file_assay_map):
            for file in assay_map:
                value = file
                if file in self.new_folder_content:
                    value = self.new_folder_content[file]
                elif file in self.files_converted_to_zip:
                    key = self.files_converted_to_zip[file]
                    value = self.new_folder_content[key]
                         
                if value != file:
                    for assay in self.data_file_assay_map[file]:
                        self.actions[assay].append(Action(action_name="UPDATE_CONTENT", action_input=file, action_output=value))          
            
    def append_file_detail(self, collection: Dict[str, List[FileDetail]], category_name: str, file: str):
        selected_file = file
        uncompressed_file = ""
        if file in self.files_converted_to_zip:
            selected_file = self.files_converted_to_zip[file]
        else:
            base, ext = os.path.splitext(file)
            if ext in self.non_standard_compressed_file_extensions:
                if base  in self.new_folder_content or base in self.files_converted_to_zip:
                    uncompressed_file = base
                else:
                    base2, ext2 = os.path.splitext(base)
                    if f"{ext2}{ext}" in self.non_standard_compressed_file_double_extensions and (base2  in self.new_folder_content or base2 in self.files_converted_to_zip): 
                        uncompressed_file = base2
            if uncompressed_file and uncompressed_file in self.file_descriptors and not self.file_descriptors[uncompressed_file].is_dir:
                 selected_file = uncompressed_file
                 self.files_uncompressed[file] = uncompressed_file
                #  self.deleted_paths[file] =[]
                #  self.deleted_paths[file].append(Action(action_name="UNCOMPRESS", action_input=file, action_output=uncompressed_file))
                 
                 
        
        if selected_file in self.file_descriptors and self.file_descriptors[selected_file]:
            file_desc = self.file_descriptors[selected_file]
            modified = datetime.datetime.fromtimestamp(file_desc.modified_time, tz=datetime.timezone.utc).strftime(UTC_SIMPLE_DATE_FORMAT)
            modified_timestamp = str(file_desc.modified_time)
            size = file_desc.file_size
            size_str = get_file_size_string(size)
            hash_value = ""
            index_key = f"{self.study_id}/{selected_file}"
            if index_key in self.sha256_dict:
                hash_value = self.sha256_dict[index_key]
            previous = ""

            if selected_file in self.new_zip_file_previous_file_map and self.new_zip_file_previous_file_map[selected_file] != selected_file:
                previous = self.new_zip_file_previous_file_map[selected_file]
            elif file != self.new_folder_content[selected_file] or uncompressed_file:
                previous = file

            detail = FileDetail(file_path=self.new_folder_content[selected_file], size=size_str, size_in_bytes=size, sha256=hash_value, 
                    modified_utc=modified, modified_timestamp=modified_timestamp, previous_file_path=previous)
            
            if category_name not in collection:
                collection[category_name] = []
            collection[category_name].append(detail)
            self.current_file_details[selected_file] = detail
        else:
            if file in self.data_file_assay_map:
                for assay in self.data_file_assay_map[file]:
                    self.actions[assay].append(Action(action_name="FIX", description=f"Data file '{file}' referenced in '{assay}' does not exist."))

    def calculate_metadata_sha256(self, study_matadata_path: str):
        for file in self.current_file_details:
            detail = self.current_file_details[file]
            if not file.startswith("FILES/") and len(file) >= 6 and file[:2] in {"m_", "s_", "a_", "i_"} and file[-4:] in {".txt", ".tsv", ".csv"}:
                filepath = os.path.join(study_matadata_path, file)
                hash_value = sha256sum(filepath)
                detail.sha256 = hash_value
        
    def classify_file(self, file_name) -> str:
        basename = os.path.basename(file_name)
        base, ext = os.path.splitext(basename)
        ext_lower = ext.lower()
        if ext_lower in {".txt", ".csv", ".tsv", ".xlsx", ".xls"}:
            category = ""
            if base.startswith("m_"):
                category = self.maf_file_column_name
            elif base.startswith("a_"):
                category = "ISA Assay File"
            elif base.startswith("i_"):
                category = "ISA Investigation File"
            elif base.startswith("s_"):
                category = "ISA Sample File"
            return category if category else "Supplementary File"
        elif ext_lower in self.raw_files_extensions:
            return "Raw Spectral Data File"
        elif ext_lower in self.derived_files_extensions:
            return "Derived Spectral Data File"
        elif ext_lower == ".zip" or ext_lower in self.non_standard_compressed_file_extensions:
            base2, ext2 = os.path.splitext(base)
            double_ext = f"{ext2}{ext}".lower()
            non_compressed_file = base
            if double_ext in self.non_standard_compressed_file_double_extensions:
                non_compressed_file = base2
            return self.classify_file(non_compressed_file)
        else:
            if ("fid" in file_name or "ser" in file_name) and "_" not in file_name and  "." not in file_name:
                return "Free Induction Decay Data File"
            elif ("acqu" in file_name) and "_" not in file_name and  "." not in file_name:
                return "Acquisition Parameter Data File"
            return "Supplementary File"
                      
    def get_files(self, search_path, patterns: List[str], recursive: bool = False):
        files = []
        if not os.path.exists(search_path):
            return files
        for pattern in patterns:
            files.extend(glob.glob(os.path.join(search_path, pattern), recursive=recursive))
        files.sort()
        return files
        
    def run_pipeline_task(self, task: Callable, task_name: str, update_all_outputs: bool=False):
        input_map: Dict[str, str] = self.new_folder_content.copy()
        output_map: Dict[str, str] = self.new_folder_content.copy()
        task(output_map)
        updates = output_map
        if not update_all_outputs:
            updates = {x:output_map[x] for x in output_map if output_map[x] != input_map[x]}
        if len(updates) > 0:
            self.update_state(updates, task_name)
        return self

    def recompress_files(self):
        def recompress_files_task(input_map: Dict[str, str]):
            keys = list(input_map.keys())
            for key in keys:
                for ext in self.non_standard_compressed_file_extensions:
                    if input_map[key].endswith(ext):
                        zip_file = input_map[key].replace(ext, ".zip")
                        input_map[key] = zip_file
                        break
            return input_map
    
        return self.run_pipeline_task(recompress_files_task, "RECOMPRESS")

    def compress_folders(self):
        def compress_folders_task(input_map: Dict[str, str]):
            keys = list(input_map.keys())
            for key in keys:
                if not input_map[key].endswith(".zip"):
                    if self.file_descriptors[key].is_dir and self.file_descriptors[key].is_stop_folder:
                        input_map[key] = f"{input_map[key]}.zip"
            return input_map
        
        return self.run_pipeline_task(compress_folders_task, "COMPRESS")
    
    def remove_empty_folders(self):
        def remove_empty_folders_task(input_map: Dict[str, str]):
            keys = list(input_map.keys())
            for key in keys:
                if self.file_descriptors[key].is_dir and self.file_descriptors[key].is_empty:
                        input_map[key] = f""
            return input_map
        
        return self.run_pipeline_task(remove_empty_folders_task, "REMOVE_EMPTY_FOLDER")    


    def remove_hidden_files(self):
        def remove_hidden_files_task(input_map: Dict[str, str]):
            keys = list(input_map.keys())
            for key in keys:
                if os.path.basename(input_map[key]).startswith(".") or "/." in input_map[key]:
                        input_map[key] = f""
            return input_map

        return self.run_pipeline_task(remove_hidden_files_task, "REMOVE_HIDDEN_FILE")    
    
    def sanitise_filenames(self):
        def sanitise_task(input_map: Dict[str, str]): 
            keys = list(input_map.keys())
            new_values: Dict[str, int] = {}
            for key in keys:
                basename = os.path.basename(input_map[key])
                dirname = os.path.dirname(input_map[key])
                new_basename = self.sanitise_filename(basename)
                new_basename = new_basename[:250] if len(new_basename) > 250 else new_basename
                new_path = os.path.join(dirname, new_basename)
                if new_path not in new_values:
                    new_values[new_path] = 1
                else:
                    new_values[new_path] += 1
                if new_values[new_path] <= 1:
                    input_map[key] = new_path
                else:
                    input_map[key] = os.path.join(dirname, f"{new_values[new_path]}_{new_basename}")
                    
            return input_map
        return self.run_pipeline_task(sanitise_task, "SANITISE_FILE")

    def sanitise_paths(self):
        def sanitise_paths_task(input_map: Dict[str, str]): 
            keys = list(input_map.keys())
            for key in keys:
                basename = os.path.basename(input_map[key])
                dirname = os.path.dirname(input_map[key])
                new_dirname = self.sanitise_filename(dirname)
                subpaths = new_dirname.split("/")
                new_dirname = "/".join([x[:250] if len(x) > 250 else x for x in subpaths])
                if new_dirname != dirname:
                    input_map[key] = os.path.join(new_dirname, basename)
            return input_map
        return self.run_pipeline_task(sanitise_paths_task, "SANITISE_PATH")
    
    def make_unique_filenames(self):
        def make_filename_unique(input_map: Dict[str, str]):
            counts = {}
            keys = list(input_map.keys())
            for key in keys:
                basename = os.path.basename(input_map[key])
                if self.file_descriptors[key].is_dir and not self.file_descriptors[key].is_stop_folder:
                    continue
                if basename not in counts:
                    counts[basename] = 0
                else:
                    counts[basename] +=  1
                    new_filename = f"{str(counts[basename])}_{str(basename)}"
                    dir_name = os.path.dirname(input_map[key])
                    input_map[key] = os.path.join(dir_name, new_filename)

            return input_map
        
        return self.run_pipeline_task(make_filename_unique, "MAKE_UNIQUE_FILENAME")            

    def split_folders(self):
        return self.run_pipeline_task(self.split_folders_task, "SPLIT_FOLDER")
    
    def remove_compressed_files(self):
        def remove_compressed_files_task(input_map: Dict[str, str]):
            value_key_mapping = {}
            for key, value in input_map.items():
                if value not in value_key_mapping:
                    value_key_mapping[value] = []
                value_key_mapping[value].append(key)
                
            keys = list(input_map.keys())
            for key in keys:
                if not key.endswith(".zip"):
                    is_archive_file = False
                    zip_file = ""
                    if f"{key}.zip" in input_map:
                        zip_file = f"{key}.zip"
                    else:
                        base, ext = os.path.splitext(key)
                        if ext in self.non_standard_compressed_file_extensions:
                            is_archive_file = True
                            if f"{base}.zip" in input_map:
                                zip_file = f"{base}.zip"
                            else:
                                base2, _ = os.path.splitext(base)
                                if f"{base2}.zip" in input_map:
                                    zip_file = f"{base2}.zip"
                    if zip_file:
                        if self.file_descriptors[key].is_dir or is_archive_file:
                            input_map[key] = ""
                            self.files_converted_to_zip[key] = zip_file
                            self.new_zip_file_previous_file_map[zip_file] = key
                        else:
                            input_map[zip_file] = ""
                            self.files_converted_to_zip[zip_file] = key
                            self.new_zip_file_previous_file_map[key] = zip_file

            return input_map    
        return self.run_pipeline_task(remove_compressed_files_task, "REMOVE_COMPRESSED_FILE")

    def check_missing_sha256(self):
        def check_missing_sha256_task(input_map: Dict[str, str]): 
            keys = list(input_map.keys())
            for key in keys:
                hash_exist = False
                if f"{self.study_id}/{key}" in self.sha256_dict:
                    hash_exist = True
                file_updated = False
                if key in self.actions:
                    for action in self.actions[key]:
                        if action.action_name == "COMPRESS" or action.action_name == "RECOMPRESS" or action.action_name == "UPDATE_CONTENT":
                            file_updated = True
                            break
                if not file_updated:
                    if hash_exist or (key in self.current_file_details and self.current_file_details[key].sha256):
                        del input_map[key]
                    
            return input_map
        return self.run_pipeline_task(check_missing_sha256_task, "CALCULATE_SHA256", update_all_outputs=True)
    
    def sanitise_filename(self, filename: str):
        if not filename or not filename.strip():
            return ""
        filename = unidecode(filename.strip())
        filename = filename.replace("+", "_PLUS_")
        filename = re.sub("[^/a-zA-Z0-9_.-]", "_", filename)       
        return filename
    
    def sanitise_metadata_filename(self, study_id: str, filename:str, prefix: str = "a_"):
        new_name = self.sanitise_filename(filename)
        if study_id.lower() in new_name:
            new_name = new_name.replace(study_id.lower(), study_id)
        if not new_name.startswith(f"{prefix}{study_id}"):
            new_name = new_name[len(prefix) :]
            parse = new_name.split("_")
            will_be_updated = {}
            for i in range(len(parse)):
                if parse[i].lower().startswith("mtbls"):
                    if len(parse[i]) > 5:
                        will_be_updated[i] = parse[i][5:]
                    else:
                        will_be_updated[i] = ""
            for i in will_be_updated:
                parse[i] = ""
            suffix = '_'.join(parse).replace("__", "_")
            new_name = f"{prefix}{study_id}_{suffix}"
        return new_name
    
    def file_sort_key(self, file: FileState):
        if not file or not isinstance(file, FileState) or not file.value:
            return ""
        if "_NEG" in file.value:
            return f"_NEG_{file.value}"
        elif "_POS" in file.value:
            return f"_POS_{file.value}"
        elif "_ALT" in file.value:
            return f"_ALT_{file.value}"
        else:
            return file.value

    def split_folders_task(self, input_map: Dict[str, str], 
                    max_file_count_on_folder:int = 500, max_file_count_on_splitted_folder: int=500, min_file_count_on_splitted_folder=50) -> Dict[str, str]:
        current_files: Dict[str, FileState] = {x:FileState(key=x, value=input_map[x]) for x in input_map}
        referenced_directories: Dict[str, List[FileState]] = {}
        for key in current_files:
            file = current_files[key]
            if file.value:
                dirname = os.path.dirname(file.value)
                if dirname not in referenced_directories:
                    referenced_directories[dirname] = set()
                referenced_directories[dirname].add(current_files[key])
                
        for referenced_folder in referenced_directories:
            if len(referenced_directories[referenced_folder]) > max_file_count_on_folder:
                
                folder_files = list(referenced_directories[referenced_folder])
                file_pairs = {}

                for file in folder_files:
                    base, ext = os.path.splitext(file.key)
                    base2, ext2 = os.path.splitext(base)
                    double_ext = f"{ext2}{ext}"
                    
                    if double_ext in self.double_extension_pairs:
                        pair_ext =  self.double_extension_pairs[double_ext]
                        pair_file = f"{base2}{pair_ext}"
                        if pair_file in input_map:
                            if pair_file not in file_pairs:
                                file_pairs[pair_file] = set()
                            file_pairs[pair_file].add(file)
                            continue
                        
                    file_pairs[file.key] = set([file])
                        
                
                extensions_map: Dict[str, List[List[str]]] = {}
                
                for key in file_pairs:
                    ref_file = key
                    val = os.path.basename(ref_file).lower()
                    base, ext = os.path.splitext(val)
                    if ext not in extensions_map:
                        extensions_map[ext] = []
                        for _ in range(len(self.split_main_bucket_indices) + 1):
                            extensions_map[ext].append([])
                    matched = False
                    for bucket_group in self.split_main_bucket_indices:
                        if bucket_group in val.upper():
                            extensions_map[ext][self.split_main_bucket_indices[bucket_group]].extend(file_pairs[key])
                            matched = True
                            break
                    if not matched:
                        extensions_map[ext][0].extend(file_pairs[key])
                        
                for ext in extensions_map:
                    new_list = []
                    for values in extensions_map[ext]:
                        if values:                    
                            values.sort(key=lambda x: x.value.lower() if x and x.value else "")
                            
                            if len(values) > max_file_count_on_splitted_folder:
                                size = max_file_count_on_splitted_folder
                                
                                bucket = int(len(values) / max_file_count_on_splitted_folder)
                                for i in range(bucket):
                                    new_list.append(values[i*size: size])
                                
                                if len(values) % max_file_count_on_splitted_folder > 0:
                                    new_list.append(values[i*size:])
                            else:
                                new_list.append(values)
                    extensions_map[ext] = new_list
                    extensions_map[ext] = [x for x in extensions_map[ext] if x]
                    
                extensions = list(extensions_map.keys())
                for key in extensions:
                    counts = [len(x) for x in extensions_map[key]]
                    if sum(counts) < min_file_count_on_splitted_folder:
                        del extensions_map[key]
                    # if len(extensions_map[key]) == 1 and len(extensions_map[key][0]) < min_file_count_on_splitted_folder:
                        
                
                        
                for extension in extensions_map:
                    folder_count = len(extensions_map[extension])
                    cleared_extension = extension.replace(".", "").upper()
                    prefix = f"{cleared_extension}_" if cleared_extension and cleared_extension != "ZIP" else ""

                    for i in range(folder_count):
                        if folder_count == 1:
                            subfolder_name = cleared_extension
                        else:
                            subfolder_name = f"{prefix}{(i + 1):03}"
                        
                        # last = min(extension_file_count, (i + 1) * maximum)
                        for file in extensions_map[extension][i]:
                            dirname = os.path.dirname(file.value)
                            basename = os.path.basename(file.value)
                            input_map[file.key] = os.path.join(dirname, subfolder_name, basename)
        return input_map

        
    def run(self, study_metadata_path: str, all_actions_fd=None, all_files_fd=None, missing_sha_f=None):
        self.remove_hidden_files()
        self.remove_empty_folders()
        self.sanitise_filenames()
        self.compress_folders()
        self.recompress_files()
        self.remove_compressed_files()
        # self.make_unique_filenames()
        self.split_folders()
        self.sanitise_paths()
        
        self.load_metadata_file_references(study_metadata_path)
        self.calculate_metadata_sha256(study_metadata_path)
        if len(self.sha256_dict) > 0:
            self.check_missing_sha256()
        self.print_actions(all_actions_fd)
        # self.print_mapping(all_files_fd)
        self.print_data_folder_summary(missing_sha_f)
        # self.print_duplicated_files()
        


if __name__ == "__main__":
    # studies: List[Tuple[str, str, int]] = get_studies(reverse=False)
    studies =[("MTBLS93", "", 2)]
    # studies = [(f"MTBLS{i}", "", 2) for i in range(700, 9053)]
    exclude_list = ["AUDIT_FILES", "INTERNAL_FILES"]
    missing_sha_f = open("missing_sha256.tsv", "w")
    for study in studies:
        study_id = study[0]
        obfuscationcode = obfuscationcode = study[1]
        study_status =  StudyStatus(study[2])
        read_only_states = {StudyStatus.DORMANT, StudyStatus.INREVIEW, StudyStatus.PUBLIC, StudyStatus.INCURATION, StudyStatus.SUBMITTED}
        if study_status in read_only_states:
            metadata_files_path = get_settings().study.mounted_paths.study_metadata_files_root_path
            folder = os.path.join(metadata_files_path, study_id)
            print(study_id)
            files: Dict[str, FileDescriptor]  = get_all_study_metadata_and_data_files(folder, exclude_list=exclude_list, include_metadata_files=False, add_sub_folders=False, list_all_files=True)
            pipeline = DataFolderUpdatePipeline(study_id=study_id, files=files, sha256_dict=sha256_dict)
            pipeline.run(study_metadata_path=folder, missing_sha_f=missing_sha_f)