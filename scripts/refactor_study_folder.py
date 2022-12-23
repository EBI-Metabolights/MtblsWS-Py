from typing import Dict, List, Tuple
import cProfile
import datetime
import enum
import glob
import os
import pstats
import re
import time
from pydantic import BaseModel
from app.ws.file_reference_evaluator import File, FileReferenceEvaluator, StudyFolder

VALID_FILENAME_PATTERN = re.compile(r'^[\w]([\w \.-]*[\w])?$') 
VALID_DIR_PATTERN = re.compile(r'^([\w]([\w \.-]*[\w])?)(/[\w]([\w \.-]*[\w])?)*$') 
def profiler(func):
    def profiler_handler(*args, **kwargs):
        profiler = cProfile.Profile()
        profiler.enable()
        try:
            return func(*args, **kwargs)
        finally:
            profiler.disable()
            stats = pstats.Stats(profiler)
            os.makedirs(f"./.profile/{func.__module__}", exist_ok=True)
            stats.dump_stats(f'./.profile/{func.__module__}/{func.__qualname__}_profile_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.prof')
        
    return profiler_handler

class OperationType(str, enum.Enum):
    RENAME = "RENAME",
    MOVE = "MOVE",
    NO_ACTION = "NO_ACTION",
    COMPRESS = "COMPRESS",
    FIND_CANDIDATE = "FIND_CANDIDATE"
     
class RefactorOperation(BaseModel):
    name: str = ""
    source: File = None
    operation: OperationType = OperationType.NO_ACTION
    target: str = None
    
class RefactorSuggessions(BaseModel):
    operations: Dict[str, List[RefactorOperation]] = {}

class ReportItem(BaseModel):
    study_id: str = ""
    is_summary: bool = False
    level: str = ""
    source: str = ""
    status: str = "WARNING"
    category: str = ""
    description: str = ""

class Report(BaseModel):
    start_time: int = 0
    end_time: int = 0
    report_items: Dict[str, List[ReportItem]] = {}
    hierarchy: StudyFolder = None
    suggessions: RefactorSuggessions = None
    
class PathEvaluator(object):
        
    def exist(self, root_path, relative_path):
        full_path = os.path.join(root_path.rstrip(os.sep), relative_path)
        return os.path.exists(full_path)
    
    def isfile(self, root_path, relative_path):
        full_path = os.path.join(root_path.rstrip(os.sep), relative_path)
        return os.path.exists(full_path) and os.path.isfile(full_path)   

    def isdir(self, root_path, relative_path):
        full_path = os.path.join(root_path.rstrip(os.sep), relative_path)
        return os.path.exists(full_path) and os.path.isdir(full_path)    
        
class StudyFolderAuditor(object):
    def __init__(self, path_evaluator=None, 
                 ignore_file_list=None, 
                 ignore_folder_list=None, 
                 allowed_reference_folders=None, 
                 referenced_folders_contain_files=None,
                 managed_data_folders=None,
                 metadata_folder=None) -> None:
        
        self.reference_evaluator = FileReferenceEvaluator()
        self.path_evaluator = path_evaluator if path_evaluator else PathEvaluator()
        self.ignore_file_list = ignore_file_list if ignore_file_list else ["files-all.json", 
                                                                           "validation_files.json", 
                                                                           "validation_report.json", 
                                                                           "metexplore_mapping.json", 
                                                                           "missing_files.txt"]
        self.ignore_folder_list = ignore_folder_list if ignore_folder_list else ["chebi_pipeline_annotations", "audit"]
        self.allowed_reference_folders = allowed_reference_folders if allowed_reference_folders else [".raw", ".RAW", ".D", ".d"]
        self.referenced_folders_contain_files = referenced_folders_contain_files if referenced_folders_contain_files else ["acqus", "fid", "acqu"]
        self.metadata_folder = metadata_folder if metadata_folder else ""
        self.managed_data_folders = managed_data_folders if managed_data_folders else ["RAW_FILES", "DERIVED_FILES", "SUPPLEMENTARY_FILES"]
    
    def get_reference_hierarchy(self, study_id: str, study_path: str) -> StudyFolder:
        return self.reference_evaluator.get_reference_hierarchy(study_id, study_path) 
        
    def find_non_existent_files(self, study_path: str, hierarchy: StudyFolder) -> List[str]:
        non_existenent_files = [path for path in hierarchy.file_index_map if not self.path_evaluator.exist(study_path, path)]
        return non_existenent_files

    def find_folders_need_compression(self, study_path: str, hierarchy: StudyFolder) -> List[str]:
        folders_need_compression = [path for path in hierarchy.file_index_map if self.path_evaluator.exist(study_path, path) and self.path_evaluator.isdir(study_path, path)]            
        return folders_need_compression
    
    def find_referenced_folders_contain_special_files(self, study_path, hierarchy: StudyFolder):
        referenced_folders_contain_special_files = []
        for folder in hierarchy.search_path:
            if folder:
                folder_path = os.path.join(study_path, folder)
                if os.path.exists(folder_path):
                    files = os.listdir(folder_path)
                    for file in files:
                        for special_file in self.referenced_folders_contain_files:
                            path = os.path.join(folder_path, file, special_file).replace(study_path, "").lstrip()
                            if path in hierarchy.file_index_map:
                                reference_path = os.path.join(folder, file).replace(study_path, "").lstrip()
                                referenced_folders_contain_special_files.append(reference_path)
            
        return referenced_folders_contain_special_files
    
    def find_unreferenced_files(self, study_path, hierarchy: StudyFolder):
        unreferenced_files = { "files":[], "folders": [], "uncategorized": []}
        ordered_referenced_paths = self.reference_evaluator.get_referenced_paths(study_path, hierarchy, 
                                                             ignored_folder_list=self.ignore_folder_list,                                                 
                                                             referenced_folder_extensions=self.allowed_reference_folders,
                                                             referenced_folders_contain_files=self.referenced_folders_contain_files)

        for referenced_path in ordered_referenced_paths:
            if self.path_evaluator.exist(study_path, referenced_path) and self.path_evaluator.isdir(study_path, referenced_path):
                folder_files = list(os.listdir(referenced_path))
                for file in folder_files:   
                    file_path = os.path.join(referenced_path, file)
                    relative_path = file_path.replace(f"{study_path.rstrip(os.sep)}{os.sep}", "")
                    if self.path_evaluator.isfile(study_path, relative_path):
                        if relative_path not in hierarchy.file_index_map and relative_path not in self.ignore_file_list:
                            unreferenced_files["files"].append(relative_path)
                    elif self.path_evaluator.isdir(study_path, relative_path):
                        if relative_path in self.ignore_folder_list:
                            continue
                        if relative_path not in hierarchy.file_index_map:
                            if relative_path not in self.ignore_file_list and file_path not in ordered_referenced_paths:
                                unreferenced_files["folders"].append(relative_path)
                    else:
                        unreferenced_files["uncategorized"].append(relative_path)
                            
        return unreferenced_files

    def find_invalid_referenced_file_names(self, hierarchy: StudyFolder) -> List[str]:
        invalid_file_names = []
        for file_name in hierarchy.file_index_map.keys():
            basename = os.path.basename(file_name)
            if not VALID_FILENAME_PATTERN.match(basename):   
                invalid_file_names.append(file_name)
        return invalid_file_names

    def find_invalid_referenced_folder_names(self, hierarchy: StudyFolder) -> List[str]:
        invalid_folder_names = []
        for folder in hierarchy.search_path:
            if folder and folder not in hierarchy.file_index_map:
                if not VALID_DIR_PATTERN.match(folder):   
                    invalid_folder_names.append(folder)
        return invalid_folder_names
        
    def evaluate_invalid_referenced_file_names(self, hierarchy: StudyFolder, report: Report, suggessions: RefactorSuggessions) -> None:
        category = "invalid_file_name"
        operation_name =  "rename_file" 
        invalid_file_names = self.find_invalid_referenced_file_names(hierarchy)
        description="Action: Rename it and update references in assay file (File must start without . and contain only space, alpha numberic chars and . _ -  )."
        self.add_report_items_to_report(invalid_file_names, report, category=category, description=description)
             
        for invalid_file_name in invalid_file_names:
            refactor = RefactorOperation(name=operation_name, operation=OperationType.RENAME, source=hierarchy.file_index_map[invalid_file_name])
            refactor.target = self.rename_file_name(refactor.source.name)
            self.add_operation_to_file(refactor.source, refactor)
            self.add_operation_to_suggession(suggessions, operation_name, refactor)
    
    def evaluate_invalid_referenced_folder_names(self, hierarchy: StudyFolder, report: Report, suggessions: RefactorSuggessions) -> None:
        category = "invalid_folder_name"
        operation_name = "rename_folder"      
        description="Invalid folder name characters\tAction: Rename it and update references in assay file(Folder must start without . and contain only space, alpha numberic chars and . _ -  )."
        invalid_folder_names = self.find_invalid_referenced_folder_names(hierarchy)
        
        self.add_report_items_to_report(invalid_folder_names, report, category=category, description=description)

        for invalid_folder_name in invalid_folder_names:
            new_folder_path = self.rename_file_name(invalid_folder_name)
            for file_name in hierarchy.file_index_map.keys():
                file_item = hierarchy.file_index_map[file_name]
                if file_item.path != invalid_folder_name and file_item.path.startswith(invalid_folder_name):
                    refactor = RefactorOperation(name=operation_name, operation=OperationType.MOVE, source=file_item)
                    refactor.target = refactor.source.path.replace(invalid_folder_name, new_folder_path)
                    self.add_operation_to_file(file_item, refactor)
                    self.add_operation_to_suggession(suggessions, operation_name, refactor)

    def evaluate_nonexistent_files(self, hierarchy: StudyFolder, report: Report, suggessions: RefactorSuggessions) -> None:
        category = "non_existent_file_or_folder"
        operation_name = "find_candidate"
        description="Options: 1) Upload this file / folder, 2) If it exists, move to correct folder and/or rename it, 3) Contact with Metabolights team"
        results = self.find_non_existent_files(hierarchy.study_path, hierarchy)
        self.add_report_items_to_report(results, report, category=category, description=description)

        for result_item in results:
            file_item = hierarchy.file_index_map[result_item]
            refactor = RefactorOperation(name=operation_name, operation=OperationType.FIND_CANDIDATE, source=file_item)
            refactor.target = ""
            self.add_operation_to_file(file_item, refactor)
            self.add_operation_to_suggession(suggessions, operation_name, refactor)

    def evaluate_referenced_folders_need_compression(self, hierarchy: StudyFolder, report: Report, suggessions: RefactorSuggessions) -> None:
        category = "need_compression"
        operation_name = "compress"
        description ="Options: 1) Compress folder (with zip extension) after content validation and update reference"
        results = self.find_folders_need_compression(hierarchy.study_path, hierarchy)
        self.add_report_items_to_report(results, report, category=category, description=description)

        for result_item in results:
            file_item = hierarchy.file_index_map[result_item]
            refactor = RefactorOperation(name=operation_name, operation=OperationType.COMPRESS, source=file_item)
            refactor.target = f"{result_item}.zip"
            self.add_operation_to_file(file_item, refactor)
            self.add_operation_to_suggession(suggessions, operation_name, refactor)
            
    def evaluate_folders_contain_referenced_special_files(self, hierarchy: StudyFolder, report: Report, suggessions: RefactorSuggessions) -> None:
        category = "need_parent_folder_compression"
        operation_name = "compress"
        
        results = hierarchy.referenced_folders_contain_special_files
        target = ""
        for folder in results:
            result_items = results[folder]
            names = []
            for result_item in result_items:
                file_item = hierarchy.file_index_map[result_item]
                names.append(result_item)
                refactor = RefactorOperation(name=operation_name, operation=OperationType.COMPRESS, source=file_item)
                refactor.target = f"{os.path.dirname(result_item)}.zip"
                target = refactor.target
                self.add_operation_to_suggession(suggessions, operation_name, refactor)
                self.add_operation_to_file(file_item, refactor)
                
            if names:
                target = target.replace(hierarchy.study_path, '').lstrip()
                
                description = f"Options: Compress parent folder  and reference the compressed file in assay. Use {target} instead of {', '.join(names)}"
                report_item = ReportItem(study_id=hierarchy.study_id, source=os.path.join(file_item.path, file_item.name) , category=category, description=description)
                self.add_report_item_to_report(report_item, report)
                        

    def evaluate_unreferenced_files(self, hierarchy: StudyFolder, report: Report, suggessions: RefactorSuggessions) -> None:
        category = "unreferenced_file"
        results = self.find_unreferenced_files(hierarchy.study_path, hierarchy)

        description="Options: 1) Reference it in ISA METADATA files, 2) Move it to correct folder and/or rename it 3) Delete it 4) Contact with Metabolights team"
        self.add_report_items_to_report(results["files"], report, category=category, description=description)
        
        description = "Options: 1) Reference containing files in ISA METADATA files, 2) Move it to correct folder and/or rename it 3) Delete it 4) Contact with Metabolights team"
        self.add_report_items_to_report(results["folders"], report, category=category, description=description)
        
        description = "Options: 1) Delete this uncategorized file or folder, 2) Contact with Metabolights team"
        self.add_report_items_to_report(results["uncategorized"], report, category=category, description=description)
    
    ISA_METADATA_FILE_PATTERN = re.compile(r'([as]_.+\.txt|i_Investigation.txt|m_.+\.tsv)')
    def evaluate_study_folder_structure(self, hierarchy: StudyFolder, report: Report, suggessions: RefactorSuggessions) -> None:
        category = "refactor_file_directory"
        report_items = []       
        new_structure_actions = self.create_new_study_structure_actions(hierarchy, report, suggessions)
        for result_item in new_structure_actions:
            target_path = os.path.join(result_item.target, result_item.source.name)
            description = f"Move file to {target_path}" 
            source = os.path.join(result_item.source.path, result_item.source.name)
            report_item = ReportItem(study_id=hierarchy.study_id, source=source, category=category, description=description)
            report_items.append(report_item)
        self.add_list_items_to_report(report_items, report=report, category=category)
        
    def create_new_study_structure_actions(self, hierarchy: StudyFolder, report: Report, suggessions: RefactorSuggessions) -> None:
        operation_name = 'move_file'
        new_structure_actions = []
        for file_name, item in hierarchy.file_index_map.items():
            if os.path.exists(os.path.join(hierarchy.study_path, file_name)):
                if not self.ISA_METADATA_FILE_PATTERN.match(item.name):
                    target = None
                    if file_name in hierarchy.raw_files and not item.path.startswith(hierarchy.raw_file_folder):
                        target = os.path.join(hierarchy.raw_file_folder, item.path) if item.path else hierarchy.raw_file_folder
                    elif file_name in hierarchy.derived_files and not item.path.startswith(hierarchy.derived_file_folder):
                        target = os.path.join(hierarchy.derived_file_folder, item.path) if item.path else hierarchy.derived_file_folder
                    elif file_name in hierarchy.supplementary_data_files and not item.path.startswith(hierarchy.supplementary_data_folder):
                        target = os.path.join(hierarchy.supplementary_data_folder, item.path) if item.path else hierarchy.supplementary_data_folder
                    if target:
                        refactor = RefactorOperation(name=operation_name, operation=OperationType.MOVE, source=item)
                        refactor.target = target
                        self.add_operation_to_file(item, refactor)
                        self.add_operation_to_suggession(suggessions, operation_name, refactor) 
                        new_structure_actions.append(refactor)                       
        return new_structure_actions
        
    @staticmethod
    def add_operation_to_file(file: File, operation: RefactorOperation):
        if "refactor" not in file.metadata:
            file.metadata["refactor"] = []
        file.metadata["refactor"].append(operation)
        
    @staticmethod
    def add_operation_to_suggession(suggessions: RefactorSuggessions, operation_name: str, operation: RefactorOperation):
        if operation_name not in suggessions.operations:
            suggessions.operations[operation_name] = []
            
        suggessions.operations[operation_name].append(operation)
               
    @staticmethod
    def add_report_items_to_report(items: List[str], report: StudyFolder, category: str, description: str):
        if items:
            if category not in report.report_items:
                report.report_items[category] = []
            report_item_list = report.report_items[category]
            for item in items:
                report_item = ReportItem(study_id=study_id, source=item, category=category, description=description)
                report_item_list.append(report_item)

    @staticmethod
    def add_report_item_to_report(item: ReportItem, report: StudyFolder):
        if item:
            if item.category not in report.report_items:
                report.report_items[item.category] = []
            report_item_list = report.report_items[item.category]
            report_item_list.append(item)

    @staticmethod
    def add_list_items_to_report(items: List[ReportItem], report: StudyFolder, category: str):
        if category not in report.report_items:
            report.report_items[category] = []
        for item in items:
            report_item_list = report.report_items[category]
            report_item_list.append(item)
            
    @staticmethod
    def rename_file_name(name: str) -> str:
        value = re.sub(r'[^a-zA-Z0-9 _\-\.]', "___", name)
        return value.strip().lstrip(".")
 

    def evaluate_study(self, study_id, study_path) -> Report:
        study_folder_auditor = StudyFolderAuditor()
        hierarchy = study_folder_auditor.get_reference_hierarchy(study_id, study_path)
        suggessions = RefactorSuggessions()
        report = Report(hierarchy=hierarchy, suggessions=suggessions)
        report.start_time = time.time()
        
        study_folder_auditor.evaluate_invalid_referenced_file_names(hierarchy, report, suggessions)
        
        study_folder_auditor.evaluate_invalid_referenced_folder_names(hierarchy, report, suggessions)
        
        study_folder_auditor.evaluate_nonexistent_files(hierarchy, report, suggessions)
        
        study_folder_auditor.evaluate_referenced_folders_need_compression(hierarchy, report, suggessions)
        
        study_folder_auditor.evaluate_folders_contain_referenced_special_files(hierarchy, report, suggessions)

        study_folder_auditor.evaluate_unreferenced_files(hierarchy, report, suggessions)
        
        study_folder_auditor.evaluate_study_folder_structure(hierarchy, report, suggessions) 
        
        report.end_time = time.time()
        return report

class ReportWriter(object):
    
    def __init__(self, file) -> None:
        self.file = file
        self._write_report_header()
    
    def print_report_to_file(self, report: Report):
        for _, item_list in report.report_items.items():
            for item in item_list:
                self.write_report_item_to_file(item)
        self.write_profile_to_file(report)
        self.write_statistics_to_file(report)

    def _write_report_header(self):
        self.file.write(f"STUDY_ID\tIS_SUMMARY\tSOURCE\tCATEGORY\tSTATUS\tDESCRIPTION\n")
            
    def write_report_item_to_file(self, item: ReportItem):
        summary = "Y" if item.is_summary else "N"
        category = item.category.replace("_", " ").title()
        source = item.study_id if item.is_summary else f'"{item.source}"'
        self.file.write(f"{item.study_id}\t{summary}\t{source}\t{category}\t{item.status}\t{item.description}\n")
        
    def write_profile_to_file(self, report: Report):
        study_id = report.hierarchy.study_id
        description = round(report.end_time - report.start_time, 4)
        profile = ReportItem(study_id=study_id, is_summary=True, status="INFO", category="profile", description=description)
        self.write_report_item_to_file(profile)
                         
    def write_statistics_to_file(self, report: Report):
        study_id = report.hierarchy.study_id
        statistics = []
        total = 0
        for category in report.report_items:
            item_count = len(report.report_items[category])
            total += item_count
            if item_count > 0:
                statistics.append(f"{category}: {item_count}")
        if statistics:
            description = f"Reported issue statistics: Total: {total}, {' '.join(statistics)}"
            status = "WARNING"
        else:
            description = f"There is no issue reported."
            status = "INFO"
        statistics_item = ReportItem(study_id=study_id, is_summary=True, status=status, category="statistics", description=description)
        self.write_report_item_to_file(statistics_item)

def study_id_compare(key: str):
    if key:
        val = os.path.basename(key)
        val = val.replace("MTBLS", '')
        if val.isnumeric():
            return int(val)
    return 0

if __name__ == "__main__":
    
    #study_root_path = "/net/isilonP/public/rw/homes/tc_cm01/metabolights/dev/studies/new_structure"
    study_root_path = "/net/isilonP/public/rw/homes/tc_cm01/metabolights/prod/studies/stage/private"
    studies = glob.glob(os.path.join(study_root_path, 'MTBLS*'))
    public_studies = set()
    with open('./public__in_review_studies.txt', 'r') as file:
        for line in file.readlines():
            public_studies.add(line.strip())
    study_folders = [f for f in studies if os.path.isdir(f) and os.path.basename(f) in public_studies]
    # study_folders = ["MTBLS1", "MTBLS2", "MTBLS3", "MTBLS4", "MTBLS5", "MTBLS6", "MTBLS7", "MTBLS8", "MTBLS9", "MTBLS10"]
    # study_folders = ["MTBLS2870"]
    start_time = time.time()
    study_folders.sort(key=study_id_compare, reverse=False)
    print(f"Evaluation is started")
    page = 0
    page_size = 30000
    first = page * page_size
    last = (page + 1) * page_size
    if first >= len(study_folders):
        print(f"Illegal index {first}. Select ")
    if last >= len(study_folders):
        selected_study_folders = study_folders[first:]
    else:
        selected_study_folders = study_folders[first:last]
    auditor = StudyFolderAuditor()
    study_reports = {}
    with open('./_refactor-report.tsv', 'w') as file:
        writer = ReportWriter(file)
        block_study_eval_start_time = time.time()
        previous_count = 1  
        count = 0
        study_id_list = []
        for study in selected_study_folders:
            count = count + 1
            study_id = os.path.basename(study)
            study_id_list.append(study_id)
            study_path = os.path.join(study_root_path, study_id)
            
            report = auditor.evaluate_study(study_id, study_path)
            
            study_reports[study_id] = report
            writer.print_report_to_file(report)
            
            if count % 10 == 0:
                print(f"{previous_count:04}-{count:04} {', '.join(study_id_list)} evaluation took %s seconds" % round(time.time() - block_study_eval_start_time, 4))
                block_study_eval_start_time  = time.time()
                previous_count = count + 1
                study_id_list.clear()
    
    print(f"Evaluation of all studies ({len(study_folders)}) took %s seconds" % round(time.time() - start_time, 4))
    