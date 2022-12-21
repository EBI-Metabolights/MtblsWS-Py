import cProfile
import datetime
import glob
import os
import pstats
import re
import time
import yaml
from app.ws.file_reference_evaluator import FileReferenceEvaluator, StudyFolder
FOLDER_SEARCH_SKIP_FILE_NAMES = ["fid", "acqu", "acqus"]

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
        
class StudyFolderRefactorManager(object):
    
    def __init__(self, study_root_path, path_evaluator=None, ignore_file_list=None, 
                 ignore_folder_list=None, 
                 allowed_reference_folders=None, 
                 referenced_folders_contain_files=None) -> None:
        self.study_root_path = study_root_path
        self.reference_evaluator = FileReferenceEvaluator()
        self.path_evaluator = path_evaluator if path_evaluator else PathEvaluator()
        self.ignore_file_list = ignore_file_list if ignore_file_list else []
        self.ignore_folder_list = ignore_folder_list if ignore_folder_list else []
        self.allowed_reference_folders = allowed_reference_folders if allowed_reference_folders else [".raw", ".RAW", ".D", ".d"]
        self.referenced_folders_contain_files = referenced_folders_contain_files if referenced_folders_contain_files else ["fid", "acqu", "acqus"]
        self.managed_folders = ["RAW_FILES", "DERIVED_FILES", "SUPPLEMENTARY_FILES"]
    def update(self, study_id, path):
        reference_files = self.reference_evaluator.get_referenced_file_list(study_id=study_id, path=path)
        for file in reference_files:
            print(file)
        
    def evaluate_reference_hierarchy(self, study_path, hierarchy):
        
        non_exist_files = [path for path in hierarchy.file_index_map if not self.path_evaluator.exist(study_path, path)]
        need_compression_folders = [path for path in hierarchy.file_index_map if self.path_evaluator.exist(study_path, path) and self.path_evaluator.isdir(study_path, path)]            
        return non_exist_files, need_compression_folders
        
    def find_unreferenced_files(self, study_path, hierarchy: StudyFolder):
        unreferenced_files = { "files":[], "folders": [], "uncategorized": [], "need_reference_update": []}
        ordered_referenced_paths, skiped_folders = self.reference_evaluator.get_referenced_paths(study_path, hierarchy, 
                                                             ignored_folder_list=self.ignore_folder_list,                                                 
                                                             referenced_folder_extensions=self.allowed_reference_folders,
                                                             referenced_folders_contain_files=self.referenced_folders_contain_files)
        unreferenced_files["need_reference_update"] = skiped_folders
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
    
def print_list_items_with_action(file, study_id, item_list, category, action, status="WARNING"):
    for item in item_list:
        file.write(f"{study_id}\t\"{item}\"\t{status}\t{category}\tAction: {action}\n")

VALID_FILENAME_PATTERN = re.compile(r'^[a-zA-Z0-9_][a-zA-Z0-9_\. -]*[a-zA-Z0-9_]$')
VALID_DIR_PATTERN = re.compile(r'^[a-zA-Z0-9_][a-zA-Z0-9_\. -/]*[a-zA-Z0-9_]$')

def evaluate_referenced_folder_names(folders, file, study_id):
    for folder in folders:
        if folder and not VALID_DIR_PATTERN.match(folder):
                file.write(f"{study_id}\t\"{folder}\"\tWARNING\tInvalid folder name characters\tAction: Rename it and update references in assay file(Folder must start without . and contain only space, alpha numberic chars and . _ -  ).\n")


def evaluate_referenced_file_names(file_names, file, study_id):
    for file_name in file_names:
        basename = os.path.basename(file_name)
        if not VALID_FILENAME_PATTERN.match(basename):
            file.write(f"{study_id}\t\"{file_name}\"\tWARNING\tInvalid filename characters\tAction: Rename it and update references in assay file (File must start without . and contain only space, alpha numberic chars and . _ -  ).\n")

def evaluate_study(file, study_root_path, study_id, study_path):
    ignore_folder_list = ["chebi_pipeline_annotations", "audit"]
    ignore_file_list = ["files-all.json", "validation_files.json", "validation_report.json", "metexplore_mapping.json", "missing_files.txt"]
    refactor_manager = StudyFolderRefactorManager(study_root_path,ignore_folder_list=ignore_folder_list, ignore_file_list=ignore_file_list)

    hierarchy = refactor_manager.reference_evaluator.get_reference_hierarchy(study_id, study_path)
    
    evaluate_referenced_folder_names(hierarchy.referenced_folders,file, study_id)
    evaluate_referenced_file_names(hierarchy.file_index_map.keys() ,file, study_id)
    
    if not hierarchy.sample_file:
        file.write(f"{study_id}\tStep 0.1\tWARNING\tInvestigation file check\tInvestigation file in reference hierarchy does not exist\n")
        
    if not hierarchy.sample_file:
        file.write(f"{study_id}\tStep 0.2\tWARNING\tSample file check\tSample file in reference hierarchy does not exist\n")
        
    if not hierarchy.assay_files:
        file.write(f"{study_id}\tStep 0.3\tWARNING\tAssay file check\tAssay file in reference hierarchy does not exist\n")
    
    non_exist_files, need_compression_folders = refactor_manager.evaluate_reference_hierarchy(study_path, hierarchy)

    if not non_exist_files:
        file.write(f"{study_id}\tStep 1\tOK\tNonexistent file/folder check\tAll files in reference hierarchy exist\n")
    else:
        file.write(f"{study_id}\tStep 1\tWARNING\tNonexistent file check\tSome files in reference hierarchy do not exist.\n")
        action="Options: 1) Upload this file / folder, 2) If it exists, move to correct folder and/or rename it, 3) Contact with Metabolights team"
        print_list_items_with_action(file, study_id=study_id, 
                                                item_list=non_exist_files, 
                                                category="Nonexistent file", 
                                                action=action)
        
    action="Options: 1) Compress (with zip extension) folder after content validation and update reference, 2) Contact with Metabolights team"
    print_list_items_with_action(file, study_id=study_id, 
                                            item_list=need_compression_folders, 
                                            category="Uncompressed referenced folder", 
                                            action=action)       
        
    unreferenced_files = refactor_manager.find_unreferenced_files(study_path, hierarchy)

    if unreferenced_files and "uncategorized" in unreferenced_files and unreferenced_files["uncategorized"]:
        file.write(f"{study_id}\tStep 2.1\tWARNING\tUncategorized file check\tSome existing files/folders are not uncategorized\n")
        action="Options: 1) Delete this uncategorized file, 2) Contact with Metabolights team"
        print_list_items_with_action(file, study_id=study_id, 
                                                item_list=unreferenced_files["uncategorized"], 
                                                category="Uncategorized file", 
                                                action=action)
    else:
        file.write(f"{study_id}\tStep 2.1\tOK\tUncategorized file check\tAll existing files and folders in study folder are categorized\n")
        
    if unreferenced_files and "files" in unreferenced_files and unreferenced_files["files"]:
        file.write(f"{study_id}\tStep 2.2\tWARNING\tUnreferenced file check\tSome existing files are not in reference hierarchy\n")
        action="Options: 1) Reference it in ISA METADATA files, 2) Check assay file and move it to correct folder and/or rename it 3) Delete it 4) Contact with Metabolights team"
        print_list_items_with_action(file, study_id=study_id, 
                                                    item_list=unreferenced_files["files"], 
                                                    category="Unreferenced file", 
                                                    action=action)
    else:
        file.write(f"{study_id}\tStep 2.2\tOK\tUnreferenced file check\tAll existing files are in reference hierarchy\n")
        
    if unreferenced_files and "folders" in unreferenced_files and unreferenced_files["folders"]:
        file.write(f"{study_id}\tStep 2.3\tWARNING\tUnreferenced folder check\tSome existing folders are not in reference hierarchy\n")
        action="Options: 1) Reference it in ISA METADATA files, 2) Check assay file and move it to correct folder and/or rename it 3) Delete it 4) Contact with Metabolights team"
        print_list_items_with_action(file, study_id=study_id, 
                                                item_list=unreferenced_files["folders"], 
                                                category="Unreferenced folder", 
                                                action=action)
    else:
        file.write(f"{study_id}\tStep 2.3\tOK\tUnreferenced folder check\tAll existing folders are in reference hierarchy\n")
        
        
    file.write(f"{study_id}\tStep 2.4\tWARNING\tReference to a file in uncompressed folder\tSome reference hierarchy files are in uncompresses folders. Compress parent folder \n")
    action="Options: 1) Compress parent folder and reference this compressed file in assay, 2) Contact with Metabolights team"
    print_list_items_with_action(file, study_id=study_id, 
                                            item_list=unreferenced_files["need_reference_update"], 
                                            category="File reference in Uncompressed folder", 
                                            action=action) 
        
    return hierarchy

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
    # study_folders = ["MTBLS4065"]
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
    with open('./_refactor-report.tsv', 'w') as file:
        file.write(f"STUDY ID\tFILE/STEP\tSTATUS\tCATEGORY\tDESCRIPTION\n")
        block_study_eval_start_time = time.time()
        previous_count = 1  
        count = 0
        study_id_list = []
        for study in selected_study_folders:
            count = count + 1
            study_id = os.path.basename(study)
            study_id_list.append(study_id)
            study_path = os.path.join(study_root_path, study_id)
            study_eval_start_time = time.time()
            # categories = set(["isa_metadata", "invalid_file_names", "need_compression", "nonexist_files", "unreferenced_files"])
            hierarchy = evaluate_study(file, study_root_path, study_id, study_path)
            file.write(f"{study_id}\tProfile\tOK\tElapse Time\t%s\n" % round(time.time() - study_eval_start_time, 4))
            if count % 10 == 0:
                print(f"{previous_count:04}-{count:04} {', '.join(study_id_list)} evaluation took %s seconds" % round(time.time() - block_study_eval_start_time, 4))
                block_study_eval_start_time  = time.time()
                previous_count = count + 1
                study_id_list.clear()
    
    print(f"Evaluation of all studies ({len(study_folders)}) took %s seconds" % round(time.time() - start_time, 4))
    