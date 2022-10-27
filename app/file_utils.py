import json
import os


def load_json_credentials_file(file_name, secrets_dir=".secrets"):
    return load_json_file(file_name, secrets_dir)
    
    
def load_json_config_file(file_name, configs_dir="configs"):
    return load_json_file(file_name, configs_dir)
    
    
def load_json_file(file_name, directory):
    file_path = os.path.join(directory, file_name)
    with open(file_path) as file:
        return json.load(file)


def make_dir_with_chmod(file_path, chmod):
    if not os.path.exists(file_path):
        os.makedirs(file_path, mode=chmod, exist_ok=True)
    previous_mask = os.umask(0)
    try:
        os.chmod(file_path, mode=chmod)
    finally:
        os.umask(previous_mask)
