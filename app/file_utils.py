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