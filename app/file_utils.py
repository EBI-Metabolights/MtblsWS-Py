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


def make_dir_with_chmod(file_path, chmod, exist_ok: bool=True):
    previous_mask = os.umask(0)
    try:
        if not os.path.exists(file_path):
            os.makedirs(file_path, mode=chmod, exist_ok=exist_ok)

        current_chmod = int(oct(os.stat(file_path).st_mode), 8)
        if current_chmod == int(chmod):
            return

        os.chmod(file_path, mode=chmod)
    finally:
        os.umask(previous_mask)
