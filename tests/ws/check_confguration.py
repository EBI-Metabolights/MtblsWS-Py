import re
import subprocess

from flask import Flask


ignore_used_in_other_packages = ['MAIL_PASSWORD', 'MAIL_PORT', 'MAIL_SERVER',
                                 'MAIL_USERNAME', 'MAIL_USE_SSL', 'MAIL_USE_TLS']


def check_configuration(verbose=True):
    app = Flask(__name__, instance_relative_config=True)
    default_configurations = []
    for default_configuration in app.config:
        default_configurations.append(default_configuration)
    app.config.from_object('config')
    app.config.from_pyfile('config.py', silent=True)

    source_path = "./app"
    ignore_list = list(set(ignore_used_in_other_packages).union(default_configurations))
    used_configs, unused_configs = check_config_file_usage(app.config, source_path, ignore_list)
    configs_in_files, files_use_configs = get_configs_used_in_source_files()

    undefined_configs = []
    for config_param in configs_in_files:
        if config_param not in app.config:
            undefined_configs.append(config_param)

    if verbose:
        if not unused_configs and not undefined_configs:
            print(f"\033[92mNo configuration parameter problem.\033[0m")
        else:
            if unused_configs:
                print(f"\033[91m\n- Unused config list: {unused_configs}\n\033[0m")
                for unused in unused_configs:
                    print(f"\033[93m- Unused config in config.py: {unused}\033[0m")

            if undefined_configs:
                print(f"\033[91m- Undefined config list: {undefined_configs}\033[0m")
                for config in undefined_configs:
                    files = ", ".join(configs_in_files[config])
                    print(f"\033[93m- Undefined! Config in source file is not in config.py: {config} <- {files}\033[0m")
        print('\033[0m')
    return unused_configs, undefined_configs


def get_configs_used_in_source_files(path="./app"):
    patterns = [f'grep -R "config.get(\'[A-Za-z_0-9_$]*\')" {path}',
                f'grep -R "config.get(\"[A-Za-z_0-9_$]*\")" {path}']

    files_use_configs = {}
    for pattern in patterns:
        p = subprocess.run(pattern, shell=True, capture_output=True)
        lines = p.stdout.decode().split('\n')
        if lines:
            for line in lines:
                stripped_line = line.strip()
                if not stripped_line or stripped_line.startswith("#"):
                    continue
                file_name = stripped_line.split()[0].replace(":", "")

                regex = 'config.get\(\s*[\'\"].*?[\'\"]\s*\)'
                results = re.findall(regex, line)
                for result in results:
                    regex_match = '[\'\"].*?[\'\"]'
                    config_values = re.findall(regex_match, result)
                    for item in config_values:
                        config_value = item.replace("\"", "").replace("\'", "")
                        if file_name not in files_use_configs:
                            files_use_configs[file_name] = set()
                        files_use_configs[file_name].add(config_value)

    configs_in_files = {}

    for key, config_set in files_use_configs.items():
        if config_set:
            for config in config_set:
                if config not in configs_in_files:
                    configs_in_files[config] = set()
                configs_in_files[config].add(key)

    return configs_in_files, files_use_configs


def check_config_file_usage(config, path, ignore_list):
    used_configs = {}
    unused_configs = []
    for config_item in config:
        if config_item in ignore_list:
            continue
        total_reference = []
        patterns = [f'grep -R "config.get(\'{config_item}\')" {path}',
                    f'grep -R \'config.get("{config_item}")\' {path}']
        for pattern in patterns:
            p = subprocess.run(pattern, shell=True, capture_output=True)
            lines = p.stdout.decode().split('\n')
            if lines:
                for line in lines:
                    clear_line = line.strip()
                    if config_item in clear_line and not clear_line.startswith("#"):
                        total_reference.append(line)

        if not total_reference:
            unused_configs.append(config_item)
        else:
            used_configs[config_item] = total_reference
    return used_configs, unused_configs


if __name__ == "__main__":
    check_configuration()
