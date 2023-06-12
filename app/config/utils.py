import configparser
import json
import os
import re
from pathlib import Path
from typing import Any, Dict

import toml
import yaml
from pydantic import BaseSettings


class MetabolightsConfigurationException(Exception):
    def __init__(self, message: str = ""):
        super(MetabolightsConfigurationException, self).__init__()
        self.message = message

    def __str__(self):
        return f"{str(self.__class__.__name__)}: {self.message}"


def get_path_from_environment(name):
    if name in os.environ and os.environ[name]:
        value = os.environ[name]
    else:
        raise MetabolightsConfigurationException(message=f"Environment variable '{name}' is not defined.")

    if not os.path.exists(value):
        message = f"{name} path '{value}' does not exist. Set this environment variable"
        raise MetabolightsConfigurationException(message=message)
    else:
        message = f"{name} is set as '{value}'"
        print(message)
    return value


PROJECT_PATH = Path(__file__).parent.parent.parent
CONFIG_FILE_PATH = get_path_from_environment("CONFIG_FILE_PATH")
SECRETS_PATH = get_path_from_environment("SECRETS_PATH")


class ApplicationBaseSettings(BaseSettings):
    class Config:
        secrets_path = SECRETS_PATH
        yaml_file_path = CONFIG_FILE_PATH

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (yaml_settings_source,)


secret_file_pattern = re.compile(r"\"\<secret_file\:([^>]*)\>\"")


def config_to_dict(config: configparser.ConfigParser):
    sections_dict = {}

    defaults = config.defaults()
    section_dict = {}
    for k, v in defaults.items():
        section_dict[v] = defaults[k]
    if section_dict:
        sections_dict["default"] = section_dict

    sections = config.sections()

    for section in sections:
        options = config.options(section)
        section_dict = {}
        for option in options:
            section_dict[option] = config.get(section, option)
        if section_dict:
            sections_dict[section] = section_dict

    return sections_dict


def update_secrets(secrets_path: str, data: str) -> str:
    secrets_path: Path = Path(secrets_path)
    for match in secret_file_pattern.findall(data):
        secret_file_path = secrets_path.joinpath(match)

        if not secret_file_path.exists():
            raise MetabolightsConfigurationException(f"Secret file '{match}' does not exist.")
        ext = secret_file_path.suffix.lower()
        if ext == ".json":
            secret_data = json.loads(secret_file_path.read_text(encoding="utf-8"))
        elif ext == ".yaml" or ext == ".yml":
            content = secret_file_path.open(mode="r").read()
            secret_data = yaml.safe_load(content)
        elif ext == ".toml":
            secret_data = toml.load(secret_file_path)
        elif ext == ".cfg" or ext == ".ini":
            config = configparser.ConfigParser()
            config.read(secret_file_path)
            secret_data = config_to_dict(config)
            secret_data = yaml.safe_load(content)
        else:
            secret_data = secret_file_path.open(mode="r").read()

        if not secret_data:
            secret_data = '""'
        if isinstance(secret_data, dict):
            secret_data = json.dumps(secret_data)
        data = data.replace(f'"<secret_file:{match}>"', secret_data)
    return data


def yaml_settings_source(settings: ApplicationBaseSettings) -> Dict[str, Any]:
    yaml_file = settings.__config__.yaml_file_path
    secrets_path = settings.__config__.secrets_path
    yaml_file_path = Path(yaml_file)

    if not yaml_file_path.exists():
        raise FileNotFoundError(f"Could not open yaml settings file at: {yaml_file_path}")

    with open(yaml_file, "r") as file:
        yaml_data = yaml.safe_load(file)

    json_data = json.dumps(yaml_data)
    updated_settings_data = update_secrets(secrets_path, json_data)
    return yaml.safe_load(updated_settings_data)
