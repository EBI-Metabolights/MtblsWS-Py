import configparser
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Tuple, Type

import toml
import yaml
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource
from pydantic.fields import FieldInfo


class MetabolightsConfigurationException(Exception):
    def __init__(self, message: str = ""):
        super(MetabolightsConfigurationException, self).__init__()
        self.message = message

    def __str__(self):
        return f"{str(self.__class__.__name__)}: {self.message}"


def get_path_from_environment(name, dafault):
    if name in os.environ and os.environ[name]:
        value = os.environ[name]
    else:
        value = dafault

    if not os.path.exists(value):
        message = f"{name} path '{value}' does not exist. Set this environment variable"
        raise MetabolightsConfigurationException(message=message)
    else:
        message = f"{name} is set as '{value}'"
        print(message)
    return value


PROJECT_PATH = Path(__file__).parent.parent.parent
CONFIG_FILE_PATH = get_path_from_environment("CONFIG_FILE_PATH", os.path.join(PROJECT_PATH, "config.yaml"))
SECRETS_PATH = get_path_from_environment("SECRETS_PATH", os.path.join(PROJECT_PATH, ".secrets"))


class ApplicationBaseSettings(BaseSettings):

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ):
        return (YamlConfigSettingsSource(settings_cls, CONFIG_FILE_PATH, SECRETS_PATH),)


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


def get_yaml_settings_source(yaml_file, secrets_path) -> Dict[str, Any]:
    yaml_file_path = Path(yaml_file)

    if not yaml_file_path.exists():
        raise FileNotFoundError(f"Could not open yaml settings file at: {yaml_file_path}")

    with open(yaml_file, "r") as file:
        yaml_data = yaml.safe_load(file)

    json_data = json.dumps(yaml_data)
    updated_settings_data = update_secrets(secrets_path, json_data)
    return yaml.safe_load(updated_settings_data)


class YamlConfigSettingsSource(PydanticBaseSettingsSource):
    """
    A simple settings source class that loads variables from a Yaml file
    at the project's root.
    """
    
    def __init__(self, settings_cls: Type[BaseSettings], config_yaml_path, secrets_path):
        super().__init__(settings_cls)
        self.config_yaml_path = config_yaml_path
        self.secrets_path = secrets_path
        self.yaml_setting = get_yaml_settings_source(self.config_yaml_path, self.secrets_path)
        
    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> Tuple[Any, str, bool]:
        field_value = self.yaml_setting[field_name]
        is_complex = isinstance(field_value, dict)
        return field_value, field_name, is_complex

    def prepare_field_value(
        self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool
    ) -> Any:
        return value

    def __call__(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {}

        for field_name, field in self.settings_cls.model_fields.items():
            field_value, field_key, value_is_complex = self.get_field_value(
                field, field_name
            )
            field_value = self.prepare_field_value(
                field_name, field, field_value, value_is_complex
            )
            if field_value is not None:
                d[field_key] = field_value

        return d