from pydantic_settings import BaseSettings, SettingsConfigDict



class MetabolightsBaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file_encoding='utf-8', env_file=".env")