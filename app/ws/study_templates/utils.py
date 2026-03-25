from typing import Any

from cachetools import TTLCache, cached

from app.ws.isa_table_templates import get_json_from_policy_service
from app.ws.study_templates.models import (
    FileTemplates,
    TemplateSettings,
    ValidationConfiguration,
    ValidationControls,
)


@cached(cache=TTLCache(maxsize=1024, ttl=60))
def get_all_templates() -> dict[str, Any]:
    templates_base_path = "/v1/data/metabolights/validation/v2/templates"
    context_path = f"{templates_base_path}"
    return get_json_from_policy_service(context_path=context_path)


@cached(cache=TTLCache(maxsize=1024, ttl=60))
def get_all_controls() -> dict[str, Any]:
    templates_base_path = "/v1/data/metabolights/validation/v2/controls"
    context_path = f"{templates_base_path}"
    return get_json_from_policy_service(context_path=context_path)


@cached(cache=TTLCache(maxsize=1024, ttl=60))
def get_validation_configuration() -> ValidationConfiguration:
    controls = get_all_controls()
    templates = get_all_templates()
    return ValidationConfiguration(
        controls=ValidationControls.model_validate(
            controls.get("result"), by_alias=True
        ),
        templates=FileTemplates.model_validate(templates.get("result"), by_alias=True),
    )


@cached(cache=TTLCache(maxsize=1, ttl=60))
def get_template_settings() -> TemplateSettings:
    context_path = "/v1/data/metabolights/validation/v2/templates/configuration"
    response = get_json_from_policy_service(context_path=context_path)
    return TemplateSettings.model_validate(response.get("result"), by_alias=True)


# if __name__ == "__main__":
#     settings = get_template_settings()
#     print(settings)
