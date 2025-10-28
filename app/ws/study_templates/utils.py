from typing import Any

from app.utils import ttl_cache
from app.ws.isa_table_templates import get_json_from_policy_service
from app.ws.study_templates.models import (
    FileTemplates,
    ValidationConfiguration,
    ValidationControls,
)


def get_all_templates() -> dict[str, Any]:
    templates_base_path = "/v1/data/metabolights/validation/v2/templates"
    context_path = f"{templates_base_path}"
    return get_json_from_policy_service(context_path=context_path)


def get_all_controls() -> dict[str, Any]:
    templates_base_path = "/v1/data/metabolights/validation/v2/controls"
    context_path = f"{templates_base_path}"
    return get_json_from_policy_service(context_path=context_path)


@ttl_cache(1024, 60 * 5)
def get_validation_configuration() -> ValidationConfiguration:
    controls = get_all_controls()
    templates = get_all_templates()
    return ValidationConfiguration(
        controls=ValidationControls.model_validate(controls.get("result"), by_alias=True),
        templates=FileTemplates.model_validate(templates.get("result"), by_alias=True),
    )
