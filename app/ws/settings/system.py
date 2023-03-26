
from app.ws.settings.base import MetabolightsBaseSettings


class SystemSettings(MetabolightsBaseSettings):
    integration_test_period_in_seconds: int = 300
    technical_issue_recipient_email: str = "metabolights-dev@ebi.ac.uk"
    metabolights_apitoken: str
    es_compound_sync_task_period_in_secs: int = 60 * 60
    es_study_sync_task_period_in_secs: int = 60 * 10
    study_folder_maintenance_task_period_in_secs: int = 60 * 10
    banner_message_key: str = "metabolights:banner:message"
    
