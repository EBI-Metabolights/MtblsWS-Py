
from app.ws.settings.base import MetabolightsBaseSettings


class SystemSettings(MetabolightsBaseSettings):
    integration_test_period_in_seconds: int = 300
    technical_issue_recipient_email: str = "metabolights-dev@ebi.ac.uk"
    
