from typing import Any, Optional

from app.config import get_settings, ApplicationSettings
from app.ws.ejprd_beacon.beacon_request_params import Granularity, RequestParams, MtblsBeaconSchemas
import flask


def build_beacon_info_response(data, beacon_request_object: RequestParams, func_response_type = None, authorized_datasets=None):
    if authorized_datasets is None:
        authorized_datasets = []

    config = get_settings()
    #beacon_request_object = RequestParams().from_request(request)
    beacon_response = {
        'meta': build_meta(config, beacon_request_object, None, Granularity.RECORD),
        'response': {
            'id': config.beacon.beacon_id,
            'name': config.beacon.beacon_name,
            'apiVersion': config.beacon.api_version,
            'environment': config.beacon.environment,
            'organization': {
                'id': config.beacon.org_id,
                'name': config.beacon.org_name,
                'description': config.beacon.org_description,
                'address': config.beacon.org_address,
                'welcomeUrl': config.beacon.org_welcome_url,
                'contactUrl': config.beacon.org_contact_url,
                'logoUrl': config.beacon.org_logo_url,
            },
            'description': config.beacon.description,
            'version': config.beacon.version,
            'welcomeUrl': config.beacon.welcome_url,
            'alternativeUrl': config.beacon.alternative_url,
            'createDateTime': config.beacon.create_datetime,
            'updateDateTime': config.beacon.update_datetime,
            'datasets': func_response_type(data, beacon_request_object, authorized_datasets),
        }
    }

    return beacon_response


def build_meta(config: ApplicationSettings, beacon_request: RequestParams, entity_schema: Optional[MtblsBeaconSchemas], returned_granularity: Granularity):
    """"Builds the `meta` part of the response

    We assume that receivedRequest is the evaluated request (qparams) sent by the user.
    """

    meta = {
        'beaconId': config.beacon.beacon_id,
        'apiVersion': config.beacon.api_version,
        'returnedGranularity': returned_granularity,
        'receivedRequestSummary': beacon_request.summary(),
        'returnedSchemas': [entity_schema.value] if entity_schema is not None else []
    }
    return meta