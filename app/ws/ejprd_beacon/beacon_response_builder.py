from typing import Any, Optional

from app.config import get_settings, ApplicationSettings, BeaconConfiguration
from app.ws.ejprd_beacon.beacon_framework import BeaconFramework
from app.ws.ejprd_beacon.beacon_request_params import Granularity, RequestParams, MtblsBeaconSchemas
import flask


class BeaconResponseBuilder:

    @staticmethod
    def build_beacon_info_response(data, beacon_request_object: RequestParams, func_response_type = None, authorized_datasets=None):
        """
        Give a general overview of the beacon. Takes in a beacon request object. Returns Mtbls study accessions as
        datasets.
        """
        if authorized_datasets is None:
            authorized_datasets = []

        config = get_settings()
        #beacon_request_object = RequestParams().from_request(request)
        beacon_response = {
            'meta': BeaconResponseBuilder.build_meta(config, beacon_request_object, None, Granularity.RECORD.value),
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

    @staticmethod
    def build_beacon_service_info_response(conf: BeaconConfiguration):
        """
        Return basic service information.
        """

        beacon_response = {
            'id': conf.beacon_id,
            'name': conf.beacon_name,
            'type': {
                'group': conf.ga4gh_service_type_group,
                'artifact': conf.ga4gh_service_type_artifact,
                'version': conf.ga4gh_service_type_version
            },
            'description': conf.description,
            'organization': {
                'name': conf.org_name,
                'url': conf.org_welcome_url
            },
            'contactUrl': conf.org_contact_url,
            'documentationUrl': conf.documentation_url,
            'createdAt': conf.create_datetime,
            'updatedAt': conf.update_datetime,
            'environment': conf.environment,
            'version': conf.version,
        }
        return beacon_response

    @staticmethod
    def build_configuration_response(conf: BeaconConfiguration, map: bool = False):
        """
        Build a .json document outlining the current metadata and beacon response configuration.
        """
        meta = {
            '$schema': 'https://raw.githubusercontent.com/ga4gh-beacon/beacon-framework-v2/main/responses/sections/beaconInformationalResponseMeta.json',
            'beaconId': conf.beacon_id,
            'apiVersion': conf.api_version,
            'returnedSchemas': []
        }

        response = {
            '$schema': 'https://raw.githubusercontent.com/ga4gh-beacon/beacon-framework-v2/main/configuration/beaconConfigurationSchema.json',
            'maturityAttributes': {
                'productionStatus': 'DEV'
            },
            'securityAttributes': {
                'defaultGranularity': 'record',
                'securityLevels': ['PUBLIC', 'REGISTERED', 'CONTROLLED']
            },
            'entryTypes': BeaconFramework.get_entry_types()
        }

        configuration_json = {
            'meta': meta,
            'response': response
        }
        configuration_json.update({'$schema': 'https://raw.githubusercontent.com/ga4gh-beacon/beacon-framework-v2/'
                                              'main/responses/beaconConfigurationResponse.json'}) if not map else None
        return configuration_json

    @staticmethod
    def build_entry_type_response():
        return BeaconFramework.get_entry_types()

    @staticmethod
    def build_filtering_terms_response():

        return {'terms': BeaconFramework.get_filter_terms()}

    @staticmethod
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
