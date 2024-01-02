import json
import os
import requests

from typing import List

from app.config import get_settings
from app.ws.ejprd_beacon.beacon_dataclasses import OntologyCollection, FilterTerm, ParsedOntology
from app.ws.ejprd_beacon.beacon_dict_utils import BeaconDictUtils
from app.ws.ejprd_beacon.beacon_request_params import MtblsBeaconSchemas
from elasticsearch import Elasticsearch

from app.ws.ejprd_beacon.culler import Culler
from app.ws.ejprd_beacon.parsers.bioportal_parser import BioPortalParser
from app.ws.ejprd_beacon.parsers.ols_parser import OLSParser


class BeaconFramework:
    """"
    Dec 22 2023
    Wrote methods to get all filter terms (IE individual ontology terms like NCIT_123: cancer) and parent ontologies.
    Need to test the api to make sure the response comes in correctly and the copying over from the scratch repo hasnt
    caused any problems. After that, I need to allow for querying by an ontology terms, and return a list of studies
    match it.
    """

    @staticmethod
    def get_filter_terms() -> List[FilterTerm]:
        config = get_settings().beacon.experimental_elasticsearch
        http_auth = (config.user, config.password)
        es_client = Elasticsearch(f'{config.host}:{config.port}', http_auth=http_auth, verify_certs=False)
        index_name = 'study'
        query = {
            "size": 2000,  # Adjust based on how many documents you need
            # "_source": ["studyDesignDescriptors.term.keyword", "studyDesignDescriptors.termAccessionNumber.keyword"]
        }

        # Execute the search query
        response = es_client.search(index=index_name, body=query)

        ontology_objects = {}
        possible_lost_terms = []
        # Process the response
        for doc in response['hits']['hits']:
            study_designs = [BeaconDictUtils.camel_case_keys(d) for d in doc['_source']['studyDesignDescriptors']]
            for dd_dict in study_designs:
                # Process each term with its corresponding accession number
                if dd_dict['termAccessionNumber'] in ['', None]:
                    possible_lost_terms.append(dd_dict['term'])
                else:
                    ontology_objects.setdefault(dd_dict['termAccessionNumber'], dd_dict['term'])
        lost_terms = list(set(possible_lost_terms))
        verified_lost_terms = [term for term in lost_terms if BeaconDictUtils.check(term, ontology_objects) is False]
        print(verified_lost_terms)

        beacon_formatted_filter_term_list = [
            FilterTerm(id=key, label=value) for key, value in ontology_objects.items()
        ]
        return beacon_formatted_filter_term_list

    @staticmethod
    def get_all_metabolights_ontologies() -> List[ParsedOntology]:
        ontologies = BeaconFramework.get_all_referenced_ontologies()
        ols_collection = BeaconFramework.get_ols_ontologies()
        bioportal_collection = BeaconFramework.get_bioportal_ontologies()
        collections = [ols_collection, bioportal_collection]

        intersection = BeaconFramework.cross_ref(elasticsearch_design_descriptors=ontologies, ontology_collections=collections)
        culler = Culler(intersection=intersection)
        culled_ols = culler.cull(collection=ols_collection)
        culled_bioportal = culler.cull(collection=bioportal_collection)
        """"
        I now have the two culled collections of ontologies, i need to parse and merge them, and then return them in the API 
        response. 
        """
        list_of_ontologies = []
        ols_parser = OLSParser()
        bioportal_parser = BioPortalParser()
        list_of_ontologies.extend(ols_parser.parse(culled_ols))
        list_of_ontologies.extend(bioportal_parser.parse(culled_bioportal))
        return list_of_ontologies

    @staticmethod
    def get_all_referenced_ontologies():
        config = get_settings().beacon.experimental_elasticsearch
        http_auth = (config.user, config.password)
        es_client = Elasticsearch(f'{config.host}:{config.port}', http_auth=http_auth, verify_certs=False)


        index_name = 'study'
        query = {
            "size": 0,
            "aggs": {
                "unique_ontology_refs": {
                    "terms": {
                        "field": "ontologySourceReferences.sourceName.keyword",
                        "size": 1000
                    }
                }
            }
        }

        response = es_client.search(index=index_name, body=query)
        unique_ontology_refs = response["aggregations"]["unique_ontology_refs"]["buckets"]
        return unique_ontology_refs

    @staticmethod
    def get_ols_ontologies() -> OntologyCollection:
        """
        Get a list of all ontologies held in
        :return:
        """
        session = requests.Session()
        response = session.get('https://www.ebi.ac.uk/ols4/api/ontologies/?page=0&size=300')
        onts = response.json()['_embedded']['ontologies']

        ols_ont_ids = [ont['ontologyId'] for ont in onts]
        if not os.path.exists("ols.json"):
            with open('ols.json', 'w+') as ols_file:
                json.dump(onts, ols_file)
        ols_collection = OntologyCollection(name='ols', ontologies=onts, ontology_ids=ols_ont_ids)
        return ols_collection

    @staticmethod
    def get_bioportal_ontologies() -> OntologyCollection:
        """
        Get a list of all ontologies held in Bioportal.
        This request takes upward of ten seconds to be resolved. I am considering saving a copy of the response to file.
        :return: an OntologyCollection representing the bioportal ontologies.
        """
        settings = get_settings()
        token = settings.bioportal.api_token
        session = requests.Session()
        params = {
            'apikey': f'{token}',
            'include': 'all'
        }
        response = session.get('http://data.bioontology.org/ontologies_full', params=params)
        bioportal_ontologies = response.json()
        if not os.path.exists('bioportal.json'):
            with open('bioportal.json', 'w+') as bioportal_file:
                json.dump(bioportal_ontologies, bioportal_file)

        bioportal_ontology_ids = [ont['ontology']['acronym'].lower() for ont in bioportal_ontologies]
        bioportal_collection = OntologyCollection(name='bioportal', ontologies=bioportal_ontologies,
                                                  ontology_ids=bioportal_ontology_ids)
        return bioportal_collection

    @staticmethod
    def cross_ref(elasticsearch_design_descriptors: List[dict], ontology_collections: List[OntologyCollection]):
        """
        Using collections of third party ontologies, cull any unrecognisable ontology IDs from the design descriptor list we
        got from our elasticsearch query,
        :param elasticsearch_design_descriptors:
        :param ontology_collections:
        :return:
        """
        mtbls_ontology_ids = [dd['key'].lower() for dd in elasticsearch_design_descriptors]

        unified_third_party_ontology_ids = {id for collection in ontology_collections for id in collection.ontology_ids}

        intersection = list(set(mtbls_ontology_ids) & unified_third_party_ontology_ids)
        print(intersection)
        return intersection

    @staticmethod
    def get_entry_types():
        """
        There is an argument to be made for not just statically defining and returning a dict, but I lifted this
        wholesale from the reference implementation.
        """
        return {
            "analysis": {
                "id": "analysis",
                "name": "Bioinformatics analysis",
                "ontologyTermForThisType": {
                    "id": "edam:operation_2945",
                    "label": "Analysis"
                },
                "partOfSpecification": "Beacon v2.0.0",
                "description": "Apply analytical methods to existing data of a specific type.",
                "defaultSchema": {
                    "id": MtblsBeaconSchemas.ANALYSES.value['schema'],
                    "name": "Default schema for a bioinformatics analysis",
                    "referenceToSchemaDefinition": "https://raw.githubusercontent.com/ga4gh-beacon/beacon-v2/main/models/json/beacon-v2-default-model/analyses/defaultSchema.json",
                    "schemaVersion": "v2.0.0"
                },
                "additionallySupportedSchemas": []
            },
            "biosample": {
                "id": "biosample",
                "name": "Biological Sample",
                "ontologyTermForThisType": {
                    "id": "NCIT:C70699",
                    "label": "Biospecimen"
                },
                "partOfSpecification": "Beacon v2.0.0",
                "description": "Any material sample taken from a biological entity for testing, diagnostic, propagation, treatment or research purposes, including a sample obtained from a living organism or taken from the biological object after halting of all its life functions. Biospecimen can contain one or more components including but not limited to cellular molecules, cells, tissues, organs, body fluids, embryos, and body excretory products. [ NCI ]",
                "defaultSchema": {
                    "id": MtblsBeaconSchemas.BIOSAMPLES.value['schema'],
                    "name": "Default schema for a biological sample",
                    "referenceToSchemaDefinition": "https://raw.githubusercontent.com/ga4gh-beacon/beacon-v2/main/models/json/beacon-v2-default-model/biosamples/defaultSchema.json",
                    "schemaVersion": "v2.0.0"
                },
                "additionallySupportedSchemas": []
            },

            "dataset": {
                "id": "dataset",
                "name": "Dataset",
                "ontologyTermForThisType": {
                    "id": "NCIT:C47824",
                    "label": "Data set"
                },
                "partOfSpecification": "Beacon v2.0.0",
                "description": "A Dataset is a collection of records, like rows in a database or cards in a cardholder.",
                "defaultSchema": {
                    "id": MtblsBeaconSchemas.DATASETS.value['schema'],
                    "name": "Default schema for datasets",
                    "referenceToSchemaDefinition": "https://raw.githubusercontent.com/ga4gh-beacon/beacon-v2/main/models/json/beacon-v2-default-model/datasets/defaultSchema.json",
                    "schemaVersion": "v2.0.0"
                },
                "aCollectionOf": [{"id": "genomicVariation", "name": "Genomic Variants"}],
                "additionalSupportedSchemas": []
            }

        }