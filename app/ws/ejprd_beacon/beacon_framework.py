from app.config import get_settings
from app.ws.ejprd_beacon.beacon_request_params import MtblsBeaconSchemas
from elasticsearch import Elasticsearch


class BeaconFramework:


    @staticmethod
    def get_filter_terms():
        config = get_settings().beacon.experimental_elasticsearch
        http_auth = (config.user, config.password)
        es_client = Elasticsearch(f'{config.host}:{config.port}', http_auth=http_auth, verify_certs=False)
        index_name = 'study'
        query = {
            "size": 0,
            "aggs": {
                "unique_study_designs": {
                    "terms": {
                        "field": "StudyDesignDescriptors",
                        "size": 10000
                    }
                }
            }
        }
        # Execute the search query
        response = es_client.search(index=index_name, body=query)

        # Extracting the aggregation results
        unique_study_designs = response["aggregations"]["unique_study_designs"]["buckets"]

        # Print the results
        for design in unique_study_designs:
            print(design['key'], design['doc_count'])
        return unique_study_designs

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