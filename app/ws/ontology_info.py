#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Mar-21
#  Modified by:   Jiakang
#
#  Copyright 2019 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
from __future__ import annotations

from enum import Enum
from functools import lru_cache
import json
import logging
import os
import re
import socket
import ssl
import sys
import traceback
from typing import Any, Callable, Dict, List, Set, Union
import urllib
from urllib.parse import quote_plus

import pandas as pd
from flask import current_app as app
from owlready2 import get_ontology, urllib, IRIS
from owlready2.namespace import Ontology
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from app.config import get_settings
from app.utils import current_time, ttl_cache

logger = logging.getLogger('wslog')


class Entity(BaseModel):
        name: str = ""
        iri: str  = "iri"
        onto_name: str  = ""
        provenance_name: str  = ""
        zooma_confidence: str  = ""
        definition: str  = ""
        provenance_uri: str =""


class factor(object):
    def __init__(self, studyID, name, type, iri):
        self.studyID = studyID
        self.name = name
        self.type = type
        self.iri = iri


class Descriptor(object):
    def __init__(self, studyID, design_type, iri):
        self.studyID = studyID
        self.design_type = design_type
        self.iri = iri
        
class MetaboLightsEntity(BaseModel):
    label: str = ""
    iri: str = ""
    description: str = ""
    children: Dict[str, MetaboLightsEntity] = {}

MetaboLightsEntity.model_rebuild()

class FilterType(int, Enum):
    EXACT_MATCH=0
    STARTS_WITH=1
    CONTAINS=2

def filter_term_label(filter_type: FilterType, keyword: str, entity: Entity, case_insensitive: bool=True):
    return filter_term_by_keyword(filter_type=filter_type, keyword=keyword, current_value=entity.name, case_insensitive=case_insensitive)

def filter_term_definition(filter_type: FilterType, keyword: str, entity: Entity, case_insensitive: bool=True):
    return filter_term_by_keyword(filter_type=filter_type, keyword=keyword, current_value=entity.definition, case_insensitive=case_insensitive)
    
def filter_term_by_keyword(filter_type: FilterType, keyword: str, current_value: str, case_insensitive: bool=True):
    if filter_type == FilterType.EXACT_MATCH:
        return keyword.lower() == current_value.lower() if case_insensitive else keyword == current_value
    elif filter_type == FilterType.STARTS_WITH:
        return current_value.lower().startswith(keyword.lower()) if case_insensitive else current_value.startswith(keyword)    
    elif filter_type == FilterType.CONTAINS:
        return keyword.lower() in current_value.lower() if case_insensitive else keyword in current_value
    return False

class DefaultControlLists(BaseModel):
    control_lists: Dict[str, List[Entity]] = {}
    model_config = ConfigDict(alias_generator=to_camel)
    
class MetaboLightsOntology():
    
    OBO_ONTOLOGY_PREFIX = "http://purl.obolibrary.org/obo"
    
    def __init__(self, ontology: Ontology) -> None:
        self.ontology = ontology
        self.label_set: Set[str] = set()
        self.iri_set: Set[str] = set()  
        self.entities: Dict[str, MetaboLightsEntity]  = {}
        self.search_entities: Dict[str, Entity]  = {}
        self.label_map: Dict[str, List[MetaboLightsEntity]] = {}
        self.root_entities: Dict[str, Set[str]]  = {}
        self.branch_map: Dict[str, List[Entity]]  = {}
        self.sorted_search_entities: List[Entity] = []
        self.initiate()
        default_list: DefaultControlLists = DefaultControlLists()
        default_list.control_lists = self.branch_map
        self.control_lists: DefaultControlLists = default_list
        self.control_lists_dict: Dict[str, Any] = self.control_lists.model_dump(by_alias=True)

    def get_default_control_lists(self, name: str=''):
        if name:
            default_list: DefaultControlLists = DefaultControlLists()
            default_list.control_lists={name: self.branch_map[name]}
            return default_list.model_dump(by_alias=True) if name in self.branch_map else {}
        if not self.control_lists.control_lists:
            self.control_lists.control_lists = self.branch_map
            self.control_lists_dict = self.control_lists.model_dump(by_alias=True)
        return self.control_lists_dict

    def get_control_lists_file_path(self):
        file_path = os.path.join(get_settings().server.log.log_path, f"temp_control_list_{socket.gethostname()}.json")
        if not os.path.exists(file_path):
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(self.control_lists_dict, f, indent=4)
        return file_path


    def get_branch_entities(self, name: str) -> List[Entity]:
        if not name:
            return self.sorted_search_entities
        
        if name in self.branch_map:
            return self.branch_map[name]
        return []

    def get_entity_by_iri(self, iri: str) -> List[Entity]:
        if not iri:
            return []
        if iri in self.iri_set:
            return [self.search_entities[iri]]
        return []

    def search_ontology_entities(self, label: str="", branch: str="", include_contain_matches: bool = True, include_case_insensitive_matches: bool = True, limit: int=50, filter_method = filter_term_label) -> List[Entity]:
        entities = self.get_branch_entities(branch)
        
        result = []
        included_items: Set[str]= set()
        if not label:
            result = [x for x in entities if x.iri not in included_items and not included_items.add(x.iri)]
            return result[0:limit] if len(result) > limit else result
        result = [x for x in entities if filter_method(FilterType.EXACT_MATCH, label, x, case_insensitive=False) and x.iri not in included_items and not included_items.add(x.iri)]
        sub_result = [x for x in entities if filter_method(FilterType.STARTS_WITH, label, x, case_insensitive=False) and x.iri not in included_items and not included_items.add(x.iri)]
        sub_result.sort(key=lambda x: x.name)
        result.extend(sub_result)
        if len(result) > limit:
            return result[0:limit]
        if include_case_insensitive_matches:
            sub_result = [x for x in entities if filter_method(FilterType.EXACT_MATCH, label, x, case_insensitive=True)  and x.iri not in included_items and not included_items.add(x.iri)]
            sub_result.sort(key=lambda x: x.name)
            result.extend(sub_result)
            if len(result) > limit:
                return result[0:limit]
            sub_result = [x for x in entities if filter_method(FilterType.STARTS_WITH, label, x, case_insensitive=True) and x.iri not in included_items and not included_items.add(x.iri)]
            sub_result.sort(key=lambda x: x.name)
            result.extend(sub_result)
            if len(result) > limit:
                return result[0:limit]
            if include_contain_matches:
                sub_result = [x for x in entities if filter_method(FilterType.CONTAINS, label, x, case_insensitive=True) and x.iri not in included_items and not included_items.add(x.iri)]
                sub_result.sort(key=lambda x: x.name)
                result.extend(sub_result)
        else:
            if include_contain_matches:
                sub_result =  [x for x in entities if filter_method(FilterType.CONTAINS, label, x, case_insensitive=False)  and x.iri not in included_items and not included_items.add(x.iri)]
                sub_result.sort(key=lambda x: x.name)
                result.extend(sub_result)           
    
        return result[0:limit] if len(result) > limit else result
    
    
    def __add_entity(self, entity, children=None):
        label =  re.sub("\s+", " ", str(entity.label[0]))
        self.iri_set.add(entity.iri)
        self.label_set.add(label)
        if not children:
            children = {}
        mtbls_entity = MetaboLightsEntity(label=label, iri=entity.iri, children=children)
        if label not in self.label_map:
            self.label_map[label] = []
        in_list = False
        
        for item in self.label_map[label]:
            if item.iri == mtbls_entity.iri:
                in_list = True
                break
        if not in_list:
            self.label_map[label].append(mtbls_entity)
        if not entity.iri in self.search_entities:
            self.search_entities[entity.iri] = Entity(name=label, iri=entity.iri, provenance_name='Metabolights', provenance_uri="http://www.ebi.ac.uk/metabolights/ontology")
        if entity.isDefinedBy:
            definition = re.sub("\s+", " ", " ".join(entity.isDefinedBy))
            self.search_entities[entity.iri].definition = definition
            mtbls_entity.description = definition
        
        if 'MTBLS' in entity.iri:
            self.search_entities[entity.iri].onto_name = 'MTBLS'
        else:
            onto_name = ""
            try:
                prefix_list = entity.iri.split("/")
                prefix = ""
                if len(prefix_list) > 1:
                    prefix = "/".join(prefix_list[:-1])
                    if prefix == MetaboLightsOntology.OBO_ONTOLOGY_PREFIX:
                        iri_last_part = prefix_list[-1].split("_")
                        if len(iri_last_part) > 1:
                            onto_name = iri_last_part[0]
                else:      
                    onto_name = get_ontology_name(entity.iri)[0]
            except Exception as ex:
                logger.debug(f"{ex}")
                

            self.search_entities[entity.iri].onto_name = onto_name
            self.search_entities[entity.iri].provenance_name = "Metabolights"
            self.search_entities[entity.iri].provenance_uri="http://www.ebi.ac.uk/metabolights/ontology"
        return mtbls_entity
        
    def initiate(self):
        classes = [cls for cls in self.ontology.classes() ]
        for onto_class in classes:
            if (onto_class.iri and str(onto_class.label[0])) and (onto_class.iri not in self.entities or not self.entities[onto_class.iri].children):
                children = [x for x in self.ontology.search(subclass_of=onto_class) if x.iri != onto_class.iri]
                children_dict: Dict[str, MetaboLightsEntity] = {}            
                for child in children:
                    if child.iri in self.entities:
                        children_dict[child.iri] = self.entities[child.iri]
                    else:
                        children_dict[child.iri] = self.__add_entity(child, None)
                        
                if onto_class.iri not in self.entities:
                    self.entities[onto_class.iri] = self.__add_entity(onto_class, children=children_dict)
                else:
                    self.entities[onto_class.iri].children = children_dict
                

        
        for entity in self.entities.values():
            entity_set: Set[str] =set()
            self.collect_all_child_entities(entity, entity_set)
            if entity_set:
                label = str(entity.label)
                self.root_entities[label] = entity_set
                self.branch_map[label] = []
                for item in entity_set:
                    entity_items= self.label_map[item]
                    for sub_item in entity_items:
                        result = self.search_entities[sub_item.iri]
                        self.branch_map[label].append(result)
                        
                self.branch_map[label].sort(key=lambda x: x.name.lower())
                
                if label in TERM_PRIORITY_MAP:
                    first_priority_terms = TERM_PRIORITY_MAP[label]
                    remaining_terms = [x for x in self.branch_map[label] if x.iri not in first_priority_terms]
                    initial_terms = [x for x in self.branch_map[label] if x.iri in first_priority_terms]
                    initial_terms.sort(key=lambda x: first_priority_terms.index(x.iri))
                    self.branch_map[label] = initial_terms + remaining_terms
        self.sorted_search_entities = sorted(self.search_entities.values(), key=lambda x: x.name)
        # map = IRIS['http://www.geneontology.org/formats/oboInOwl#hasExactSynonym']
        # d = []
        # for cls in classes:
        #     try:
        #         if cls in map:
        #             synonym_list = list(map[cls])
        #             if cls in synonym_list:
        #                 d.append(cls)
        #     except Exception as e:
        #         pass
                
    
    def collect_all_child_entities(self, entity:MetaboLightsEntity,  entity_set: Set[str]):
        for v in entity.children.values():
            label = str(v.label)
            if label:
                entity_set.add(label)
            self.collect_all_child_entities(v, entity_set=entity_set)

PROTOCOL_REF_ORDER = [
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_001100",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_001105",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_001108",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_001109",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_001110",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_001111",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_001112",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_001106",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_001107"
    ]

TERM_PRIORITY_MAP = {
    "Characteristics[Organism]": [
        "http://purl.obolibrary.org/obo/NCBITaxon_9606",
        "http://purl.obolibrary.org/obo/NCBITaxon_10090",
        "http://purl.obolibrary.org/obo/NCBITaxon_10116",
        "http://purl.obolibrary.org/obo/NCBITaxon_3702",
        "http://purl.obolibrary.org/obo/NCBITaxon_29760",
        "http://purl.obolibrary.org/obo/NCBITaxon_562",
        "http://purl.obolibrary.org/obo/NCBITaxon_5691",
        "http://purl.obolibrary.org/obo/ENVO_00002149",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_002304",
        "http://purl.obolibrary.org/obo/MSIO_0000023"
    ],
    "Characteristics[Organism part]": [
        "http://purl.obolibrary.org/obo/BTO_0000131",
        "http://purl.obolibrary.org/obo/BTO_0000133",
        "http://purl.obolibrary.org/obo/BTO_0001419",
        "http://purl.obolibrary.org/obo/BTO_0000440",
        "http://purl.obolibrary.org/obo/BTO_0000713",
        "http://purl.obolibrary.org/obo/BTO_0000733",
        "http://purl.obolibrary.org/obo/NCIT_C13413",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000131",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000125",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_002304",
    ],
    "Characteristics[Sample type]": [
        "http://purl.obolibrary.org/obo/CHMO_0002746",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_001090",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000218",
        "http://purl.obolibrary.org/obo/MSIO_0000026",
        "http://purl.obolibrary.org/obo/MSIO_0000025",
        "http://purl.obolibrary.org/obo/MSIO_0000024"
    ],
    "Study Design Type": [
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000279",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000272",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000218",
        "http://purl.obolibrary.org/obo/CHMO_0000591",
        "http://purl.obolibrary.org/obo/CHMO_0000715",
        "http://purl.obolibrary.org/obo/CHMO_0000497",
        "http://purl.obolibrary.org/obo/CHMO_0000575",
        "http://purl.obolibrary.org/obo/PRIDE_0000461",
        "http://www.ebi.ac.uk/efo/EFO_0004982",
        "http://purl.obolibrary.org/obo/NCIT_C16269",
    ],
    "Study Publication Status": [
        "http://www.ebi.ac.uk/efo/EFO_0001795",
        "http://www.ebi.ac.uk/efo/EFO_0001794",
        "http://www.ebi.ac.uk/efo/EFO_0010558",
        "http://www.ebi.ac.uk/efo/EFO_0001796"
    ],
    "Study Factor Type": [
        "http://www.ebi.ac.uk/efo/EFO_0000724",
        "http://www.ebi.ac.uk/efo/EFO_0000727",
        "http://www.ebi.ac.uk/efo/EFO_0000513",
        "http://www.ebi.ac.uk/efo/EFO_0000246",
        "http://www.ebi.ac.uk/efo/EFO_0002755",
        "http://www.ebi.ac.uk/efo/EFO_0000408",
        "http://www.ebi.ac.uk/efo/EFO_0000410",
        "http://www.ebi.ac.uk/efo/EFO_0000487",
        "http://www.ebi.ac.uk/efo/EFO_0002091",
        "http://www.ebi.ac.uk/efo/EFO_0002090"
    ],
    "LC-MS Protocol REF": PROTOCOL_REF_ORDER,
    "GC-MS Protocol REF": PROTOCOL_REF_ORDER,
    "NMR Protocol REF": PROTOCOL_REF_ORDER,
    "Parameter Value[Mass analyzer]": [
        "http://purl.obolibrary.org/obo/MS_1000484",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000698",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000699",
        "http://purl.obolibrary.org/obo/MS_1000084",
    ],
    "LC-MS Parameter Value[Ion source]": [
        "http://purl.obolibrary.org/obo/MS_1000073",
        "http://purl.obolibrary.org/obo/MS_1000398",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000690",
        "http://purl.obolibrary.org/obo/MS_1000070",
        "http://purl.obolibrary.org/obo/MS_1000075",
        "http://purl.obolibrary.org/obo/MS_1002011",
    ],
    "GC-MS Parameter Value[Ion source]": [
        "http://purl.obolibrary.org/obo/MS_1000389",
        "http://purl.obolibrary.org/obo/MS_1000071",
        "http://purl.obolibrary.org/obo/MS_1000258"
    ],
    "LC-MS Parameter Value[Column type]": [
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000853",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000856",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000855"
    ],
    "GC-MS Parameter Value[Column type]": [
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000991",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000990",
        "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_000989"
    ],
    "Study Person Role": [
        "http://purl.obolibrary.org/obo/NCIT_C42781",
        "http://purl.obolibrary.org/obo/NCIT_C19924",
        "http://purl.obolibrary.org/obo/NCIT_C25936"
    ]
}

def get_ontology_search_result(term, branch, ontologies, mapping, queryFields):
    result = []
    if not term and not branch:
        return result

    if ontologies:  # if has ontology searching restriction
        logger.info('Search ontology %s in' % ontologies)
        print('Searching ontology  %s' % ontologies)
        try:
            result = getOLSTerm(term, mapping, ontologies=ontologies)
            result += getBioportalTerm(term, ontologies=ontologies)

        except Exception as e:
            print(e.args)
            logger.info(e.args)

    else:
        if not queryFields:  # if found the term, STOP
            # is_url = term.startswith('http')
            regex = re.compile(
                r'[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)',
                re.IGNORECASE)

            is_url = term is not None and regex.search(term) is not None

            if is_url and not term.startswith('//') and not term.startswith('http'):
                term = '//' + term

            if is_url and not term.startswith('http:'):
                term = 'http:' + term

            logger.info('Search %s from resources one by one' % term)
            # print('Search %s from resources one by one' % term)
            result = getMetaboTerm(term, branch, mapping)

            branch_mappings = {"instruments": {"root_label": "instrument", "ontology_name": "msio"},
                                "column type": {"root_label": "chromatography", "ontology_name": "chmo"},
                                "unit": {"root_label": "unit", "ontology_name": "uo"},
                                }
            if branch in branch_mappings:
                branch_mapping = branch_mappings[branch]
                search_term = term if term else "*"
                result += OLSbranchSearch(search_term, branch_mapping["root_label"], branch_mapping["ontology_name"])
                
        
            if not result and not is_url:
                # print("Can't find query in MTBLS ontology, search metabolights-zooma.tsv")
                logger.info("Can't find query in MTBLS ontology, search metabolights-zooma.tsv")
                try:
                    result = getMetaboZoomaTerm(term, mapping)
                except Exception as e:
                    print(e.args)
                    logger.info(e.args)

            if not result:
                # print("Can't query it in Zooma.tsv, requesting OLS")
                logger.info("Can't query it in Zooma.tsv, requesting OLS")
                try:
                    result = getOLSTerm(term, mapping, ontology=ontology)
                except Exception as e:
                    print(e.args)
                    logger.info(e.args)

            if not result and not is_url:
                # print("Can't find query in OLS, requesting Zooma")
                logger.info("Can't find query in OLS, requesting Zooma")
                try:
                    result = getZoomaTerm(term)
                except Exception as e:
                    print(e.args)
                    logger.info(e.args)

            if not result:
                # print("Can't query it in Zooma, request Bioportal")
                logger.info("Can't query it in Zooma, request Bioportal")
                try:
                    result = getBioportalTerm(term)
                except Exception as e:
                    # print(e.args)
                    logger.info(e.args)

        else:
            if 'MTBLS' in queryFields:
                result += getMetaboTerm(term, branch, mapping)

            if 'MTBLS_Zooma' in queryFields:
                result += getMetaboZoomaTerm(term, mapping)

            if 'OLS' in queryFields:
                result += getOLSTerm(term, mapping)

            if 'Zooma' in queryFields:
                result += getZoomaTerm(term, mapping)

            if 'Bioportal' in queryFields:
                result += getBioportalTerm(term)

    response = []
    # add WoRMs terms as a entity
    if branch == 'taxonomy':
        r = getWormsTerm(term)
        result += r
    else:
        pass
    
    term = term if term else ""
    exact = []
    starts_with = []
    rest = result
    if term:
        selected = set()
        exact = [x for x in result if x.name.lower() == term.lower() and x.name and x.name not in selected and not selected.add(x.name)]
        starts_with = [x for x in result if x.name and x.name not in selected and x.name.lower().startswith(term.lower()) and not selected.add(x.name)]
        rest = [x for x in result if x.name and x.name not in selected and not selected.add(x.name)]

        # "factor", "role", "taxonomy", "characteristic", "publication", "design descriptor", "unit",
        #                          "column type", "instruments", "confidence", "sample type"
        if branch == 'Study Publication Status':
            priority = {'EFO': 0, 'MTBLS': 10}
        elif branch == 'role':
            priority = {'MTBLS': 0, 'NCIT': 10}
        elif branch == 'Characteristics[Organism]':
            priority = {'NCBITAXON': 0, 'MTBLS': 10, 'WoRMs': 20, 'ENVO': 30, 'EFO': 40, 'NCIT': 50, 'MSOI': 60}
        elif branch == 'Characteristics[Organism part]':
            priority = {'BTO': 0, 'MTBLS': 10, 'EFO': 20, 'NCIT': 30, 'PO': 40, 'ENVO': 50, 'CHEBI': 60}
        elif branch == 'Characteristics[Sample type]':
            priority = {'CHMO': 0, 'MTBLS': 10, 'MSIO': 20}
        elif branch == 'unit':
            priority = {'UO': 0, 'MTBLS': 10, "EFO": 30, "NCIT": 40}
        elif branch == 'factor' or 'Study Factor Type':
            priority = {'EFO': 0, 'NCIT': 1, 'ENVO': 2, 'MTBLS': 3, 'CHEBI': 4, 'CHMO': 5, 'MESH': 6, 'PO': 7}
        elif branch == 'design descriptor' or 'Study Design Type':
            priority = {'EFO': 1, 'NCIT': 2, 'MTBLS': 3, 'CHEBI': 4, 'CHMO': 5, 'GO': 6, 'MESH': 7}
        else:
            priority = {'EFO': 10, 'NCBITAXON': 20, 'UO': 25, 'BTO': 30, 'MTBLS': 0,  'CHMO': 50, 'NCIT': 60, 'PO': 70, 'CHEBI': 40,}

        prioritrised_ontology_names = { x.upper():priority[x] for x in priority}
        exact = sort_terms_by_priority(exact, prioritrised_ontology_names)
        starts_with = sort_terms_by_priority(starts_with, prioritrised_ontology_names)
        rest = sort_terms_by_priority(rest, prioritrised_ontology_names)
        # rest = reorder(rest, term)
        result = exact + starts_with + rest
    result = remove_duplicated_terms(result)

    # result = remove_duplicated_terms(result)

    for cls in result:
        temp = '''    {
                        "comments": [],
                        "annotationValue": "",
                        "annotationDefinition": "", 
                        "termAccession": "",
                        "wormsID": "", 
                        
                        "termSource": {
                            "comments": [],
                            "name": "",
                            "file": "",
                            "provenanceName": "",
                            "version": "",
                            "description": ""
                        }                            
                    }'''

        d = json.loads(str(temp))
        try:
            d['annotationValue'] = cls.name
            d["annotationDefinition"] = cls.definition
            if branch == 'taxonomy':
                d['wormsID'] = cls.iri.rsplit('id=', 1)[-1]
            d["termAccession"] = cls.iri
            d['termSource']['name'] = cls.onto_name
            d['termSource']['provenanceName'] = cls.provenance_name
            d['termSource']['file'] = cls.provenance_uri

            if cls.onto_name == 'MTBLS':
                d['termSource']['file'] = 'https://www.ebi.ac.uk/metabolights/ontology'
                d['termSource']['provenanceName'] = 'Metabolights'
                d['termSource']['version'] = '1.0'
                d['termSource']['description'] = 'Metabolights Ontology'
        except Exception as e:
            pass

        if cls.provenance_name == 'MTBLS_Zooma':
            d['termSource']['version'] = str(current_time().date())
        response.append(d)

    # response = [{'SubClass': x} for x in res]
    # print('--' * 30)
    return {"OntologyTerm": response}

@lru_cache(1)
def load_ontology_file(filepath)-> MetaboLightsOntology:
    try:
        ontology = get_ontology(filepath).load()
        if ontology:
            return MetaboLightsOntology(ontology=ontology)
    except Exception as ex:
        print(f"Error loading ontology file. {str(ex)}")
        print(traceback.format_exc())
        logger.info("Fail to load ontology from {path}. {ex}".format(path=filepath, ex=str(ex)))
    
    return None

def initiate_mtbls_model(): 
    file = get_settings().file_resources.mtbls_ontology_file
    load_ontology_file(file)

initiate_mtbls_model()

@ttl_cache(2048)
def getMetaboTerm(keyword, branch, mapping='', limit=100):
    file = get_settings().file_resources.mtbls_ontology_file
    mtbls_ontology: MetaboLightsOntology = load_ontology_file(file)
    if not mtbls_ontology:
        return []
    # onto = mtbls_ontology.ontology

    # result = []
    # cls = []
    search_result: List[Entity] = []
    if keyword:
        include_contain_matches =  mapping != 'exact'
        search_result = mtbls_ontology.get_entity_by_iri(keyword)
        if not search_result:
            search_result = mtbls_ontology.search_ontology_entities(keyword, branch=branch, include_case_insensitive_matches=True, include_contain_matches=include_contain_matches, limit=limit)

        return search_result
    
        # try:
        #     cls += onto.search(label=keyword, _case_sensitive=False)
        # except:
        #     logger.info("Can't find {term} in MTBLS ontology, continue...".format(term=keyword))
        #     pass

        # if mapping != 'exact':
        #     # fuzzy match
        #     try:
        #         # cls += onto.search(label=keyword + '*', _case_sensitive=False)
        #         cls += onto.search(label='*' + keyword + "*", _case_sensitive=False)
                
        #     except Exception as ex:
        #         logger.info("Can't find terms similar with {term} in MTBLS ontology, continue...".format(term=keyword))
        #         print("Can't find terms similar with {term} in MTBLS ontology, continue...".format(term=keyword))

        # if not cls:
        #     return []

        # if branch:  # term = 1 , branch = 1, search branch
        #     try:
        #         sup = onto.search_one(label=branch, _case_sensitive=False)
        #         logger.info("Search {term} in MTBLS ontology {branch}".format(term=keyword, branch=branch))
        #     except:
        #         logger.info("Can't find a branch called " + branch)
        #         return []

        #     subs = sup.descendants()
        #     try:
        #         subs.remove(sup)
        #     except:
        #         pass
        #     result += list(set(subs) & set(cls))

        #     # synonym match
        #     if branch == 'taxonomy' or branch == 'factors':
        #         for cls in subs:
        #             try:
        #                 map = IRIS['http://www.geneontology.org/formats/oboInOwl#hasExactSynonym']
        #                 Synonym = list(map[cls])
        #                 if keyword.lower() in [syn.lower() for syn in Synonym]:
        #                     result.append(cls)
        #             except Exception as e:
        #                 pass

        # else:  # term =1 branch = 0, search whole ontology
        #     result += cls

    else:  # term = None
        logger.debug("Search Metabolights ontology whole {branch} branch ... ".format(branch=branch))
        entities = mtbls_ontology.get_branch_entities(branch)
        return entities[:limit] if len(entities) > limit else entities        
        # if branch:  # term = 0, branch = 1, return whole ontology branch
        #     logger.info("Search Metabolights ontology whole {branch} branch ... ".format(branch=branch))
        #     entities = mtbls_ontology.get_branch_entities(branch)
        #     return entities[:20] if len(entities) > 20 else entities
        #     # try:
        #     #     sup = onto.search_one(label=branch, _case_sensitive=False)
        #     #     sub = sup.descendants()
        #     #     try:
        #     #         sub.remove(sup)
        #     #     except:
        #     #         pass

        #     #     result += sub
                
        #     #     # Change entity priority
        #     #     if branch == 'design descriptor' and not keyword:

        #     #         initial_ontology_terms = [272, 279, 69, 42, 40, 225, 54,]
        #     #         mtbls_ontology_iri_prefix = "http://www.ebi.ac.uk/metabolights/ontology/MTBLS_"
        #     #         first_priority_terms = [f"{mtbls_ontology_iri_prefix}{x:06}" for x in initial_ontology_terms]
        #     #         remaining_terms = [x for x in result if x.iri not in first_priority_terms]
        #     #         initial_terms = [x for x in result if x.iri in first_priority_terms]
        #     #         initial_terms.sort(key=lambda x: first_priority_terms.index(x.iri))
        #     #         result = initial_terms + remaining_terms

        #     #     result = result[:20]

        #     # except Exception as e:
        #     #     print(e)
        #     #     logger.info("Can't find a branch called " + branch)
        #     #     print("Can't find a branch called " + branch)
        #     #     return []
        # else:  # term = None, branch = None
        #     return []

    # res = []

    # for cls in result:
    #     enti = Entity(name=cls.label[0], iri=cls.iri,
    #                   provenance_name='Metabolights')

    #     if cls.isDefinedBy:
    #         enti.definition = cls.isDefinedBy[0]

    #     if 'MTBLS' in cls.iri:
    #         enti.onto_name = 'MTBLS'

    #     else:
    #         try:
    #             onto_name = get_ontology_name(enti.iri)[0]
    #         except:
    #             onto_name = ''

    #         enti.onto_name = onto_name
    #         enti.provenance_name = onto_name

    #     res.append(enti)

    # return res

@ttl_cache(1024)
def getOLSTerm(keyword, map, ontologies=''):
    logger.info('Requesting OLS...')
    print('Requesting OLS...')
    res = []

    if not keyword:
        return res

    elif 'http:' in keyword:
        label, definition, onto_name = getOLSTermInfo(keyword)
        if len(definition) > 0:
            definition = definition[0]
        else:
            definition = ''
        if len(label) > 1:
            base_url = get_settings().external_dependencies.api.ols_api_url
            enti = Entity(name=label, iri=keyword, definition=definition, onto_name=onto_name,
                          provenance_name="OLS", provenance_uri=base_url)
            res.append(enti)
        return res

    try:
        # https://www.ebi.ac.uk/ols4/api/search?q=lung&queryFields=label,synonym&fieldList=iri,label,short_form,obo_id,ontology_name,ontology_prefix
        uri = 'search?q=' + keyword.replace(' ', "+") + \
              '&queryFields=label,synonym' \
              '&type=class' \
              '&fieldList=iri,label,short_form,ontology_name,description,ontology_prefix' \
              '&rows=30'  # &exact=true
        url = os.path.join(get_settings().external_dependencies.api.ols_api_url, uri)
        if map == 'exact':
            url += '&exact=true'

        if ontologies:
            onto_list = ",".join([x.strip() for x in ontologies.split(",")])
            url += '&ontology=' + onto_list

        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)
        responses = j_content["response"]['docs']
        ontologies_uppercase = ontologies.upper().split(",")
        for term in responses:
            # name = ' '.join([w.capitalize() if w.islower() else w for w in term['label'].split()])

            name = term['label']
            try:
                definition = term['description'][0]
            except:
                definition = ''

            try:
                onto_name, _ = get_ontology_name(term['iri'])
            except:
                onto_name = ''
            if onto_name.upper() in ontologies_uppercase:
                base_url = get_settings().external_dependencies.api.ols_api_url
                enti = Entity(name=name, iri=term['iri'], definition=definition, onto_name=onto_name,
                            provenance_name="OLS", provenance_uri=base_url)
                res.append(enti)
            if len(res) >= 20:
                break

    except Exception as e:
        print(e.args)
        logger.error('getOLS' + str(e))
    return res

@ttl_cache(256)
def getStartIRI(start, onto_name):
    uri = 'search?q=' + start + '&ontology=' + onto_name + '&queryFields=label'
    url = os.path.join(get_settings().external_dependencies.api.ols_api_url, uri)
    fp = urllib.request.urlopen(url, 5)
    content = fp.read().decode('utf-8')
    json_str = json.loads(content)
    res = json_str['response']['docs'][0]['iri']
    return urllib.parse.quote_plus(res)

@ttl_cache(1024)
def OLSbranchSearch(keyword, branch_name, onto_name):
    res = []
    if not keyword:
        return res
    try: 
        branchIRI = getStartIRI(branch_name, onto_name)
        keyword = keyword.replace(' ', '%20')
        uri = 'search?q=' + keyword + '&rows=10&ontology=' + onto_name + '&allChildrenOf=' + branchIRI
        base_url = get_settings().external_dependencies.api.ols_api_url
        url = os.path.join(base_url, uri)
        # print(url)
        fp = urllib.request.urlopen(url, timeout=5)
        content = fp.read().decode('utf-8')
        json_str = json.loads(content)
        
        for ele in json_str['response']['docs']:
            enti = Entity(name=ele['label'], iri=ele['iri'], onto_name=onto_name, provenance_name="OLS", provenance_uri=base_url)
            res.append(enti)
            
        return res
    except Exception as ex: 
        logger.debug(f"{str(ex)}")
        return []

@lru_cache(1)
def get_metabo_zooma_terms_dataframe() -> pd.DataFrame:
    fileName = get_settings().file_resources.mtbls_zooma_file # metabolights_zooma.tsv
    df = pd.read_csv(fileName, sep="\t", header=0, encoding='utf-8')
    df = df.drop_duplicates(subset='PROPERTY_VALUE', keep="last")
    return df

@ttl_cache(1024)
def getMetaboZoomaTerm(keyword, mapping):
    logger.info('Searching Metabolights-zooma.tsv')
    print('Searching Metabolights-zooma.tsv')
    res = []

    if keyword in [None, '']:
        return res

    try:
        df = get_metabo_zooma_terms_dataframe()
        if mapping == 'exact':
            temp = df.loc[df['PROPERTY_VALUE'].str.lower() == keyword.lower()]
        else:
            temp1 = df.loc[df['PROPERTY_VALUE'].str.lower() == keyword.lower()]
            reg = "^" + keyword + "+"
            temp2 = df.loc[df['PROPERTY_VALUE'].str.contains(reg, case=False)]
            frame = [temp1, temp2]
            temp = pd.concat(frame).reset_index(drop=True)

        temp = temp.drop_duplicates(subset='PROPERTY_VALUE', keep="last", inplace=False)

        for i in range(len(temp)):
            iri = temp.iloc[i]['SEMANTIC_TAG']
            # name = ' '.join(
            #     [w.capitalize() if w.islower() else w for w in temp.iloc[i]['PROPERTY_VALUE'].split()])

            name = temp.iloc[i]['PROPERTY_VALUE']
            obo_ID = iri.rsplit('/', 1)[-1]

            enti = Entity(name=name,
                          iri=iri,
                          provenance_name='MTBLS_Zooma',
                          provenance_uri='metabolights_zooma.tsv',
                          zooma_confidence='High')

            try:
                enti.onto_name, enti.definition = get_ontology_name(iri)
            except:
                enti.onto_name = 'MTBLS'

            res.append(enti)
    except Exception as e:
        logger.error('Fail to load metabolights-zooma.tsv' + str(e))

    return res

@ttl_cache(1024)
def getZoomaTerm(keyword, mapping=''):
    logger.info('Requesting Zooma...')
    print('Requesting Zooma...')
    res = []

    if keyword in [None, '']:
        return res

    try:
        # url = 'http://snarf.ebi.ac.uk:8480/spot/zooma/v2/api/services/annotate?propertyValue=' + keyword.replace(' ',"+")
        uri = 'services/annotate?propertyValue=' + keyword.replace(' ', "+")
        base_url = get_settings().external_dependencies.api.zooma_api_url
        url = os.path.join(base_url, uri)
        ssl._create_default_https_context = ssl._create_unverified_context
        fp = urllib.request.urlopen(url, timeout=5)
        content = fp.read().decode('utf8')
        json_str = json.loads(content)
        for term in json_str:
            iri = term['semanticTags'][0]

            # name = ' '.join(
            #     [w.capitalize() if w.islower() else w for w in term["annotatedProperty"]['propertyValue'].split()])

            name = term["annotatedProperty"]['propertyValue']

            if mapping == 'exact' and name != keyword:
                continue

            enti = Entity(name=name,
                          iri=iri,
                          zooma_confidence=term['confidence'])

            if enti.onto_name == '':
                enti.onto_name, enti.definition = get_ontology_name(iri)

            try:
                enti.provenance_name = term['derivedFrom']['provenance']['source']['name']
            except:
                enti.provenance_name = "zooma"

            try:
                enti.provenance_uri = term['derivedFrom']['provenance']['source']['uri']
            except:
                enti.provenance_uri = "http://www.ebi.ac.uk/spot/zooma"
            if enti.provenance_name == 'Metabolights':
                res = [enti] + res
            else:
                res.append(enti)

            if len(res) >= 10:
                break
    except Exception as e:
        logger.error('getZooma' + str(e))
    return res

@ttl_cache(1024)
def getBioportalTerm(keyword, ontologies=''):
    logger.info('Requesting Bioportal...')
    print('Requesting Bioportal...')
    res = []

    if keyword in [None, '']:
        return res

    try:
        if 'http:' in keyword:
            uri = 'search?q=' + keyword.replace(' ', "+") + '&require_exact_match=true'
        else:
            uri = 'search?q=' + keyword.replace(' ', "+")
        base_url = get_settings().external_dependencies.api.bioontology_api_url
        url = os.path.join(base_url, uri)

        onto_list = ','.join([x.strip().upper() for x in ontologies.split(",")])
        url += '&ontologies=' + onto_list + '&require_exact_match=true'

        request = urllib.request.Request(url)
        request.add_header('Authorization', 'apikey token=' + get_settings().bioportal.api_token)
        response = urllib.request.urlopen(request)
        content = response.read().decode('utf-8')
        j_content = json.loads(content)

        iri_record = []

        for term in j_content['collection']:
            iri = term['@id']
            if iri in iri_record:
                continue

            try:
                onto_name = term['links']['ontology'].split('/')[-1]
            except:
                onto_name = get_ontology_name(iri)[0]

            enti = Entity(name=term['prefLabel'],
                          iri=iri,
                          onto_name=onto_name, provenance_name="Bioportal", provenance_uri=base_url)
            res.append(enti)
            iri_record.append(iri)
            if len(res) >= 10:
                break
    except Exception as e:
        logger.error('getBioportal' + str(e))
    return res

@ttl_cache(1024)
def getWormsTerm(keyword):
    logger.info('Requesting WoRMs ...')
    print('Requesting WoRMs ...')

    res = []
    if keyword in [None, '']:
        return res

    try:
        uri = 'AphiaRecordsByName/{keyword}?like=true&marine_only=true&offset=1'.format(
            keyword=keyword.replace(' ', '%20'))
        base_url = get_settings().external_dependencies.api.marine_species_api_url
        url = os.path.join(base_url, uri)
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)

        for term in j_content:
            if term and "scientificname" in term and term["scientificname"] and "url" in term and term["url"]:
                name = term["scientificname"]
                iri = term["url"]
                definition = term["authority"]
                onto_name = 'WoRMS'
                provenance_name = 'WoRMS'

                enti = Entity(name=name, iri=iri, definition=definition, onto_name=onto_name,
                              provenance_name=provenance_name, provenance_uri=base_url)
                res.append(enti)

            if len(res) >= 10:
                break
    except Exception as e:
        logger.error(str(e))

    return res

@ttl_cache(512)
def getWoRMsID(term):
    try:
        uri = 'AphiaIDByName/' + term.replace(' ', '%20') + "?marine_only=true"
        url = os.path.join(get_settings().external_dependencies.api.marine_species_api_url, uri)
        fp = urllib.request.urlopen(url)
        AphiaID = fp.read().decode('utf-8')
        if AphiaID != '-999':
            return AphiaID
        return ''
    except:
        return ''

@ttl_cache(512)
def getOLSTermInfo(iri):
    # enti = Entity(name=name, iri=term['iri'], definition=definition, onto_name=onto_name, provenance_name=provenance_name)

    try:
        uri = 'terms/findByIdAndIsDefiningOntology?iri=' + iri
        base_url = get_settings().external_dependencies.api.ols_api_url
        url = os.path.join(base_url, uri)
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)

        label = j_content['_embedded']['terms'][0]['label']
        definition = j_content['_embedded']['terms'][0]["description"]
        if definition is None:
            definition = ''
        onto_name = j_content['_embedded']['terms'][0]['ontology_prefix']

        return label, definition, onto_name
    except:
        return '', '', ''

@ttl_cache(512)
def getOnto_info(pre_fix):
    '''
     get ontology information include  "name", "file", "version", "description"
     :param pre_fix: ontology prefix
     :return: "ontology iri", "version", "ontology description"
     '''
    try:
        uri = 'ontologies/' + pre_fix
        url = os.path.join(get_settings().external_dependencies.api.ols_api_url, uri)
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)

        iri = j_content['config']['id']
        version = j_content['config']['version']
        description = j_content['config']['title']
        return iri, version, description
    except:
        if pre_fix == "MTBLS":
            return 'http://www.ebi.ac.uk/metabolights/ontology', '1.0', 'EBI Metabolights ontology'
        return '', '', ''

@ttl_cache(1024)
def get_ontology_name(iri):
    # get ontology name by giving iri of entity
    try:
        uri = 'terms/findByIdAndIsDefiningOntology?iri=' + iri
        url = os.path.join(get_settings().external_dependencies.api.ols_api_url, uri)
        fp = urllib.request.urlopen(url, timeout=5)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)
        try:
            return j_content['_embedded']['terms'][0]['ontology_prefix'], \
                   j_content['_embedded']['terms'][0]['description'][0]
        except:
            return j_content['_embedded']['terms'][0]['ontology_prefix'], ''
    except:
        if 'MTBLS' in iri:
            return 'MTBLS', 'Metabolights'
        elif 'BAO' in iri:
            return 'BAO', ''
        else:
            substring = iri.rsplit('/', 1)[-1]
            if '_' in substring:
                substring = substring.rsplit('_')[0]
            return ''.join(x for x in substring if x.isalpha()), ''

def term_sort_key(term: Entity, ontology_priority_map):
    priority = ontology_priority_map.get(term.onto_name.upper(), 100000)
    return f"{priority:08}:{term.name.lower()}"

def sort_terms_by_priority(res_list: List[Entity], ontology_priority_map):
    res = sorted(res_list, key=lambda x: term_sort_key(x, ontology_priority_map))
    return res


def reorder(res_list, keyword):
    def sort_key(s, keyword):
        try:
            exact = s.lower() == keyword.lower()
        except:
            exact = False

        try:
            start = s.startswith(keyword)
        except:
            start = False
        try:
            partial = keyword in s
        except:
            partial = False

        return exact, start, partial

    try:
        res = sorted(res_list, key=lambda x: sort_key(x.name, keyword), reverse=True)
        return res
    except:
        return res_list


def remove_duplicated_terms(res_list):
    iri_pool = set()
    
    new_list = []
    for res in res_list:
        if res.iri not in iri_pool:
            iri_pool.add(res.iri)
            new_list.append(res)
            
    return new_list


def getDescriptionURL(onto_name, iri):
    ir = quote_plus(quote_plus(iri))
    uri = 'ontologies/' + onto_name + '/terms/' + ir
    url = os.path.join(get_settings().external_dependencies.api.ols_api_url, uri)
    return url


if __name__ == "__main__":
    filepath = get_settings().file_resources.mtbls_ontology_file
    ontology_input = load_ontology_file(filepath)
    ontology_model = MetaboLightsOntology(ontology=ontology_input)
    