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

from functools import lru_cache
import json
import logging
import os
import ssl
import urllib
from urllib.parse import quote_plus

import pandas as pd
from flask import current_app as app
from owlready2 import get_ontology, urllib, IRIS

from app.config import get_settings

logger = logging.getLogger('wslog')


class entity(object):
    def __init__(self, name, iri='', ontoName='', provenance_name='', provenance_uri='',
                 Zooma_confidence='', definition=''):
        self.name = name
        self.iri = iri
        self.ontoName = ontoName
        self.provenance_name = provenance_name
        self.Zooma_confidence = Zooma_confidence
        self.definition = definition


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

@lru_cache(1)
def load_ontology_file(filepath):
    try:
        return get_ontology(filepath).load()
    except:
        logger.info("Fail to load ontology from {path}".format(path=filepath))
        return []
    
def getMetaboTerm(keyword, branch, mapping=''):
    file = get_settings().file_resources.mtbls_ontology_file
    try:
        onto = load_ontology_file(file)
        if not onto:
            return []
    except:
        logger.info("Fail to load ontology from {path}".format(path=file))
        return []

    result = []
    cls = []

    if keyword:
        # exact match
        if keyword.startswith('http'):
            try:
                cls += onto.search(iri=keyword)
            except:
                logger.info("Can't find {term} in MTBLS ontology, continue...".format(term=keyword))
                pass

        try:
            cls += onto.search(label=keyword, _case_sensitive=False)
        except:
            logger.info("Can't find {term} in MTBLS ontology, continue...".format(term=keyword))
            pass

        if mapping != 'exact':
            # fuzzy match
            try:
                # cls += onto.search(label=keyword + '*', _case_sensitive=False)
                cls += onto.search(label='*' + keyword + '*', _case_sensitive=False)
            except:
                logger.info("Can't find terms similar with {term} in MTBLS ontology, continue...".format(term=keyword))
                print("Can't find terms similar with {term} in MTBLS ontology, continue...".format(term=keyword))

        if not cls:
            return []

        if branch not in [None, '']:  # term = 1 , branch = 1, search branch
            try:
                sup = onto.search_one(label=branch, _case_sensitive=False)
                logger.info("Search {term} in MTBLS ontology {branch}".format(term=keyword, branch=branch))
            except:
                logger.info("Can't find a branch called " + branch)
                return []

            subs = sup.descendants()
            try:
                subs.remove(sup)
            except:
                pass
            result += list(set(subs) & set(cls))

            # synonym match
            if branch == 'taxonomy' or branch == 'factors':
                for cls in subs:
                    try:
                        map = IRIS['http://www.geneontology.org/formats/oboInOwl#hasExactSynonym']
                        Synonym = list(map[cls])
                        if keyword.lower() in [syn.lower() for syn in Synonym]:
                            result.append(cls)
                    except Exception as e:
                        pass

        else:  # term =1 branch = 0, search whole ontology
            result += cls

    else:  # term = None
        if branch not in [None, '']:  # term = 0, branch = 1, return whole ontology branch
            logger.info("Search Metabolights ontology whole {branch} branch ... ".format(branch=branch))

            try:
                sup = onto.search_one(label=branch, _case_sensitive=False)
                sub = sup.descendants()
                try:
                    sub.remove(sup)
                except:
                    pass

                result += sub

                # Change entity priority
                if branch == 'design descriptor' and keyword in [None, '']:
                    first_priority_terms = ['ultra-performance liquid chromatography-mass spectrometry',
                                            'untargeted metabolites', 'targeted metabolites']

                    for term in first_priority_terms:
                        temp = onto.search_one(label=term, _case_sensitive=False)
                        result.remove(temp)
                        result = [temp] + result

                result = result[:20]

            except Exception as e:
                print(e)
                logger.info("Can't find a branch called " + branch)
                print("Can't find a branch called " + branch)
                return []
        else:  # term = None, branch = None
            return []

    res = []

    for cls in result:
        enti = entity(name=cls.label[0], iri=cls.iri,
                      provenance_name='Metabolights')

        if cls.isDefinedBy:
            enti.definition = cls.isDefinedBy[0]

        if 'MTBLS' in cls.iri:
            enti.ontoName = 'MTBLS'

        else:
            try:
                onto_name = getOnto_Name(enti.iri)[0]
            except:
                onto_name = ''

            enti.ontoName = onto_name
            enti.provenance_name = onto_name

        res.append(enti)

    # OLS branch search
    if branch == 'instruments':
        if keyword in [None, '']:
            res += OLSbranchSearch('*', 'instrument', 'msio')
        else:
            res += OLSbranchSearch(keyword, 'instrument', 'msio')
    elif branch == 'column type':
        if keyword in [None, '']:
            res += OLSbranchSearch('*', 'chromatography', 'chmo')
        else:
            res += OLSbranchSearch(keyword, 'chromatography', 'chmo')
    elif branch == 'unit':
        if keyword in [None, '']:
            res += OLSbranchSearch('*', 'unit', 'uo')
        else:
            res += OLSbranchSearch(keyword, 'unit', 'uo')

    return res


def getOLSTerm(keyword, map, ontology=''):
    logger.info('Requesting OLS...')
    print('Requesting OLS...')
    res = []

    if keyword in [None, '']:
        return res

    elif 'http:' in keyword:
        label, definition, ontoName = getOLSTermInfo(keyword)
        if len(definition) > 0:
            definition = definition[0]
        else:
            definition = ''
        if len(label) > 1:
            enti = entity(name=label, iri=keyword, definition=definition, ontoName=ontoName,
                          provenance_name=ontoName)
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

        if ontology not in [None, '']:
            onto_list = ','.join(ontology)
            url += '&ontology=' + onto_list

        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)
        responses = j_content["response"]['docs']

        for term in responses:
            # name = ' '.join([w.capitalize() if w.islower() else w for w in term['label'].split()])

            name = term['label']
            try:
                definition = term['description'][0]
            except:
                definition = ''

            try:
                ontoName, provenance_name = getOnto_Name(term['iri'])
            except:
                ontoName = ''
                provenance_name = ''

            enti = entity(name=name, iri=term['iri'], definition=definition, ontoName=ontoName,
                          provenance_name=provenance_name)

            res.append(enti)
            if len(res) >= 20:
                break

    except Exception as e:
        print(e.args)
        logger.error('getOLS' + str(e))
    return res


def OLSbranchSearch(keyword, branchName, ontoName):
    res = []
    if keyword in [None, '']:
        return res

    def getStartIRI(start, ontoName):
        uri = 'search?q=' + start + '&ontology=' + ontoName + '&queryFields=label'
        url = os.path.join(get_settings().external_dependencies.api.ols_api_url, uri)
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        json_str = json.loads(content)
        res = json_str['response']['docs'][0]['iri']
        return urllib.parse.quote_plus(res)

    branchIRI = getStartIRI(branchName, ontoName)
    keyword = keyword.replace(' ', '%20')
    uri = 'search?q=' + keyword + '&rows=10&ontology=' + ontoName + '&allChildrenOf=' + branchIRI
    url = os.path.join(get_settings().external_dependencies.api.ols_api_url, uri)
    # print(url)
    fp = urllib.request.urlopen(url)
    content = fp.read().decode('utf-8')
    json_str = json.loads(content)

    for ele in json_str['response']['docs']:
        enti = entity(name=ele['label'], iri=ele['iri'], ontoName=ontoName, provenance_name=ontoName)
        res.append(enti)
    return res


def getMetaboZoomaTerm(keyword, mapping):
    logger.info('Searching Metabolights-zooma.tsv')
    print('Searching Metabolights-zooma.tsv')
    res = []

    if keyword in [None, '']:
        return res

    try:
        fileName = get_settings().file_resources.mtbls_zooma_file # metabolights_zooma.tsv
        df = pd.read_csv(fileName, sep="\t", header=0, encoding='utf-8')
        df = df.drop_duplicates(subset='PROPERTY_VALUE', keep="last")

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

            enti = entity(name=name,
                          iri=iri,
                          provenance_name='metabolights-zooma',
                          provenance_uri='https://www.ebi.ac.uk/metabolights/',
                          Zooma_confidence='High')

            try:
                enti.ontoName, enti.definition = getOnto_Name(iri)
            except:
                enti.ontoName = 'MTBLS'

            res.append(enti)
    except Exception as e:
        logger.error('Fail to load metabolights-zooma.tsv' + str(e))

    return res


def getZoomaTerm(keyword, mapping=''):
    logger.info('Requesting Zooma...')
    print('Requesting Zooma...')
    res = []

    if keyword in [None, '']:
        return res

    try:
        # url = 'http://snarf.ebi.ac.uk:8480/spot/zooma/v2/api/services/annotate?propertyValue=' + keyword.replace(' ',"+")
        uri = 'services/annotate?propertyValue=' + keyword.replace(' ', "+")
        url = os.path.join(get_settings().external_dependencies.api.zooma_api_url, uri)
        ssl._create_default_https_context = ssl._create_unverified_context
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf8')
        json_str = json.loads(content)
        for term in json_str:
            iri = term['semanticTags'][0]

            # name = ' '.join(
            #     [w.capitalize() if w.islower() else w for w in term["annotatedProperty"]['propertyValue'].split()])

            name = term["annotatedProperty"]['propertyValue']

            if mapping == 'exact' and name != keyword:
                continue

            enti = entity(name=name,
                          iri=iri,
                          Zooma_confidence=term['confidence'])

            if enti.ontoName == '':
                enti.ontoName, enti.definition = getOnto_Name(iri)

            try:
                enti.provenance_name = term['derivedFrom']['provenance']['source']['name']
            except:
                enti.provenance_name = enti.ontoName

            if enti.provenance_name == 'metabolights':
                res = [enti] + res
            else:
                res.append(enti)

            if len(res) >= 10:
                break
    except Exception as e:
        logger.error('getZooma' + str(e))
    return res


def getBioportalTerm(keyword, ontology=''):
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
        
        url = os.path.join(get_settings().external_dependencies.api.bioontology_api_url, uri)

        if ontology:
            ontology = [x.upper() for x in ontology]
        onto_list = ','.join(ontology)
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
                ontoName = term['links']['ontology'].split('/')[-1]
            except:
                ontoName = getOnto_Name(iri)[0]

            enti = entity(name=term['prefLabel'],
                          iri=iri,
                          ontoName=ontoName, provenance_name=ontoName)
            res.append(enti)
            iri_record.append(iri)
            if len(res) >= 10:
                break
    except Exception as e:
        logger.error('getBioportal' + str(e))
    return res


def getWormsTerm(keyword):
    logger.info('Requesting WoRMs ...')
    print('Requesting WoRMs ...')

    res = []
    if keyword in [None, '']:
        return res

    try:
        uri = 'AphiaRecordsByName/{keyword}?like=true&marine_only=true&offset=1'.format(
            keyword=keyword.replace(' ', '%20'))
        url = os.path.join(get_settings().external_dependencies.api.marine_species_api_url, uri)
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)

        for term in j_content:
            if term and "scientificname" in term and term["scientificname"] and "url" in term and term["url"]:
                name = term["scientificname"]
                iri = term["url"]
                definition = term["authority"]
                ontoName = 'WoRMs'
                provenance_name = 'World Register of Marine Species'

                enti = entity(name=name, iri=iri, definition=definition, ontoName=ontoName,
                              provenance_name=provenance_name)
                res.append(enti)

            if len(res) >= 10:
                break
    except Exception as e:
        logger.error(str(e))

    return res


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


def getOLSTermInfo(iri):
    # enti = entity(name=name, iri=term['iri'], definition=definition, ontoName=ontoName, provenance_name=provenance_name)

    try:
        uri = 'terms/findByIdAndIsDefiningOntology?iri=' + iri
        url = os.path.join(get_settings().external_dependencies.api.ols_api_url, uri)
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)

        label = j_content['_embedded']['terms'][0]['label']
        definition = j_content['_embedded']['terms'][0]["description"]
        if definition is None:
            definition = ''
        ontoName = j_content['_embedded']['terms'][0]['ontology_prefix']

        return label, definition, ontoName
    except:
        return '', '', ''


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


def getOnto_Name(iri):
    # get ontology name by giving iri of entity
    try:
        uri = 'terms/findByIdAndIsDefiningOntology?iri=' + iri
        url = os.path.join(get_settings().external_dependencies.api.ols_api_url, uri)
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)
        try:
            return j_content['_embedded']['terms'][0]['ontology_prefix'], \
                   j_content['_embedded']['terms'][0]['description'][0]
        except:
            return j_content['_embedded']['terms'][0]['ontology_prefix'], ''
    except:
        if 'MTBLS' in iri:
            return 'MTBLS', 'Metabolights ontology'
        elif 'BAO' in iri:
            return 'BAO', 'BioAssay Ontology'
        else:
            substring = iri.rsplit('/', 1)[-1]
            if '_' in substring:
                substring = substring.rsplit('_')[0]
            return ''.join(x for x in substring if x.isalpha()), ''


def getOnto_version(pre_fix):
    try:
        uri = 'ontologies/' + pre_fix
        url = os.path.join(get_settings().external_dependencies.api.ols_api_url, uri)
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)
        return j_content['config']['version']
    except:
        return ''


def getOnto_url(pre_fix):
    try:
        uri = 'ontologies/' + pre_fix
        url = os.path.join(get_settings().external_dependencies.api.ols_api_url, uri)
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)
        return j_content['config']['id']
    except:
        return ''


def setPriority(res_list, priority):
    res = sorted(res_list, key=lambda x: priority.get(x.ontoName, 1000))
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


def removeDuplicated(res_list):
    iri_pool = []
    for res in res_list:
        if res.iri in iri_pool:
            res_list.remove(res)
        else:
            iri_pool.append(res.iri)
    return res_list


def getDescriptionURL(ontoName, iri):
    ir = quote_plus(quote_plus(iri))
    uri = 'ontologies/' + ontoName + '/terms/' + ir
    url = os.path.join(get_settings().external_dependencies.api.ols_api_url, uri)
    return url
