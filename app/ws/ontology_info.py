import json
from urllib.parse import quote_plus

from owlready2 import urllib


class onto_information():
    ''' basic onto_information of entities'''

    def __init__(self, onto):
        '''initialization'''
        self.onto = onto

    def get_subs(self, cls, num=100):
        '''return list of sub classes -> list'''
        # print('matching subs of %s' % cls.label)
        sub = []

        list_subs(cls, sub, num)
        # print(type(sub[0]))
        return sub

    def get_supers(self, cls):
        ''''return list of super classes'''
        # print('matching sups of %s ' % cls.label)
        sup = []
        list_supers(cls, sup)
        return [x for x in sup if len(x.label) > 0]

    def sub_count(self, cls):
        '''return subclass count'''
        # print('counting subclass of %s..' % cls.label)
        return len(self.get_subs(cls))

    def sup_count(self, cls):
        '''return subclass count'''
        # print('counting superclass of %s..' % cls.label)
        return len(self.get_supers(cls))

    def get_iri(self, cls):
        return cls.iri

    def get_factors(self, cls):
        return list(cls.seeAlso)


class entity():
    def __init__(self, name, iri='', ontoName='', provenance_name='', provenance_uri='',
                 Zooma_confidence='', definition=''):

        self.name = name
        self.iri = iri
        self.ontoName = ontoName
        self.provenance_name = provenance_name
        self.Zooma_confidence = Zooma_confidence




        # try:
        #     ir = quote_plus(quote_plus(iri))
        #     url = 'http://www.ebi.ac.uk/ols/api/terms/findByIdAndIsDefiningOntology/' + ir
        #     fp = urllib.request.urlopen(url)
        #     content = fp.read().decode('utf-8')
        #     j_content = json.loads(content)
        #
        #     try:
        #         self.ontoName = j_content['_embedded']['terms'][0]['ontology_prefix']
        #     except:
        #         self.ontoName = ontoName
        #
        #     try:
        #         self.provenance_uri = j_content['_embedded']['terms'][0]['ontology_iri']
        #     except:
        #         self.provenance_uri = provenance_uri
        #
        #     try:
        #         self.definition = j_content['_embedded']['terms'][0]["description"][0]
        #     except:
        #         self.definition = definition
        #
        # except:
        #     pass

    def getOntoInfo(self, iri):
        try:
            url = 'https://www.ebi.ac.uk/ols/api/terms/findByIdAndIsDefiningOntology?iri=' + iri
            fp = urllib.request.urlopen(url)
            content = fp.read().decode('utf-8')
            j_content = json.loads(content)

            ontoName = j_content['_embedded']['terms'][0]['ontology_prefix']
            ontoURL = j_content['_embedded']['terms'][0]['ontology_iri']
            definition = j_content['_embedded']['terms'][0]["description"]

            return ontoName, ontoURL, definition
        except:
            return '', '', ''


def list_supers(onto_c, sup):
    if onto_c.label == '' or onto_c.iri == 'http://www.w3.org/2002/07/owl#Thing':
        return
    for parent in onto_c.is_a:
        try:
            list_supers(parent, sup)
            sup.append(parent)
        except:
            continue


def list_subs(onto_c, sub, num):
    if onto_c.label and onto_c.label == '' and onto_c.iri != 'http://www.w3.org/2002/07/owl#Thing':
        return
    for children in onto_c.subclasses():
        try:
            list_subs(children, sub, num)
            if len(sub) >= num:
                return
            sub.append(children)
        except:
            continue
