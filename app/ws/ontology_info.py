# onto_info
# Created by JKChang
# 25/07/2018, 11:18
# Tag:
# Description:

class onto_information():
    ''' basic onto_information of entities'''

    def __init__(self, onto):
        '''initialization'''
        self.onto = onto

    def get_subs(self, cls):
        '''return list of sub classes -> list'''
        print('matching subs of %s' % cls.label)
        sub = []
        list_subs(cls, sub)
        # print(type(sub[0]))
        return sub

    def get_supers(self, cls):
        ''''return list of super classes'''
        print('matching sups of %s ' % cls.label)
        sup = []
        list_supers(cls, sup)
        return [x for x in sup if len(x.label) > 0]

    def sub_count(self, cls):
        '''return subclass count'''
        print('counting subclass of %s..' % cls.label)
        return len(self.get_subs(cls))

    def sup_count(self, cls):
        '''return subclass count'''
        print('counting superclass of %s..' % cls.label)
        return len(self.get_supers(cls))

    def get_iri(self, cls):
        return cls.iri

    def get_factors(self,cls):
        return list(cls.seeAlso)

class entity():
    def __init__(self,name = None, iri = None, obo_ID= None, ontoName = None, Zooma_confidence = None):
        if name is None:
            self.name = ''
        else:
            self.name = name

        if iri is None:
            self.iri = ''
        else:
            self.iri = iri

        if obo_ID is None:
            self.obo_ID = ''
        else:
            self.obo_ID = obo_ID

        if ontoName is None:
            self.ontoName = ''
        else:
            self.ontoName = ontoName

        if Zooma_confidence is None:
            self.Zooma_confidence = ''
        else:
            self.Zooma_confidence = Zooma_confidence


def list_supers(onto_c, sup):
    if onto_c.label == '' or onto_c.iri == 'http://www.w3.org/2002/07/owl#Thing':
        return
    for parent in onto_c.is_a:
        try:
            list_supers(parent, sup)
            sup.append(parent)
        except:
            continue


def list_subs(onto_c, sub):
    if onto_c.label == '' or onto_c.iri == 'http://www.w3.org/2002/07/owl#Thing':
        return
    for children in onto_c.subclasses():
        try:
            list_subs(children, sub)
            sub.append(children)
        except:
            continue
