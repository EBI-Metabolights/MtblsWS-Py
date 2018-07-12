# testing
# Created by JKChang
# 26/06/2018, 14:09
# Tag:
# Description: 

class information():
    ''' basic information of entities'''

    def __init__(self, onto):
        '''initialization'''
        self.onto = onto

    def get_subs(self, txt):
        print('matching %s subclass..' %txt)
        '''return list of sub classes'''
        sub = []
        try:
            onto_c = self.onto.search_one(label=txt)
            list_subs(onto_c, sub)
            return [list(x)[0] for x in sub if len(x) > 0]
        except:
            try:
                onto_c = self.onto.search_one(iri=txt)
                list_subs(onto_c, sub)
                return [list(x)[0] for x in sub if len(x) > 0]
            except:
                print('can not find %s' % txt)
                pass

    def get_supers(self, txt):
        print('matching %s superclass..' % txt)
        ''''return list of super classes'''
        sup = []
        try:
            onto_c = self.onto.search_one(label=txt)
            list_supers(onto_c, sup)
            return [list(x)[0] for x in sup if len(x) > 0]
        except:
            try:
                onto_c = self.onto.search_one(iri=txt)
                list_subs(onto_c, sup)
                return [list(x)[0] for x in sup if len(x) > 0]
            except:
                print('can not find %s' % txt)
                pass

    def sub_count(self, class_label):
        print('counting %s subclass..' % class_label)
        '''return subclass count'''
        res = self.get_subs(class_label)
        return len(res)

    def super_count(self, class_label):
        print('counting %s superclass..' % class_label)
        '''return superclass count'''
        res = self.get_supers(class_label)
        return len(res)

    def get_iri(self, class_label):
        try:
            onto_c = self.onto.search_one(label=class_label)
            # print('matching %s ..' %class_label)
            return onto_c.iri
        except:
            pass


def list_supers(onto_c, sup):
    if onto_c.label == '':
        return
    for parent in onto_c.is_a:
        list_supers(parent, sup)
        sup.append(parent.label)


def list_subs(onto_c, sub):
    if onto_c.label == '':
        return
    for children in onto_c.subclasses():
        list_subs(children, sub)
        sub.append(children.label)
