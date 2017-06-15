from flask_restful_swagger import swagger
from isatools.model.v1 import Person, OntologyAnnotation, OntologySource
import json


@swagger.model
class StudyContact(Person):
    def __init__(self, first_name=None, last_name=None, mid_initials=None,
                 email=None, phone=None, fax=None,
                 address=None, affiliation=None,
                 roles=None,
                 comments=None,
                 id_=''
                 ):
        super().__init__(comments)
        self.id = id_
        self.last_name = last_name
        self.first_name = first_name
        self.mid_initials = mid_initials
        self.email = email
        self.phone = phone
        self.fax = fax
        self.address = address
        self.affiliation = affiliation
        self.roles = roles


def serialize_OntologySource(isa_obj):
    assert isinstance(isa_obj, OntologySource)
    # name (str):
    # file (str):
    # version (str):
    # description (str):
    # comments (list,):
    return {
        'name': isa_obj.name,
        'file': isa_obj.file,
        'version': isa_obj.version,
        'description': isa_obj.description,
        'comments': isa_obj.comments
    }


def serialize_OntologyAnnotation(isa_obj):
    assert isinstance(isa_obj, OntologyAnnotation)
    # term (str, NoneType):
    # term_source (OntologySource, NoneType):
    # term_accession (str, NoneType):
    # comments (list, NoneType):
    term_source = None
    if hasattr(isa_obj, 'term_source') and isa_obj.term_source is not None:
        term_source = serialize_OntologySource(isa_obj.term_source)

    return {
        'term': isa_obj.term,
        'term_source': term_source,
        'term_accession': isa_obj.term_accession,
        'comments': isa_obj.comments
    }


def serialize_person(isa_obj):
    assert isinstance(isa_obj, Person)
    # last_name (str, NoneType):
    # first_name (str, NoneType):
    # mid_initials (str, NoneType):
    # email (str, NoneType):
    # phone (str, NoneType):
    # fax (str, NoneType):
    # address (str, NoneType):
    # affiliation (str, NoneType):
    # roles (list, NoneType):
    # comments (list, OntologyAnnotations):
    return {
        'last_name': isa_obj.last_name,
        'first_name': isa_obj.first_name,
        'mid_initials': isa_obj.mid_initials,
        'email': isa_obj.email,
        'phone': isa_obj.phone,
        'fax': isa_obj.fax,
        'address': isa_obj.address,
        'affiliation': isa_obj.affiliation,
        'roles': json.loads(json.dumps(isa_obj.roles, default=serialize_OntologyAnnotation, sort_keys=True)),
        'comments': isa_obj.comments
    }
