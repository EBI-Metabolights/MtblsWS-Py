from flask_restful import fields
from flask_restful_swagger import swagger
from isatools.model.v1 import Person, OntologyAnnotation, OntologySource, Protocol
from isatools.model.v1 import ProtocolParameter, ProtocolComponent
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


OntologySource_api_model = {
    'name': fields.String,
    'file': fields.String,
    'version': fields.String,
    'description': fields.String,
    'comments': fields.List(fields.String)
}

OntologyAnnotation_api_model = {
    'term': fields.String,
    'term_source': OntologySource_api_model,
    'term_accession': fields.String,
    'comments': fields.List(fields.String)
}

ProtocolParameter_api_model = {
    'name': fields.Nested(OntologyAnnotation_api_model),
    'unit': fields.Nested(OntologyAnnotation_api_model),
    'comments': fields.List(fields.String)
}

Protocol_api_model = {
    'name': fields.String,
    'protocol_type': fields.Nested(OntologyAnnotation_api_model),
    'description': fields.String,
    'uri': fields.String,
    'version': fields.String,
    'parameters': fields.List(fields.Nested(ProtocolParameter_api_model)),
    'components': fields.List(fields.Nested(OntologyAnnotation_api_model)),
    'comments': fields.List(fields.String)
}

Person_api_model = {
    'last_name': fields.String,
    'first_name': fields.String,
    'mid_initials': fields.String,
    'email': fields.String,
    'phone': fields.String,
    'fax': fields.String,
    'address': fields.String,
    'affiliation': fields.String,
    'roles': fields.List(fields.Nested(OntologyAnnotation_api_model)),
    'comments': fields.List(fields.String)
}


def serialize_protocol(isa_obj):
    assert isinstance(isa_obj, Protocol)
    # name (str):
    # protocol_type (OntologyAnnotation):
    # description (str):
    # uri (str):
    # version (str):
    # parameters (list, ProtocolParameter):
    # components (list, OntologyAnnotation):
    # comments (list, str):
    return {
        'name': isa_obj.name,
        'protocol_type': json.loads(json.dumps(isa_obj.protocol_type, default=serialize_OntologyAnnotation, sort_keys=True)),
        'description': isa_obj.description,
        'uri': isa_obj.uri,
        'version': isa_obj.version,
        'parameters': json.loads(json.dumps(isa_obj.parameters, default=serialize_ProtocolParameter, sort_keys=True)),
        'components': json.loads(json.dumps(isa_obj.components, default=serialize_OntologyAnnotation, sort_keys=True)),
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


def serialize_ProtocolParameter(isa_obj):
    assert isinstance(isa_obj, ProtocolParameter)
    # name (OntologyAnnotation): A parameter name as a term
    # unit (OntologyAnnotation): A unit, if applicable
    # comments (list, NoneType):
    parameter_name = None
    if hasattr(isa_obj, 'parameter_name') and isa_obj.parameter_name is not None:
        parameter_name = serialize_OntologyAnnotation(isa_obj.parameter_name)

    unit = None
    if hasattr(isa_obj, 'unit') and isa_obj.unit is not None:
        unit = serialize_OntologyAnnotation(isa_obj.unit)

    return {
        'parameter_name': parameter_name,
        'unit': unit,
        'comments': isa_obj.comments
    }


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


def unserialize_Protocol(json_protocol):
    # name (str):
    # protocol_type (OntologyAnnotation):
    # description (str):
    # uri (str):
    # version (str):
    # parameters (list, ProtocolParameter):
    # components (list, OntologyAnnotation):
    # comments (list, str):
    name = ''
    if 'name' in json_protocol and json_protocol['name'] is not None:
        name = json_protocol['name']

    protocol_type = OntologyAnnotation()
    if 'protocol_type' in json_protocol and json_protocol['protocol_type'] is not None:
        protocol_type = unserialize_OntologyAnnotation(json_protocol['protocol_type'])

    description = ''
    if 'description' in json_protocol and json_protocol['description'] is not None:
        description = json_protocol['description']

    uri = ''
    if 'uri' in json_protocol and json_protocol['uri'] is not None:
        uri = json_protocol['uri']

    version = ''
    if 'version' in json_protocol and json_protocol['version'] is not None:
        version = json_protocol['version']

    parameters = list()
    if 'parameters' in json_protocol:
        for parameter in json_protocol['parameters']:
            parameters.append(unserialize_ProtocolParameter(parameter))

    components = list()
    if len(json_protocol['components']) > 0:
        for comp in json_protocol['components']:
            components.append(ProtocolComponent(name=comp['name']))

    comments = list()
    if 'comments' in json_protocol:
        for comment in json_protocol['comments']:
            comments.append(comment)

    return Protocol(name=name,
                    protocol_type=protocol_type,
                    description=description,
                    uri=uri,
                    version=version,
                    parameters=parameters,
                    components=components,
                    comments=comments)


def unserialize_OntologyAnnotation(json_obj):
    # term (str, NoneType):
    # term_source (OntologySource, NoneType):
    # term_accession (str, NoneType):
    # comments (list, NoneType):
    term = ''
    if 'term' in json_obj and json_obj['term'] is not None:
        term = json_obj['term']

    term_source = None
    if 'term_source' in json_obj and json_obj['term_source'] is not None:
        term_source = unserialize_OntologySource(json_obj['term'])

    term_accession = ''
    if 'term_accession' in json_obj and json_obj['term_accession'] is not None:
        term_accession = json_obj['term_accession']

    comments = list()
    if 'comments' in json_obj:
        for comment in json_obj['comments']:
            comments.append(comment)

    return OntologyAnnotation(term=term,
                              term_source=term_source,
                              term_accession=term_accession,
                              comments=comments)


def unserialize_OntologySource(json_obj):
    # name (str):
    # file (str):
    # version (str):
    # description (str):
    # comments (list,):
    name = ''
    if 'name' in json_obj and json_obj['name'] is not None:
        name = json_obj['name']

    file = ''
    if 'name' in json_obj and json_obj['file'] is not None:
        file = json_obj['file']

    version = ''
    if 'version' in json_obj and json_obj['version'] is not None:
        version = json_obj['version']

    description = ''
    if 'description' in json_obj and json_obj['description'] is not None:
        description = json_obj['description']

    comments = list()
    if 'comments' in json_obj:
        for comment in json_obj['comments']:
            comments.append(comment)

    return OntologySource(name=name,
                          file=file,
                          version=version,
                          description=description,
                          comments='')


def unserialize_ProtocolParameter(json_obj):
    # name (OntologyAnnotation): A parameter name as a term
    # unit (OntologyAnnotation): A unit, if applicable
    # comments (list, NoneType):
    parameter_name = OntologyAnnotation()
    if 'parameter_name' in json_obj and json_obj['parameter_name'] is not None:
        parameter_name = unserialize_OntologyAnnotation(json_obj['parameter_name'])

    unit = OntologyAnnotation()
    if 'unit' in json_obj and json_obj['unit'] is not None:
        unit = unserialize_OntologyAnnotation(json_obj['unit'])

    comments = list()
    if 'comments' in json_obj:
        for comment in json_obj['comments']:
            comments.append(comment)

    return ProtocolParameter(parameter_name=parameter_name,
                             unit=unit,
                             comments=comments)


def serialize_Person(isa_obj):
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
