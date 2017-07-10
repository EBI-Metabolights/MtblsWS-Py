from flask_restful import fields
from flask_restful_swagger import swagger
from isatools.model.v1 import Person, OntologyAnnotation, OntologySource, Protocol
from isatools.model.v1 import ProtocolParameter, StudyFactor, Comment
import json


Comment_api_model = {
    # name (str):
    # value (str, int, float, NoneType):
    'name': fields.String,
    'value': fields.String
}


def serialize_comment(isa_obj):
    assert isinstance(isa_obj, Comment)
    # name (str):
    # value (str, int, float, NoneType):
    return {
        'name': isa_obj.name,
        'value': isa_obj.value
    }


def unserialize_comment(json_obj):
    # name (str):
    # value (str, int, float, NoneType):
    name = ''
    if 'name' in json_obj and json_obj['name'] is not None:
        name = json_obj['name']
    value = ''
    if 'value' in json_obj and json_obj['value'] is not None:
        value = json_obj['value']

    return Comment(name=name,
                   value=value)


OntologySource_api_model = {
    'name': fields.String,
    'file': fields.String,
    'version': fields.String,
    'description': fields.String,
    'comments': fields.List(fields.Nested(Comment_api_model))
}


def serialize_ontology_source(isa_obj):
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
        'comments': json.loads(json.dumps(isa_obj.comments, default=serialize_comment, sort_keys=True))
    }


def unserialize_ontology_source(json_obj):
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
    if 'comments' in json_obj and json_obj['comments'] is not None:
        for comment in json_obj['comments']:
            comments.append(unserialize_comment(comment))

    return OntologySource(name=name,
                          file=file,
                          version=version,
                          description=description,
                          comments=comments)


OntologyAnnotation_api_model = {
    'term': fields.String,
    'term_source': OntologySource_api_model,
    'term_accession': fields.String,
    'comments': fields.List(fields.Nested(Comment_api_model))
}


def serialize_ontology_annotation(isa_obj):
    assert isinstance(isa_obj, OntologyAnnotation)
    # term (str, NoneType):
    # term_source (OntologySource, NoneType):
    # term_accession (str, NoneType):
    # comments (list, NoneType):
    term_source = None
    if hasattr(isa_obj, 'term_source') and isa_obj.term_source is not None:
        term_source = serialize_ontology_source(isa_obj.term_source)
    return {
        'term': isa_obj.term,
        'term_source': term_source,
        'term_accession': isa_obj.term_accession,
        'comments': json.loads(json.dumps(isa_obj.comments, default=serialize_comment, sort_keys=True))
    }


def unserialize_ontology_annotation(json_obj):
    # term (str, NoneType):
    # term_source (OntologySource, NoneType):
    # term_accession (str, NoneType):
    # comments (list, NoneType):
    term = ''
    if 'term' in json_obj and json_obj['term'] is not None:
        term = json_obj['term']
    term_source = None
    if 'term_source' in json_obj and json_obj['term_source'] is not None:
        term_source = unserialize_ontology_source(json_obj['term_source'])
    term_accession = ''
    if 'term_accession' in json_obj and json_obj['term_accession'] is not None:
        term_accession = json_obj['term_accession']
    comments = list()
    if 'comments' in json_obj and json_obj['comments'] is not None:
        for comment in json_obj['comments']:
            comments.append(unserialize_comment(comment))

    return OntologyAnnotation(term=term,
                              term_source=term_source,
                              term_accession=term_accession,
                              comments=comments)


ProtocolParameter_api_model = {
    'parameter_name': fields.Nested(OntologyAnnotation_api_model),
    'unit': fields.Nested(OntologyAnnotation_api_model),
    'comments': fields.List(fields.Nested(Comment_api_model))
}


def serialize_protocol_parameter(isa_obj):
    assert isinstance(isa_obj, ProtocolParameter)
    # name (OntologyAnnotation): A parameter name as a term
    # unit (OntologyAnnotation): A unit, if applicable
    # comments (list, NoneType):
    parameter_name = None
    if hasattr(isa_obj, 'parameter_name') and isa_obj.parameter_name is not None:
        parameter_name = serialize_ontology_annotation(isa_obj.parameter_name)
    unit = None
    if hasattr(isa_obj, 'unit') and isa_obj.unit is not None:
        unit = serialize_ontology_annotation(isa_obj.unit)
    return {
        'parameter_name': parameter_name,
        'unit': unit,
        'comments': json.loads(json.dumps(isa_obj.comments, default=serialize_comment, sort_keys=True))
    }


def unserialize_protocol_parameter(json_obj):
    # name (OntologyAnnotation): A parameter name as a term
    # unit (OntologyAnnotation): A unit, if applicable
    # comments (list, NoneType):
    parameter_name = OntologyAnnotation()
    if 'parameter_name' in json_obj and json_obj['parameter_name'] is not None:
        parameter_name = unserialize_ontology_annotation(json_obj['parameter_name'])
    unit = OntologyAnnotation()
    if 'unit' in json_obj and json_obj['unit'] is not None:
        unit = unserialize_ontology_annotation(json_obj['unit'])
    comments = list()
    if 'comments' in json_obj and json_obj['comments'] is not None:
        for comment in json_obj['comments']:
            comments.append(unserialize_comment(comment))

    return ProtocolParameter(parameter_name=parameter_name,
                             unit=unit,
                             comments=comments)


Protocol_api_model = {
    'name': fields.String,
    'protocol_type': fields.Nested(OntologyAnnotation_api_model),
    'description': fields.String,
    'uri': fields.String,
    'version': fields.String,
    'parameters': fields.List(fields.Nested(ProtocolParameter_api_model)),
    'components': fields.List(fields.Nested(OntologyAnnotation_api_model)),
    'comments': fields.List(fields.Nested(Comment_api_model))
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
        'protocol_type': json.loads(json.dumps(isa_obj.protocol_type, default=serialize_ontology_annotation, sort_keys=True)),
        'description': isa_obj.description,
        'uri': isa_obj.uri,
        'version': isa_obj.version,
        'parameters': json.loads(json.dumps(isa_obj.parameters, default=serialize_protocol_parameter, sort_keys=True)),
        'components': json.loads(json.dumps(isa_obj.components, default=serialize_ontology_annotation, sort_keys=True)),
        'comments': json.loads(json.dumps(isa_obj.comments, default=serialize_comment, sort_keys=True))
    }


def unserialize_protocol(json_obj):
    # name (str):
    # protocol_type (OntologyAnnotation):
    # description (str):
    # uri (str):
    # version (str):
    # parameters (list, ProtocolParameter):
    # components (list, OntologyAnnotation):
    # comments (list, str):
    name = ''
    if 'name' in json_obj and json_obj['name'] is not None:
        name = json_obj['name']
    protocol_type = OntologyAnnotation()
    if 'protocol_type' in json_obj and json_obj['protocol_type'] is not None:
        protocol_type = unserialize_ontology_annotation(json_obj['protocol_type'])
    description = ''
    if 'description' in json_obj and json_obj['description'] is not None:
        description = json_obj['description']
    uri = ''
    if 'uri' in json_obj and json_obj['uri'] is not None:
        uri = json_obj['uri']
    version = ''
    if 'version' in json_obj and json_obj['version'] is not None:
        version = json_obj['version']
    parameters = list()
    if 'parameters' in json_obj:
        for parameter in json_obj['parameters']:
            parameters.append(unserialize_protocol_parameter(parameter))
    components = list()
    if len(json_obj['components']) > 0:
        for comp in json_obj['components']:
            components.append(unserialize_ontology_annotation(comp))
    comments = list()
    if 'comments' in json_obj and json_obj['comments'] is not None:
        for comment in json_obj['comments']:
            comments.append(unserialize_comment(comment))

    return Protocol(name=name,
                    protocol_type=protocol_type,
                    description=description,
                    uri=uri,
                    version=version,
                    parameters=parameters,
                    components=components,
                    comments=comments)


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
    'comments': fields.List(fields.Nested(Comment_api_model))
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
        'roles': json.loads(json.dumps(isa_obj.roles, default=serialize_ontology_annotation, sort_keys=True)),
        'comments': json.loads(json.dumps(isa_obj.comments, default=serialize_comment, sort_keys=True))
    }


def unserialize_person(json_obj):
    # last_name (str, NoneType):
    # first_name (str, NoneType):
    # mid_initials (str, NoneType):
    # email (str, NoneType):
    # phone (str, NoneType):
    # fax (str, NoneType):
    # address (str, NoneType):
    # affiliation (str, NoneType):
    # roles (list, OntologyAnnotations):
    # comments (list, str):
    last_name = ''
    if 'last_name' in json_obj and json_obj['last_name'] is not None:
        last_name = json_obj['last_name']
    first_name = ''
    if 'first_name' in json_obj and json_obj['first_name'] is not None:
        first_name = json_obj['first_name']
    mid_initials = ''
    if 'mid_initials' in json_obj and json_obj['mid_initials'] is not None:
        mid_initials = json_obj['mid_initials']
    email = ''
    if 'email' in json_obj and json_obj['email'] is not None:
        email = json_obj['email']
    phone = ''
    if 'phone' in json_obj and json_obj['phone'] is not None:
        phone = json_obj['phone']
    fax = ''
    if 'fax' in json_obj and json_obj['fax'] is not None:
        fax = json_obj['fax']
    address = ''
    if 'address' in json_obj and json_obj['address'] is not None:
        address = json_obj['address']
    affiliation = ''
    if 'affiliation' in json_obj and json_obj['affiliation'] is not None:
        affiliation = json_obj['affiliation']
    roles = list()
    if len(json_obj['roles']) > 0:
        for role in json_obj['roles']:
            roles.append(unserialize_ontology_annotation(role))
    comments = list()
    if 'comments' in json_obj and json_obj['comments'] is not None:
        for comment in json_obj['comments']:
            comments.append(unserialize_comment(comment))

    return Person(first_name=first_name,
                  last_name=last_name,
                  mid_initials=mid_initials,
                  email=email,
                  phone=phone,
                  fax=fax,
                  address=address,
                  affiliation=affiliation,
                  roles=roles,
                  comments=comments)


StudyFactor_api_model = {
    # name (str):
    # factor_type (OntologyAnnotation):
    # comments (list, NoneType):
    'name': fields.String,
    'factor_type': fields.Nested(OntologyAnnotation_api_model),
    'comments': fields.List(fields.Nested(Comment_api_model))
}


def serialize_study_factor(isa_obj):
    assert isinstance(isa_obj, StudyFactor)
    # name (str):
    # factor_type (OntologyAnnotation):
    # comments (list, NoneType):
    return {
        'name': isa_obj.name,
        'factor_type': json.loads(json.dumps(isa_obj.factor_type, default=serialize_ontology_annotation, sort_keys=True)),
        'comments': json.loads(json.dumps(isa_obj.comments, default=serialize_comment, sort_keys=True))
    }


def unserialize_study_factor(json_obj):
    # name (str):
    # factor_type (OntologyAnnotation):
    # comments (list, Comment):
    name = ''
    if 'name' in json_obj and json_obj['name'] is not None:
        name = json_obj['name']
    factor_type = OntologyAnnotation()
    if 'factor_type' in json_obj and json_obj['factor_type'] is not None:
        factor_type = unserialize_ontology_annotation(json_obj['factor_type'])
    comments = list()
    if 'comments' in json_obj and json_obj['comments'] is not None:
        for comment in json_obj['comments']:
            comments.append(unserialize_comment(comment))

    return StudyFactor(name=name,
                       factor_type=factor_type,
                       comments=comments)
