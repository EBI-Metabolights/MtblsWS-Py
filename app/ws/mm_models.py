from isatools.model import *
from marshmallow import Schema, fields, post_load, post_dump, pre_load, pre_dump


class CommentSchema(Schema):
    # marshmallow schema for ISA-API class Comment
    #
    # name                          (str)
    # value                         (str)
    class Meta:
        ordered = True

    name = fields.Str()
    value = fields.Str()

    @post_load
    def make_obj(self, data):
        return Comment(**data)


class IsaSchema(Schema):
    comments = fields.Nested(CommentSchema, many=True)


class OntologySourceSchema(IsaSchema):
    # marshmallow schema for ISA-API class OntologySource
    #
    # name                          (str)
    # file                          (str)
    # version                       (str)
    # description                   (str)
    # comments                      (list: Comment)
    class Meta:
        ordered = True

    name = fields.String()
    file = fields.String()
    version = fields.String()
    description = fields.String()

    @post_load
    def make_obj(self, data):
        return OntologySource(**data)


class OntologyAnnotationSchema(IsaSchema):
    # marshmallow schema for ISA-API class OntologyAnnotation
    #
    # term                          (str)
    # term_source                   (OntologySource)
    # term_accession                (str)
    # comments                      (list: Comment)
    class Meta:
        strict = True
        ordered = True

    term = fields.String(required=True,
                         load_from='annotationValue', dump_to='annotationValue')
    term_source = fields.Nested(OntologySourceSchema, load_from='termSource', dump_to='termSource',
                                required=False, allow_none=True)
    term_accession = fields.String(load_from='termAccession', dump_to='termAccession')

    @post_load
    def make_obj(self, data):
        return OntologyAnnotation(**data)


class PersonSchema(IsaSchema):
    # marshmallow schema for ISA-API class Person
    #
    # last_name -> lastName         (str)
    # first_name -> firstName       (str)
    # mid_initials -> midInitials   (str)
    # email                         (str)
    # phone                         (str)
    # fax                           (str)
    # address                       (str)
    # affiliation                   (str)
    # roles                         (list: OntologyAnnotation)
    # comments                      (list: Comment)
    class Meta:
        strict = True
        ordered = True

    first_name = fields.Str(required=True, load_from='firstName', dump_to='firstName')
    last_name = fields.Str(required=True, load_from='lastName', dump_to='lastName')
    email = fields.Email(required=True)
    affiliation = fields.Str(required=True)
    address = fields.Str()
    fax = fields.Str()
    mid_initials = fields.Str(load_from='midInitials', dump_to='midInitials')
    phone = fields.Str()
    roles = fields.Nested(OntologyAnnotationSchema, many=True)

    @post_load
    def make_obj(self, data):
        return Person(**data)

    # add an envelope to responses
    @post_dump(pass_many=True)
    def set_envelop(self, data, many):
        if 'contact' in self.context:
            key = 'contacts' if many else 'contact'
            return {
                key: data
            }


class ProtocolParameterSchema(IsaSchema):
    # marshmallow schema for ISA-API class ProtocolParameter
    #
    # parameter_name -> parameterName   (OntologyAnnotation)
    # comments                          (list: Comment)
    class Meta:
        strict = True
        ordered = True

    parameter_name = fields.Nested(OntologyAnnotationSchema, required=True,
                                   load_from='parameterName', dump_to='parameterName')

    @post_load
    def make_obj(self, data):
        return ProtocolParameter(**data)


class ProtocolSchema(IsaSchema):
    # marshmallow schema for ISA-API class Protocol
    #
    # name                              (str)
    # protocol_type -> protocolType     (OntologyAnnotation)
    # description                       (str)
    # uri                               (str)
    # version                           (str)
    # parameters                        (list: ProtocolParameter)
    # components                        (list: str | OntologyAnnotation)
    # comments                          (list: comment)
    class Meta:
        strict = True
        ordered = True

    name = fields.Str(required=True)
    protocol_type = fields.Nested(OntologyAnnotationSchema,
                                  load_from='protocolType', dump_to='protocolType')
    description = fields.Str()
    uri = fields.Str()
    version = fields.Str()
    parameters = fields.Nested(ProtocolParameterSchema, missing=None, many=True)
    components = fields.Nested(OntologyAnnotationSchema, missing=None, many=True)

    @post_load
    def make_obj(self, data):
        return Protocol(**data)

    # add an envelope to responses
    @post_dump(pass_many=True)
    def set_envelop(self, data, many):
        if 'protocol' in self.context:
            key = 'protocols' if many else 'protocol'
            return {
                key: data
            }


class StudyFactorSchema(IsaSchema):
    # marshmallow schema for ISA-API class StudyFactor
    #
    # name -> factorName        (str)
    # factor_type -> factorType (OntologyAnnotation)
    # comments                  (list: Comment)
    class Meta:
        strict = True
        ordered = True

    name = fields.Str(required=True,
                      load_from='factorName', dump_to='factorName')
    factor_type = fields.Nested(OntologyAnnotationSchema, missing=None,
                                load_from='factorType', dump_to='factorType')

    @post_load
    def make_obj(self, data):
        return StudyFactor(**data)

    # add an envelope to responses
    @post_dump(pass_many=True)
    def set_envelop(self, data, many):
        if 'factor' in self.context:
            key = 'factors' if many else 'factor'
            return {
                key: data
            }


class ValueField(fields.Field):
    def _serialize(self, value, attr, obj):
        if isinstance(value, (int, float, str)):
            return value
        if isinstance(value, OntologyAnnotation):
            val = {
                'annotationValue': value.term,
                'termSource': OntologySourceSchema(many=False).dump(value.term_source, many=False).data,
                'termAccession': value.term_accession,
                'comments': value.comments
            }
            return val

    def _deserialize(self, value, attr, data):
        # str, int or float
        val = data.get(attr)
        if isinstance(val, (int, float, str)):
            return value
        if isinstance(val, OntologyAnnotation):
            return OntologyAnnotationSchema().load(val)


class FactorValueSchema(IsaSchema):
    # marshmallow schema for ISA-API class Sample
    #
    # factor_name -> category   (StudyFactor)
    # value                     (str, int, float, OntologyAnnotation)
    # unit                      (OntologyAnnotation)
    # comments                  (list: Comment)
    class Meta:
        strict = True
        ordered = True

    factor_name = fields.Nested(StudyFactorSchema, required=True,
                                load_from='category', dump_to='category')
    value = ValueField(attribute='value')
    unit = fields.Nested(OntologyAnnotationSchema, required=False, allow_none=True)

    @post_load
    def make_obj(self, data):
        return FactorValue(**data)


class StudyDesignDescriptorSchema(OntologyAnnotationSchema):
    # Basically an ISA-API class OntologyAnnotation, but with
    # especial envelop tags when dumped

    # add an envelope to responses
    @post_dump(pass_many=True)
    def set_envelop(self, data, many):
        if 'descriptor' in self.context:
            key = 'studyDesignDescriptors' if many else 'studyDesignDescriptor'
            return {
                key: data
            }


class PublicationSchema(IsaSchema):
    # marshmallow schema for ISA-API class Publication
    #
    # pubmed_id -> pubMedID         (str)
    # doi                           (str)
    # author_list -> authorList     (str)
    # title                         (str)
    # status                        (str, OntologyAnnotation)
    # comments                      (list: Comment)
    class Meta:
        strict = True
        ordered = True

    title = fields.Str(required=True)
    author_list = fields.Str(many=True, load_from='authorList', dump_to='authorList')
    pubmed_id = fields.Str(load_from='pubMedID', dump_to='pubMedID')
    doi = fields.Str()
    status = fields.Nested(OntologyAnnotationSchema)

    @post_load
    def make_obj(self, data):
        return Publication(**data)

    # add an envelope to responses
    @post_dump(pass_many=True)
    def set_envelop(self, data, many):
        key = 'publications' if many else 'publication'
        return {
            key: data
        }


class CharacteristicSchema(IsaSchema):
    # marshmallow schema for ISA-API class Characteristic
    # material_attribute_value_schema.json in ISA-Model v1.0
    #
    # category  (OntologyAnnotation)
    # value     (str, int, float, OntologyAnnotation)
    # unit      (list: OntologyAnnotation)
    # comments  (list: Comment)
    class Meta:
        strict = True
        ordered = True

    category = fields.Nested(OntologyAnnotationSchema, required=True)
    value = ValueField(attribute='value')
    unit = fields.Nested(OntologyAnnotationSchema, required=False, allow_none=True)

    @post_load
    def make_obj(self, data):
        return Characteristic(**data)


class SourceSchema(IsaSchema):
    # marshmallow schema for ISA-API class Source
    #
    # name              (str)
    # characteristics   (list: OntologyAnnotation)
    # comments          (list: Comment)
    class Meta:
        strict = True
        ordered = True

    name = fields.Str(required=True)
    characteristics = fields.Nested(CharacteristicSchema, many=True)

    @post_load
    def make_obj(self, data):
        return Source(**data)

    # add an envelope to responses
    @post_dump(pass_many=True)
    def set_envelop(self, data, many):
        if 'source' in self.context:
            key = 'sources' if many else 'source'
            return {
                key: data
            }


class SampleSchema(IsaSchema):
    # marshmallow schema for ISA-API class Sample
    #
    # name                              (str)
    # characteristics                   (list: Characteristic)
    # factor_values -> factorValues     (FactorValues)
    # derives_from                      (Source)
    # comments                          (list: Comment)
    class Meta:
        strict = True
        ordered = True

    name = fields.Str(required=True)
    characteristics = fields.Nested(CharacteristicSchema, many=True)
    factor_values = fields.Nested(FactorValueSchema, many=True,
                                  load_from='factorValues', dump_to='factorValues')
    derives_from = fields.Nested(SourceSchema, many=True)

    @post_load
    def make_obj(self, data):
        return Sample(**data)

    # add an envelope to responses
    @post_dump(pass_many=True)
    def set_envelop(self, data, many):
        if 'sample' in self.context:
            key = 'samples' if many else 'sample'
            return {
                key: data
            }


class ParameterValueSchema(IsaSchema):
    # marshmallow schema for ISA-API class ParameterValue
    #
    # category      (ProtocolParameter)
    # value         (str, int, float, OntologyAnnotation)
    # unit          (list: OntologyAnnotation)
    # comments      (list: Comment)
    class Meta:
        strict = True
        ordered = True

    category = fields.Nested(ProtocolParameterSchema, required=True)
    value = ValueField(attribute='value')
    unit = fields.Nested(OntologyAnnotationSchema, required=False, allow_none=True)

    @post_load
    def make_obj(self, data):
        return ParameterValue(**data)


class MaterialSchema(IsaSchema):
    # marshmallow schema for ISA-API class Material
    #
    # name                              (str)
    # type                              (str, ["Extract Name", "Labeled Extract Name"])
    # characteristics                   (list: Characteristic)
    # factor_values -> factorValues     (FactorValues)
    # derives_from                      (Source)
    # comments                          (list: Material)
    class Meta:
        strict = True
        ordered = True

    name = fields.Str(required=True)
    type = fields.Str()
    characteristics = fields.Nested(CharacteristicSchema, many=True)
    factor_values = fields.Nested(FactorValueSchema, many=True,
                                  load_from='factorValues', dump_to='factorValues')
    derives_from = fields.Nested('self', many=True)

    @post_load
    def make_obj(self, data):
        return Material(**data)

    # add an envelope to responses
    @post_dump(pass_many=True)
    def set_envelop(self, data, many):
        if 'other_material' in self.context:
            key = 'otherMaterials' if many else 'otherMaterial'
            return {
                key: data
            }


class DataFileSchema(IsaSchema):
    # marshmallow schema for ISA-API class DataFile
    #
    # filename              (str)
    # label                 (str, ['Array Data File' or 'Raw Data File'])
    # generated_from        (list: Source)
    class Meta:
        strict = True
        ordered = True

    filename = fields.Str(required=True)
    label = fields.Str()
    generated_from = fields.Nested(SourceSchema, many=True)

    @post_load
    def make_obj(self, data):
        return DataFile(**data)


class InputOutpuField(fields.Field):
    def _serialize(self, value, attr, obj):
        if isinstance(value, Material):
            return MaterialSchema.dump(obj)
        if isinstance(value, Source):
            return SourceSchema.dump(obj)
        if isinstance(value, Sample):
            return SampleSchema.dump(obj)
        if isinstance(value, DataFile):
            return DataFileSchema.dump(obj)

    def _deserialize(self, value, attr, data):
        # str, int or float
        val = data.get(attr)
        if isinstance(val, (int, float, str)):
            return value
        if isinstance(val, OntologyAnnotation):
            return OntologyAnnotationSchema().load(val)


class ProcessSchema(IsaSchema):
    # marshmallow schema for ISA-API class Process
    #
    # name                                      (str)
    # executes_protocol -> executesProtocol     (Protocol)
    # date_                                     (str)
    # performer                                 (str)
    # parameter_values -> parameterValues       (list: ParameterValues)
    # prev_process -> previousProcess           (Process)
    # next_process -> nextProcess               (Process)
    # inputs                                    (list: Sources, Samples, Materials, DataFiles)
    # outputs                                   (list: Samples, Materials, DataFiles)
    # comments                                  (list: Comment)
    class Meta:
        strict = True
        ordered = True

    name = fields.Str(required=True)
    executes_protocol = fields.Nested(ProtocolSchema,
                                      load_from='executesProtocol', dump_to='executesProtocol')
    date = fields.Str()
    performer = fields.Str()
    parameter_values = fields.Nested(ParameterValueSchema, many=True,
                                     load_from='parameterValues', dump_to='parameterValues')
    prev_process = fields.Nested('self')
    next_process = fields.Nested('self')
    inputs = InputOutpuField(attribute='inputs')
    outputs = InputOutpuField(attribute='outputs')

    @post_load
    def make_obj(self, data):
        return Process(**data)

    # add an envelope to responses
    @post_dump(pass_many=True)
    def set_envelop(self, data, many):
        if 'process' in self.context:
            key = 'processes' if many else 'process'
            return {
                key: data
            }


class StudySchema(IsaSchema):
    # marshmallow schema for ISA-API class Study
    #
    pass


class IsaInvestigationSchema(Schema):
    # id_ -> id                                                 (str)
    # identifier                                                (str)
    # title                                                     (str)
    # description                                               (str)
    # submission_date -> submissionDate                         (str)
    # public_release_date -> publicReleaseDate                  (str)
    # filename                                                  (str)
    # contacts -> people                                        (list: Person)
    # publications                                              (list: StudyPublications)
    # ontology_source_references -> ontologySourceReferences    (list: OntologyAnnotation)
    # studies                                                   (list: Study)
    # "comments": [],
    class Meta:
        ordered = True

    id_ = fields.Str()
    identifier = fields.Str()
    title = fields.Str()
    description = fields.Str()
    submissionDate = fields.Str()
    public_release_date = fields.Str(dump_to='public_release_date')
    filename = fields.Str()
    contacts = fields.Nested(PersonSchema, many=True, dump_to='people')
    publications = fields.Nested(PublicationSchema, many=True)
    ontology_source_references = fields.Nested(OntologyAnnotationSchema, many=True,
                                               dump_to='ontologySourceReferences')
    studies = fields.Nested(StudySchema, many=True)
    comments = fields.Nested(CommentSchema, many=True)
