#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Jan-30
#  Modified by:   kenneth
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

from isatools.model import *
from marshmallow import Schema, fields, post_load, post_dump


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
    class Meta:
        strict = True
        ordered = True

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

    name = fields.String(required=False, allow_none=True)
    file = fields.String(required=False, allow_none=True)
    version = fields.String(required=False, allow_none=True)
    description = fields.String(required=False, allow_none=True)

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
    email = fields.Str(required=True)
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
    class Meta:
        strict = True
        ordered = True

    def _serialize(self, value, attr, obj):
        if isinstance(value, (int, float, str)):
            if isinstance(value, str) and not len(value) > 0:
                return None
            return value
        if isinstance(value, OntologyAnnotation):
            termsource = OntologySourceSchema().dump(value.term_source).data if value.term_source else None
            val = {
                'annotationValue': value.term,
                'termSource': termsource,
                'termAccession': value.term_accession,
                'comments': value.comments
            }
            return val

    def _deserialize(self, value, attr, data):
        if 'annotationValue' in value:
            return OntologyAnnotationSchema().load(value).data
        else:
            return value


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
        if 'publication' in self.context:
            key = 'publications' if many else 'publication'
            return {
                key: data
            }


class MaterialAttributeValueSchema(IsaSchema):
    # marshmallow schema for ISA-API class Characteristic
    # material_attribute_value_schema.json in ISA-Model v1.0
    #
    # category  (OntologyAnnotation)
    # value     (str, int, float, OntologyAnnotation)
    # unit      (list: OntologyAnnotation)
    # comments  (list: Comment)
    class Meta:
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

    name = fields.Str()
    characteristics = fields.Nested(MaterialAttributeValueSchema, many=True)

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
    # derives_from -> derivesFrom       (Source)
    # comments                          (list: Comment)

    name = fields.Str()
    characteristics = fields.Nested(MaterialAttributeValueSchema, many=True)
    factor_values = fields.Nested(FactorValueSchema, many=True,
                                  load_from='factorValues', dump_to='factorValues')
    derives_from = fields.Nested(SourceSchema, many=True,
                                 load_from='derivesFrom', dump_to='derivesFrom')

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


class OtherMaterialSchema(IsaSchema):
    # marshmallow schema for ISA-API class OtherMaterial
    #
    # name                              (str)
    # type                              (str, ["Extract Name", "Labeled Extract Name"])
    # characteristics                   (list: Characteristic)
    # comments                          (list: Comment)

    name = fields.Str(allow_none=True)
    # needed two separate fields due to clash with reserved words
    type_ = fields.Str(allow_none=True, load_from='typeMaterial')
    type = fields.Str(allow_none=True, dump_to='typeMaterial')
    characteristics = fields.Nested(MaterialAttributeValueSchema, many=True)

    @post_load
    def make_obj(self, data):
        return Material(**data)


class DataFileSchema(IsaSchema):
    # marshmallow schema for ISA-API class DataFile
    #
    # filename                          (str)
    # label                             (str)
    # generated_from -> generatedFrom   (list: [Source, Sample])
    # comments                          (list: Comment)

    filename = fields.Str(required=True)
    label = fields.Str()
    generated_from = fields.Nested(SampleSchema, many=True,
                                   load_from='generatedFrom', dump_to='generatedFrom')

    @post_load
    def make_obj(self, data):
        return DataFile(**data)

    # add an envelope to responses
    @post_dump(pass_many=True)
    def set_envelop(self, data, many):
        if 'dataFile' in self.context:
            key = 'dataFiles' if many else 'dataFile'
            return {
                key: data
            }


class ParameterValueSchema(Schema):
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
    value = ValueField(attribute='value', required=True)
    unit = fields.Nested(OntologyAnnotationSchema, allow_none=True)

    @post_load
    def make_obj(self, data):
        return ParameterValue(**data)


class InputOutputField(fields.Field):
    class Meta:
        strict = True
        ordered = True

    def _serialize(self, value, attr, obj):
        val = list()
        for item in value:
            if isinstance(item, Source):
                val.append(SourceSchema().dump(item).data)
            elif isinstance(item, Sample):
                val.append(SampleSchema().dump(item).data)
            elif isinstance(item, Material):
                val.append(OtherMaterialSchema().dump(item).data)
            elif isinstance(item, DataFile):
                val.append(DataFileSchema().dump(item).data)
        return val

    def _deserialize(self, value, attr, data):
        val = list()
        for item in value:
            if 'filename' in item:
                val.append(DataFileSchema().load(item).data)
            elif 'derivesFrom' in item:
                val.append(SampleSchema().load(item).data)
            elif 'type' in item:
                val.append(OtherMaterialSchema().load(item).data)
            else:
                val.append(SourceSchema().load(item).data)
        return val


class ProcessSchema(IsaSchema):
    # marshmallow schema for ISA-API class Process
    #
    # name                                      (str)
    # executes_protocol -> executesProtocol     (Protocol)
    # date_ -> date                             (str)
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

    name = fields.Str(allow_none=True)
    executes_protocol = fields.Nested(ProtocolSchema,
                                      load_from='executesProtocol', dump_to='executesProtocol')
    date_ = fields.Str(allow_none=True, load_from='date', dump_to='date')
    performer = fields.Str(allow_none=True)
    parameter_values = fields.Nested(ParameterValueSchema, many=True,
                                     load_from='parameterValues', dump_to='parameterValues')
    prev_process = fields.Nested('self', only=('name', 'executes_protocol.name'),
                                 dump_to='previousProcess',
                                 allow_none=True)
    next_process = fields.Nested('self', only=('name', 'executes_protocol.name'),
                                 dump_to='nextProcess',
                                 allow_none=True)
    inputs = InputOutputField(allow_none=True)
    outputs = InputOutputField(allow_none=True)

    @post_load
    def make_obj(self, data):
        return Process(**data)

    # add an envelope to responses
    @post_dump(pass_many=True)
    def set_envelop(self, data, many):
        if 'process' in self.context:
            key = 'processSequence'
            return {
                key: data
            }


class AssaySchema(IsaSchema):
    # marshmallow schema for ISA-API class Assay
    #
    # measurement_type -> measurementType                       (OntologyAnnotation)
    # technology_type -> technologyType                         (OntologyAnnotation)
    # technology_platform -> technologyPlatform                 (str)
    # filename                                                  (str)
    # data_files -> dataFiles                                   (list: DataFiles)
    # process_sequence -> processSequence                       (list: Process)
    # sources                                                   (list: Source)
    # samples                                                   (list: Sample)
    # other_material -> otherMaterials                          (list: OtherMaterials)
    # characteristic_categories -> characteristicCategories     (list: OntologyAnnotation)
    # units                                                     (list: OntologyAnnotation)
    # comments                                                  (list: Comment)
    class Meta:
        ordered = True

    measurement_type = fields.Nested(OntologyAnnotationSchema,
                                     load_from='measurementType', dump_to='measurementType')
    technology_type = fields.Nested(OntologyAnnotationSchema,
                                    load_from='technologyType', dump_to='technologyType')
    technology_platform = fields.Str(load_from='technologyPlatform', dump_to='technologyPlatform')
    filename = fields.Str()
    data_files = fields.Nested(DataFileSchema, many=True,
                               load_from='dataFiles', dump_to='dataFiles')
    process_sequence = fields.Nested(ProcessSchema, many=True,
                                     only=('name',
                                           'date_', 'performer',
                                           'parameter_values',
                                           'executes_protocol',
                                           'prev_process',
                                           # 'prev_process.name',
                                           # 'prev_process.executes_protocol.name',
                                           'next_process',
                                           # 'next_process.name',
                                           # 'next_process.executes_protocol.name',
                                           'inputs',
                                           'outputs',
                                           'comments'
                                           ),
                                     load_from='processSequence', dump_to='processSequence')
    sources = fields.Nested(SourceSchema, many=True, allow_none=True)
    samples = fields.Nested(SampleSchema, many=True)
    other_material = fields.Nested(OtherMaterialSchema, many=True,
                                   load_from='otherMaterials', dump_to='otherMaterials')
    characteristic_categories = fields.Nested(OntologyAnnotationSchema, many=True,
                                              load_from='characteristicCategories', dump_to='characteristicCategories')
    units = fields.Nested(OntologyAnnotationSchema, many=True)

    graph = fields.Str()


    @post_load
    def make_obj(self, data):
        return Assay(**data)


class StudySchema(IsaSchema):
    # marshmallow schema for ISA-API class Study
    #

    identifier = fields.Str()
    filename = fields.Str()
    title = fields.Str()
    description = fields.Str()
    submission_date = fields.Str(load_from='submissionDate', dump_to='submissionDate')
    public_release_date = fields.Str(load_from='publicReleaseDate', dump_to='publicReleaseDate')
    contacts = fields.Nested(PersonSchema, many=True, load_from='people', dump_to='people')
    design_descriptors = fields.Nested(StudyDesignDescriptorSchema, many=True,
                                       load_from='studyDesignDescriptors', dump_to='studyDesignDescriptors')
    publications = fields.Nested(PublicationSchema, many=True)
    factors = fields.Nested(StudyFactorSchema, many=True)
    protocols = fields.Nested(ProtocolSchema, many=True)
    assays = fields.Nested(AssaySchema, many=True)
    sources = fields.Nested(SourceSchema, many=True)
    samples = fields.Nested(SampleSchema, many=True)
    other_materials = fields.Nested(OtherMaterialSchema, many=True,
                                    load_from='otherMaterials', dump_to='otherMaterials')
    process_sequence = fields.Nested(ProcessSchema, many=True,
                                     load_from='processSequence', dump_to='processSequence')
    characteristic_categories = fields.Nested(OntologyAnnotationSchema, many=True,
                                              load_from='characteristicCategories',
                                              dump_to='characteristicCategories')
    units = fields.Nested(OntologyAnnotationSchema, many=True)

    @post_load
    def make_obj(self, data):
        return Study(**data)

    # add an envelope to responses
    @post_dump(pass_many=True)
    def set_envelop(self, data, many):
        if 'study' in self.context:
            key = 'studies' if many else 'study'
            return {
                key: data
            }


class IsaInvestigationSchema(IsaSchema):
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
    # comments                                                  (list: Comment)

    identifier = fields.Str()
    title = fields.Str()
    description = fields.Str()
    submission_date = fields.Str(load_from='submissionDate', dump_to='submissionDate')
    public_release_date = fields.Str(load_from='publicReleaseDate', dump_to='publicReleaseDate')
    filename = fields.Str()
    contacts = fields.Nested(PersonSchema, many=True, load_from='people', dump_to='people')
    publications = fields.Nested(PublicationSchema, many=True)
    ontology_source_references = fields.Nested(OntologySourceSchema, many=True, load_from='ontologySourceReferences', dump_to='ontologySourceReferences')
    studies = fields.Nested(StudySchema, many=True)

    @post_load
    def make_obj(self, data):
        return Investigation(**data)
