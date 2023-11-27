from app.ws.isa_tools_v1.model.comments import Commentable
from app.ws.isa_tools_v1.model.ontology_annotation import OntologyAnnotation
from app.ws.isa_tools_v1.model.characteristic import Characteristic
from app.ws.isa_tools_v1.model.source import Source
from app.ws.isa_tools_v1.model.process_sequence import ProcessSequenceNode
from app.ws.isa_tools_v1.model.factor_value import FactorValue
from app.ws.isa_tools_v1.model.identifiable import Identifiable
from app.ws.isa_tools_v1.model.loader_indexes import loader_states as indexes


class Sample(Commentable, ProcessSequenceNode, Identifiable):
    """Represents a Sample material in an experimental graph.

    Attributes:
        name: A name/reference for the sample material.
        characteristics: A list of Characteristics used to qualify the material
            properties.
        factor_values: A list of FactorValues used to qualify the material in
            terms of study factors/design.
        derives_from: A link to the source material that the sample is derived
            from.
        comments: Comments associated with instances of this class.
    """

    def __init__(self, name='', id_='', factor_values=None,
                 characteristics=None, derives_from=None, comments=None):
        Commentable.__init__(self, comments)
        ProcessSequenceNode.__init__(self)
        Identifiable.__init__(self)

        self.id = id_
        self.__name = name
        self.__factor_values = []
        self.__characteristics = []
        self.__derives_from = []

        if factor_values:
            self.__factor_values = factor_values
        if characteristics:
            self.__characteristics = characteristics
        if derives_from:
            self.__derives_from = derives_from

    @property
    def name(self):
        """:obj:`str`: the name of the sample material"""
        return self.__name

    @name.setter
    def name(self, val):
        if val is not None and not isinstance(val, str):
            raise AttributeError('Sample.name must be a str or None; got {0}:{1}'.format(val, type(val)))
        self.__name = val

    @property
    def factor_values(self):
        """:obj:`list` of :obj:`FactorValue`: Container for sample material
        factor_values"""
        return self.__factor_values

    @factor_values.setter
    def factor_values(self, val):
        if val is not None and hasattr(val, '__iter__'):
            if val == [] or all(isinstance(x, FactorValue) for x in val):
                self.__factor_values = list(val)
        else:
            raise AttributeError('Sample.factor_values must be iterable containing FactorValues')

    @property
    def characteristics(self):
        """:obj:`list` of :obj:`Characteristic`: Container for sample material
        characteristics"""
        return self.__characteristics

    @characteristics.setter
    def characteristics(self, val):
        if val is not None and hasattr(val, '__iter__'):
            if val == [] or all(isinstance(x, Characteristic) for x in val):
                self.__characteristics = list(val)
        else:
            raise AttributeError('Sample.characteristics must be iterable containing Characteristics')

    def has_char(self, char):
        if isinstance(char, str):
            char = Characteristic(category=OntologyAnnotation(term=char))
        if isinstance(char, Characteristic):
            return char in self.characteristics
        return False

    def get_char(self, name):
        hits = [x for x in self.characteristics if x.category.term == name]
        try:
            result = next(iter(hits))
        except StopIteration:
            result = None
        return result

    @property
    def derives_from(self):
        """:obj:`list` of :obj:`Source`: a list of references from this sample
        material to a source material(s)"""
        return self.__derives_from

    @derives_from.setter
    def derives_from(self, val):
        if val is not None and hasattr(val, '__iter__'):
            if val == [] or all(isinstance(x, Source) for x in val):
                self.__derives_from = list(val)
        else:
            raise AttributeError(
                'Sample.derives_from must be iterable containing Sources')

    def __repr__(self):
        return ("app.ws.isa_tools_v1.model.Sample(name='{sample.name}', "
                "characteristics={sample.characteristics}, "
                "factor_values={sample.factor_values}, "
                "derives_from={sample.derives_from}, "
                "comments={sample.comments})").format(sample=self)

    def __str__(self):
        return ("Sample(\n\t"
                "name={sample.name}\n\t"
                "characteristics={num_characteristics} Characteristic objects\n\t"
                "factor_values={num_factor_values} FactorValue objects\n\t"
                "derives_from={num_derives_from} Source objects\n\t"
                "comments={num_comments} Comment objects\n)"
                ).format(sample=self,
                         num_characteristics=len(self.characteristics),
                         num_factor_values=len(self.factor_values),
                         num_derives_from=len(self.derives_from),
                         num_comments=len(self.comments))

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return isinstance(other, Sample) \
               and self.name == other.name \
               and self.characteristics == other.characteristics \
               and self.factor_values == other.factor_values \
               and self.derives_from == other.derives_from \
               and self.comments == other.comments

    def __ne__(self, other):
        return not self == other

    def to_dict(self, ld=False):
        sample = {
            "@id": self.id,
            "name": self.name,
            "characteristics": [characteristic.to_dict(ld=ld) for characteristic in self.characteristics],
            "factorValues": [factor_values.to_dict(ld=ld) for factor_values in self.factor_values],
            "derivesFrom": [{"@id": derives_from.id} for derives_from in self.derives_from],
            "comments": [comment.to_dict(ld=ld) for comment in self.comments]
        }
        return self.update_isa_object(sample, ld)

    def from_dict(self, sample):
        self.id = sample.get('@id', '')
        self.name = sample.get('name', '').replace('sample-', '')
        self.load_comments(sample.get('comments', []))

        # characteristics
        for characteristic_data in sample.get('characteristics', []):
            id_ = characteristic_data.get('category', {}).get('@id', '')
            data = {
                'comments': characteristic_data.get('comments', []),
                'category': indexes.get_characteristic_category(id_),
                'value': characteristic_data['value'],
                'unit': characteristic_data.get('unit', '')
            }
            characteristic = Characteristic()
            characteristic.from_dict(data)
            self.characteristics.append(characteristic)

        # factor values
        for factor_value_data in sample.get('factorValues', []):
            factor = FactorValue()
            factor.from_dict(factor_value_data)
            self.factor_values.append(factor)

        for derives_data in sample.get('derivesFrom', []):
            self.derives_from.append(indexes.get_source(derives_data["@id"]))
