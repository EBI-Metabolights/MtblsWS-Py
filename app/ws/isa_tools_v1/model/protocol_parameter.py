from app.ws.isa_tools_v1.model.comments import Commentable
from app.ws.isa_tools_v1.model.ontology_annotation import OntologyAnnotation
from app.ws.isa_tools_v1.model.identifiable import Identifiable


class ProtocolParameter(Commentable, Identifiable):
    """A parameter used by a protocol.

    Attributes:
        parameter_name: A parameter name as an ontology term
        comments: Comments associated with instances of this class.
    """

    def __init__(self, id_='', parameter_name=None, comments=None):
        super().__init__(comments=comments)
        self.id = id_
        self.__parameter_name = None
        self.parameter_name = parameter_name

    @property
    def parameter_name(self):
        """:obj:`OntologyAnnotation`: an ontology annotation representing the
        parameter name"""
        return self.__parameter_name

    @parameter_name.setter
    def parameter_name(self, val):
        if val is None or isinstance(val, OntologyAnnotation):
            self.__parameter_name = val
        elif isinstance(val, str):
            self.__parameter_name = OntologyAnnotation(term=val)
        else:
            error_msg = ('ProtocolParameter.parameter_name must be either a string or an OntologyAnnotation or None; '
                         'got {0}:{1}').format(val, type(val))
            raise AttributeError(error_msg)

    def __repr__(self):
        return ('app.ws.isa_tools_v1.model.ProtocolParameter('
                'parameter_name={parameter_name}, '
                'comments={parameter.comments})').format(parameter=self, parameter_name=repr(self.parameter_name))

    def __str__(self):
        parameter_name = self.parameter_name.term if self.parameter_name else ''
        return ("ProtocolParameter(\n\t"
                "parameter_name={parameter_name}\n\t"
                "comments={num_comments} Comment objects\n)"
                ).format(parameter_name=parameter_name, num_comments=len(self.comments))

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return (isinstance(other, ProtocolParameter)
                and self.parameter_name == other.parameter_name
                and self.comments == other.comments)

    def __ne__(self, other):
        return not self == other

    def to_dict(self, ld=False):
        protocol_parameter = {
            '@id': self.id,
            'parameterName': self.parameter_name.to_dict(ld=ld)
        }
        return self.update_isa_object(protocol_parameter, ld=ld)

    def from_dict(self, protocol_parameter):
        self.id = protocol_parameter.get('@id', '')
        self.load_comments(protocol_parameter.get('comments', ''))

        parameter_name_data = protocol_parameter.get('parameterName', None)
        if parameter_name_data:
            parameter_name = OntologyAnnotation()
            parameter_name.from_dict(parameter_name_data)
            self.parameter_name = parameter_name
