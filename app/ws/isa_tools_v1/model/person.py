from app.ws.isa_tools_v1.model.comments import Commentable
from app.ws.isa_tools_v1.model.ontology_annotation import OntologyAnnotation
from app.ws.isa_tools_v1.model.identifiable import Identifiable


class Person(Commentable, Identifiable):
    """A person/contact that can be attributed to an Investigation or Study.

    Attributes:
        last_name: The last name of a person.
        first_name: The first name of a person.
        mid_initials: The middle initials of a person.
        email: The email address of a person.
        phone: The telephone number.
        fax: The fax number.
        address: The address of a person.
        affiliation: The organization affiliation for a person.
        roles: A list of role(s) performed by this person. Roles reported here
            need not correspond to roles held withing their affiliated
            organization.
        comments: Comments associated with instances of this class.
    """

    def __init__(self,
                 id_='',
                 last_name='',
                 first_name='',
                 mid_initials='',
                 email='',
                 phone='',
                 fax='',
                 address='',
                 affiliation='',
                 roles=None,
                 comments=None):
        super().__init__(comments=comments)

        self.id = id_
        self.__last_name = last_name
        self.__first_name = first_name
        self.__mid_initials = mid_initials
        self.__email = email
        self.__phone = phone
        self.__fax = fax
        self.__address = address
        self.__affiliation = affiliation

        self.__roles = []
        if roles:
            self.roles = roles

    @property
    def last_name(self):
        """:obj:`str`: the last_name of the person"""
        return self.__last_name

    @last_name.setter
    def last_name(self, val):
        if val is not None and not isinstance(val, str):
            raise AttributeError('Person.last_name must be a str or None; got {0}:{1}'
                                 .format(val, type(val)))
        self.__last_name = val

    @property
    def first_name(self):
        """:obj:`str`: the first_name of the person"""
        return self.__first_name

    @first_name.setter
    def first_name(self, val):
        if val is not None and not isinstance(val, str):
            raise AttributeError('Person.first_name must be a str or None; got {0}:{1}'
                                 .format(val, type(val)))
        self.__first_name = val

    @property
    def mid_initials(self):
        """:obj:`str`: the mid_initials of the person"""
        return self.__mid_initials

    @mid_initials.setter
    def mid_initials(self, val):
        if val is not None and not isinstance(val, str):
            raise AttributeError('Person.mid_initials must be a str or None; got {0}:{1}'
                                 .format(val, type(val)))
        self.__mid_initials = val

    @property
    def email(self):
        """:obj:`str`: the email of the person"""
        return self.__email

    @email.setter
    def email(self, val):
        if val is not None and not isinstance(val, str):
            raise AttributeError('Person.email must be a str or None; got {0}:{1}'
                                 .format(val, type(val)))
        self.__email = val

    @property
    def phone(self):
        """:obj:`str`: the phone number of the person"""
        return self.__phone

    @phone.setter
    def phone(self, val):
        if val is not None and not isinstance(val, str):
            raise AttributeError('Person.phone must be a str or None; got {0}:{1}'
                                 .format(val, type(val)))
        self.__phone = val

    @property
    def fax(self):
        """:obj:`str`: the fax number of the person"""
        return self.__fax

    @fax.setter
    def fax(self, val):
        if val is not None and not isinstance(val, str):
            raise AttributeError('Person.fax must be a str or None; got {0}:{1}'
                                 .format(val, type(val)))
        self.__fax = val

    @property
    def address(self):
        """:obj:`str`: the address of the person"""
        return self.__address

    @address.setter
    def address(self, val):
        if val is not None and not isinstance(val, str):
            raise AttributeError('Person.address must be a str or None; got {0}:{1}'
                                 .format(val, type(val)))
        self.__address = val

    @property
    def affiliation(self):
        """:obj:`str`: the affiliation of the person"""
        return self.__affiliation

    @affiliation.setter
    def affiliation(self, val):
        if val is not None and not isinstance(val, str):
            raise AttributeError('Person.affiliation must be a str or None; got {0}:{1}'
                                 .format(val, type(val)))
        self.__affiliation = val

    @property
    def roles(self):
        """:obj:`list` of :obj:`OntologyAnnotation`: Container for person roles
        """
        return self.__roles

    @roles.setter
    def roles(self, val):
        if val is not None and hasattr(val, '__iter__'):
            if val == [] or all(isinstance(x, OntologyAnnotation) for x in val):
                self.__roles = list(val)
        else:
            raise AttributeError('{0}.roles must be iterable containing OntologyAnnotations'
                                 .format(type(self).__name__))

    def __repr__(self):
        return ("app.ws.isa_tools_v1.model.Person("
                "last_name='{person.last_name}', " 
                "first_name='{person.first_name}', " 
                "mid_initials='{person.mid_initials}', " 
                "email='{person.email}', phone='{person.phone}', " 
                "fax='{person.fax}', address='{person.address}', " 
                "affiliation='{person.affiliation}', roles={person.roles}, " 
                "comments={person.comments})"
                ).format(person=self)

    def __str__(self):
        return ("Person(\n\t"
                "last_name={person.last_name}\n\t"
                "first_name={person.first_name}\n\t"
                "mid_initials={person.mid_initials}\n\t"
                "email={person.email}\n\t"
                "phone={person.phone}\n\t"
                "fax={person.fax}\n\t"
                "address={person.address}\n\t"
                "affiliation={person.affiliation}\n\t"
                "roles={num_roles} OntologyAnnotation objects\n\t"
                "comments={num_comments} Comment objects\n)"
                ).format(person=self,
                         num_roles=len(self.roles),
                         num_comments=len(self.comments))

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return (isinstance(other, Person)
                and self.last_name == other.last_name
                and self.first_name == other.first_name
                and self.mid_initials == other.mid_initials
                and self.email == other.email
                and self.phone == other.phone
                and self.fax == other.fax
                and self.address == other.address
                and self.affiliation == other.affiliation
                and self.roles == other.roles
                and self.comments == other.comments)

    def __ne__(self, other):
        return not self == other

    def to_dict(self, ld=False):
        person = {
            "address": self.address,
            "affiliation": self.affiliation,
            "comments": [comment.to_dict(ld=ld) for comment in self.comments],
            "email": self.email,
            "fax": self.fax,
            "firstName": self.first_name,
            "lastName": self.last_name,
            "midInitials": self.mid_initials,
            "phone": self.phone,
            "roles": [role.to_dict(ld=ld) for role in self.roles]
        }
        return self.update_isa_object(person, ld=ld)

    def from_dict(self, person):
        self.address = person['address'] if 'address' in person else ''
        self.affiliation = person['affiliation'] if 'affiliation' in person else ''
        self.email = person['email'] if 'email' in person else ''
        self.first_name = person['firstName'] if 'firstName' in person else ''
        self.last_name = person['lastName'] if 'lastName' in person else ''
        self.mid_initials = person['midInitials'] if 'midInitials' in person else ''
        self.phone = person['phone'] if 'phone' in person else ''
        self.fax = person['fax'] if 'fax' in person else ''

        self.load_comments(person.get('comments', []))

        # roles
        roles = []
        for role_data in person.get('roles', []):
            role = OntologyAnnotation()
            role.from_dict(role_data)
            roles.append(role)
        self.roles = roles
