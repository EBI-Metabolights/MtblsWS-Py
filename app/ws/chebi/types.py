from enum import Enum


class SearchCategory(str, Enum):
    ALL = "ALL"
    CHEBI_ID = "CHEBI ID"
    CHEBI_NAME = "CHEBI NAME"
    DEFINITION = "DEFINITION"
    ALL_NAMES = "ALL NAMES"
    IUPAC_NAME = "IUPAC NAME"
    DATABASE_LINK_REGISTRY_NUMBER_CITATION = "DATABASE LINK/REGISTRY NUMBER/CITATION"
    FORMULA = "FORMULA"
    MASS = "MASS"
    MONOISOTOPIC_MASS = "MONOISOTOPIC MASS"
    CHARGE = "CHARGE"
    INCHI_INCHI_KEY = "INCHI/INCHI KEY"
    SMILES = "SMILES"
    SPECIES = "SPECIES"


class StarsCategory(str, Enum):
    ALL = "ALL"
    TWO_ONLY = "TWO ONLY"
    THREE_ONLY = "THREE ONLY"


class RelationshipType(str, Enum):
    IS_A = "is a"
    HAS_PART = "has part"
    IS_CONJUGATE_BASE_OF = "is conjugate base of"
    IS_CONJUGATE_ACID_OF = "is conjugate acid of"
    IS_TAUTOMER_OF = "is tautomer of"
    IS_ENANTIOMER_OF = "is enantiomer of"
    HAS_FUNCTIONAL_PARENT = "has functional parent"
    HAS_PARENT_HYDRIDE = "has parent hydride"
    IS_SUBSTITUENT_GROUP_FROM = "is substituent group from"
    HAS_ROLE = "has role"


class StructureType(str, Enum):
    MOLFILE = "MOLFILE"
    CML = "CML"
    SMILES = "SMILES"


class StructureSearchCategory(str, Enum):
    IDENTITY = "IDENTITY"
    SUBSTRUCTURE = "SUBSTRUCTURE"
    SIMILARITY = "SIMILARITY"
