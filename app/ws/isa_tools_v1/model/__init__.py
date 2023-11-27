"""ISA Model 1.0 implementation in Python.

This module implements the ISA Abstract Model 1.0 as Python classes, as
specified in the `ISA Model and Serialization Specifications 1.0`_, and
additional classes to support compatibility between ISA-Tab and ISA-JSON.

Todo:
    * Check consistency with published ISA Model
    * Finish docstringing rest of the module
    * Add constraints on attributes throughout, and test

.. _ISA Model and Serialization Specs 1.0: http://isa-specs.readthedocs.io/

"""
from app.ws.isa_tools_v1.model.assay import Assay
from app.ws.isa_tools_v1.model.characteristic import Characteristic
from app.ws.isa_tools_v1.model.comments import Commentable, Comment
from app.ws.isa_tools_v1.model.context import set_context
from app.ws.isa_tools_v1.model.datafile import (
    DataFile,
    RawDataFile,
    DerivedDataFile,
    RawSpectralDataFile,
    DerivedArrayDataFile,
    ArrayDataFile,
    DerivedSpectralDataFile,
    ProteinAssignmentFile,
    PeptideAssignmentFile,
    DerivedArrayDataMatrixFile,
    PostTranslationalModificationAssignmentFile,
    AcquisitionParameterDataFile,
    FreeInductionDecayDataFile
)
from app.ws.isa_tools_v1.model.factor_value import FactorValue, StudyFactor
from app.ws.isa_tools_v1.model.investigation import Investigation
from app.ws.isa_tools_v1.model.logger import log
from app.ws.isa_tools_v1.model.material import Material, Extract, LabeledExtract
from app.ws.isa_tools_v1.model.mixins import MetadataMixin, StudyAssayMixin, _build_assay_graph
from app.ws.isa_tools_v1.model.ontology_annotation import OntologyAnnotation
from app.ws.isa_tools_v1.model.ontology_source import OntologySource
from app.ws.isa_tools_v1.model.parameter_value import ParameterValue
from app.ws.isa_tools_v1.model.person import Person
from app.ws.isa_tools_v1.model.process import Process
from app.ws.isa_tools_v1.model.process_sequence import ProcessSequenceNode
from app.ws.isa_tools_v1.model.protocol import Protocol, load_protocol_types_info
from app.ws.isa_tools_v1.model.protocol_component import ProtocolComponent
from app.ws.isa_tools_v1.model.protocol_parameter import ProtocolParameter
from app.ws.isa_tools_v1.model.publication import Publication
from app.ws.isa_tools_v1.model.sample import Sample
from app.ws.isa_tools_v1.model.source import Source
from app.ws.isa_tools_v1.model.study import Study
from app.ws.isa_tools_v1.model.logger import log
from app.ws.isa_tools_v1.model.utils import _build_assay_graph, plink, batch_create_assays, batch_create_materials, _deep_copy
