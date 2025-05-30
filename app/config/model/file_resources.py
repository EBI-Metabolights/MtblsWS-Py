from pydantic import BaseModel


class FileResources(BaseModel):
    mtbls_ontology_file: str
    mtbls_zooma_file: str
    mzml_xsd_schema_file_path: str
    validations_file: str

    study_default_template_path: str
    study_partner_metabolon_template_path: str
    study_mass_spectrometry_maf_file_template_path: str = ".resources/m_metabolite_profiling_mass_spectrometry_v2_maf.tsv"
    study_nmr_spectroscopy_maf_file_template_path: str = ".resources/m_metabolite_profiling_NMR_spectrometry_v2_maf.tsv"