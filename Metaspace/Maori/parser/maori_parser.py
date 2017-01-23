import os
import json
from Metaspace.Maori.model import maoriStudy

"""
METASPACE MaoriParser

Parser for JSON-based metadata for METASPACE Maori

author: jrmacias
date: 20170123
"""


class MaoriParser:

    def parse_maori(self, in_filename):
        assert os.path.exists(in_filename), "Did not find json input file: %s" % in_filename
        with open(in_filename, 'r', encoding='utf-8') as in_handle:
            return self.read_json(in_handle)

    def read_json(self, in_handle):
            maori_json = json.load(in_handle)
            return maori_json

    def get_maori_study(self, maori_json):
        # Required
        assert 'Sample_Information' in maori_json
        assert 'Sample_Preparation' in maori_json
        assert 'MS_Analysis' in maori_json
        assert 'Submitted_By' in maori_json
        assert 'metaspace_options' in maori_json
        assert 'Additional_Information' in maori_json
        maori_study = maoriStudy.MaoriStudy()
        maori_study.Sample_Information = self.get_sample_information(maori_json['Sample_Information'])
        maori_study.SamplePreparation = self.get_sample_preparation(maori_json['Sample_Preparation'])
        maori_study.MsAnalysis = self.get_ms_analysis(maori_json['MS_Analysis'])
        maori_study.SubmittedBy = self.get_submitted_by(maori_json['Submitted_By'])
        maori_study.MetaspaceOptions = self.get_metaspace_options(maori_json['metaspace_options'])
        maori_study.AdditionalInformation = self.get_additional_information(maori_json['Additional_Information'])
        return maori_study

    def get_sample_information(self, sub_json):
        # Required
        assert 'Organism' in sub_json
        assert 'Organism_Part' in sub_json
        assert 'Condition' in sub_json
        sample_information = maoriStudy.SampleInformation()
        sample_information.Organism = sub_json['Organism']
        sample_information.OrganismPart = sub_json['Organism_Part']
        sample_information.Condition = sub_json['Condition']
        # Optional
        if 'Sample_Growth_Conditions' in sub_json:
            sample_information.SampleGrowthConditions = sub_json['Sample_Growth_Conditions']
        return sample_information

    def get_sample_preparation(self, sub_json):
        # Required
        assert 'Sample_Stabilisation' in sub_json
        assert 'Tissue_Modification' in sub_json
        assert 'MALDI_Matrix' in sub_json
        assert 'MALDI_Matrix_Application' in sub_json
        sample_preparation = maoriStudy.SamplePreparation()
        sample_preparation.SampleStabilisation = sub_json['Sample_Stabilisation']
        sample_preparation.TissueModification = sub_json['Tissue_Modification']
        sample_preparation.MALDIMatrix = sub_json['MALDI_Matrix']
        sample_preparation.MALDIMatrixApplication = sub_json['MALDI_Matrix_Application']
        return sample_preparation

    def get_ms_analysis(self, sub_json):
        # Required
        assert 'Polarity' in sub_json
        assert 'Ionisation_Source' in sub_json
        assert 'Analyzer' in sub_json
        ms_analysis = maoriStudy.MsAnalysis()
        ms_analysis.Polarity = sub_json['Polarity']
        ms_analysis.IonisationSource = sub_json['Ionisation_Source']
        ms_analysis.Analyzer = sub_json['Analyzer']
        # Optional
        if 'Detector_Resolving_Power' in sub_json:
            # Required
            assert 'mz' in sub_json['Detector_Resolving_Power']
            assert 'Resolving_Power' in sub_json['Detector_Resolving_Power']
            ms_analysis.DetectorResolvingPower = maoriStudy.DetectorResolvingPower()
            ms_analysis.DetectorResolvingPower.mz = sub_json['Detector_Resolving_Power']['mz']
            ms_analysis.DetectorResolvingPower.ResolvingPower = sub_json['Detector_Resolving_Power']['Resolving_Power']
        return ms_analysis

    def get_submitted_by(self, sub_json):
        submitted_by = maoriStudy.SubmittedBy()
        # Required
        assert 'Institution' in sub_json
        assert 'Submitter' in sub_json
        assert 'Surname' in sub_json['Submitter']
        assert 'First_Name' in sub_json['Submitter']
        assert 'Email' in sub_json['Submitter']
        assert 'Principal_Investigator' in sub_json
        assert 'Surname' in sub_json['Principal_Investigator']
        assert 'First_Name' in sub_json['Principal_Investigator']
        assert 'Email' in sub_json['Principal_Investigator']
        submitted_by.Institution = sub_json['Institution']
        submitted_by.Submitter.Surname = sub_json['Submitter']['Surname']
        submitted_by.Submitter.First_Name = sub_json['Submitter']['First_Name']
        submitted_by.Submitter.Email = sub_json['Submitter']['Email']
        submitted_by.PrincipalInvestigator.Surname = sub_json['Principal_Investigator']['Surname']
        submitted_by.PrincipalInvestigator.First_Name = sub_json['Principal_Investigator']['First_Name']
        submitted_by.PrincipalInvestigator.Email = sub_json['Principal_Investigator']['Email']
        return submitted_by

    def get_metaspace_options(self, sub_json):
        # Required
        assert 'Metabolite_Database' in sub_json
        assert 'Dataset_Name' in sub_json
        metaspace_options = maoriStudy.MetaspaceOptions()
        metaspace_options.MetaboliteDatabase = sub_json['Metabolite_Database']
        metaspace_options.DatasetName = sub_json['Dataset_Name']
        # Optional
        metaspace_options.AlphaTester = sub_json['Alpha-tester']
        return metaspace_options

    def get_additional_information(self, sub_json):
        additional_information = maoriStudy.AdditionalInformation()
        # Optional
        additional_information.PublicationDOI = sub_json['Publication_DOI']
        additional_information.SampleDescriptionFreetext = sub_json['Sample_Description_Freetext']
        additional_information.SamplePreparationFreetext = sub_json['Sample_Preparation_Freetext']
        additional_information.AdditionalInformationFreetext = sub_json['Additional_Information_Freetext']
        if 'Expected_Molecules_Freetext' in sub_json:
            additional_information.ExpectedMoleculesFreetext = sub_json['Expected_Molecules_Freetext']
        return additional_information


# mParser = MaoriParser()
# mObj = mParser.parse_maori(os.path.normpath(os.path.join('../', "test_data/maori_sample.json")))
# print(mObj)
# mObj = mParser.parse_maori(os.path.normpath(os.path.join('../', "test_data/failing_sample_file.json")))
# print(mObj)
# mObj = mParser.parse_maori(os.path.normpath(os.path.join('../', "test_data/passing_sample_file.json")))
# print(mObj)
# ms = mParser.get_maori_study(mObj)
# print(ms.AdditionalInformation.PublicationDOI)
