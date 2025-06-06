import logging
import pandas

logger = logging.getLogger('builder')


class DataFrameUtils:

    @staticmethod
    def NMR_assay_cleanup(df: pandas.DataFrame) -> pandas.DataFrame:
        '''
        Change / mapping NMR Study dataframe column name
        :param df: NMR Dataframe to cleanup
        :return: DataFrame with renamed columns and unwanted columns removed.
        '''
        keep = ['Study', 'Sample Name', 'Protocol REF', 'Protocol REF.1',
                'Parameter Value[NMR tube type]', 'Parameter Value[Solvent]',
                'Parameter Value[Sample pH]', 'Parameter Value[Temperature]', 'Unit',
                'Label', 'Protocol REF.2', 'Parameter Value[Instrument]',
                'Parameter Value[NMR Probe]', 'Parameter Value[Number of transients]',
                'Parameter Value[Pulse sequence name]',
                'Acquisition Parameter Data File', 'Protocol REF.3', 'NMR Assay Name',
                'Free Induction Decay Data File', 'Protocol REF.4',
                'Derived Spectral Data File', 'Protocol REF.5',
                'Data Transformation Name', 'Metabolite Assignment File']

        rename = {'Sample Name': 'Sample.Name', 'Protocol REF': 'Protocol.REF.0', 'Protocol REF.1': 'Protocol.REF.1',
                  'Parameter Value[NMR tube type]': 'Parameter.Value.NMR.tube.type.',
                  'Parameter Value[Solvent]': 'Parameter.Value.Solvent.',
                  'Parameter Value[Sample pH]': 'Parameter.Value.Sample.pH.',
                  'Parameter Value[Temperature]': 'Parameter.Value.Temperature.', 'Unit': 'Unit', 'Label': 'Label',
                  'Protocol REF.2': 'Protocol.REF.2', 'Parameter Value[Instrument]': 'Parameter.Value.Instrument.',
                  'Parameter Value[NMR Probe]': 'Parameter.Value.NMR.Probe.',
                  'Parameter Value[Number of transients]': 'Parameter.Value.Number.of.transients.',
                  'Parameter Value[Pulse sequence name]': 'Parameter.Value.Pulse.sequence.name.',
                  'Acquisition Parameter Data File': 'Acquisition.Parameter.Data.File',
                  'Protocol REF.3': 'Protocol.REF.3',
                  'NMR Assay Name': 'NMR.Assay.Name',
                  'Free Induction Decay Data File': 'Free.Induction.Decay.Data.File',
                  'Protocol REF.4': 'Protocol.REF.4', 'Derived Spectral Data File': 'Derived.Spectral.Data.File',
                  'Protocol REF.5': 'Protocol.REF.5', 'Data Transformation Name': 'Data.Transformation.Name',
                  'Metabolite Assignment File': 'Metabolite.Assignment.File'}
        df = df[keep]
        df = df.rename(columns=rename)
        return df

    @staticmethod
    def LCMS_assay_cleanup(df: pandas.DataFrame) -> pandas.DataFrame:
        '''
        Change / mapping LCMS Study dataframe column name
        :param df: LC** DataFrame to cleanup
        :return: DataFrame with renamed columns and unwanted columns removed.
        '''
        keep = ['Study', 'Sample Name', 'Protocol REF', 'Parameter Value[Post Extraction]',
                'Parameter Value[Derivatization]', 'Extract Name', 'Protocol REF.1',
                'Parameter Value[Chromatography Instrument]',
                'Parameter Value[Column model]', 'Parameter Value[Column type]',
                'Labeled Extract Name', 'Label', 'Protocol REF.2',
                'Parameter Value[Scan polarity]', 'Parameter Value[Scan m/z range]',
                'Parameter Value[Instrument]', 'Parameter Value[Ion source]',
                'Parameter Value[Mass analyzer]', 'MS Assay Name',
                'Raw Spectral Data File', 'Protocol REF.3', 'Normalization Name',
                'Derived Spectral Data File', 'Protocol REF.4',
                'Data Transformation Name', 'Metabolite Assignment File']

        rename = {'Study': 'Study',
                  'Sample Name': 'Sample.Name',
                  'Protocol REF': 'Protocol.REF.0',
                  'Parameter Value[Post Extraction]': 'Parameter.Value.Post.Extraction.',
                  'Parameter Value[Derivatization]': 'Parameter.Value.Derivatization.',
                  'Extract Name': 'Extract.Name',
                  'Protocol REF.1': 'Protocol.REF.1',
                  'Parameter Value[Chromatography Instrument]': 'Parameter.Value.Chromatography.Instrument.',
                  'Parameter Value[Column model]': 'Parameter.Value.Column.model.',
                  'Parameter Value[Column type]': 'Parameter.Value.Column.type.',
                  'Labeled Extract Name': 'Labeled.Extract.Name',
                  'Label': 'Label',
                  'Protocol REF.2': 'Protocol.REF.2',
                  'Parameter Value[Scan polarity]': 'Parameter.Value.Scan.polarity.',
                  'Parameter Value[Scan m/z range]': 'Parameter.Value.Scan.m/z.range.',
                  'Parameter Value[Instrument]': 'Parameter.Value.Instrument.',
                  'Parameter Value[Ion source]': 'Parameter.Value.Ion.source.',
                  'Parameter Value[Mass analyzer]': 'Parameter.Value.Mass.analyzer.',
                  'MS Assay Name': 'MS.Assay.Name',
                  'Raw Spectral Data File': 'Raw.Spectral.Data.File',
                  'Protocol REF.3': 'Protocol.REF.3',
                  'Normalization Name': 'Normalization.Name',
                  'Derived Spectral Data File': 'Derived.Spectral.Data.File',
                  'Protocol REF.4': 'Protocol.REF.4',
                  'Data Transformation Name': 'Data.Transformation.Name',
                  'Metabolite Assignment File': 'Metabolite.Assignment.File'}
        k = pandas.DataFrame(columns=keep)
        k = pandas.concat([k, df], sort=False)
        df = k[keep]
        df = df.rename(columns=rename)
        return df


    @staticmethod
    def sample_cleanup(df) -> pandas.DataFrame:
        '''
        Change / mapping sample file dataframe column name
        :param df: Sample file to cleanup
        :return: Dataframe with with renamed columns and unwanted columns removed
        '''
        keep = ['Study', 'Characteristics[Organism]', 'Characteristics[Organism part]', 'Protocol REF', 'Sample Name']
        rename = {'Characteristics[Organism]': 'Characteristics.Organism.',
                  'Characteristics[Organism part]': 'Characteristics.Organism.part.',
                  'Protocol REF': 'Protocol.REF',
                  'Sample Name': 'Sample.Name'}

        k = pandas.DataFrame(columns=keep)
        k = pandas.concat([k, df], sort=False)
        df = k[keep]
        df = df.rename(columns=rename)
        return df

    @staticmethod
    def collapse(df):
        num_rows = len(df.index)
        num_cols = len(df.columns)
        df.drop(df.index[1:])
        new_col_msg = f'Rows collapsed: {num_rows}'
        df.insert(num_cols, 'Summary', new_col_msg)
        return df

    @staticmethod
    def get_column_headers_by_detection_type(studytype):
        """Return column header based on the detection method in the study. Broadly assumes there are only two categories,
        LCMS and all its variants, and NMR. GC-MS is not accounted for yet, as we currently aren't interested in analysing
        that data. It would only take the selection of the relevant column headers to implement."""
        if studytype.count('LC') > 0:
            return \
                pandas.DataFrame(columns=['Study', 'Sample.Name', 'Protocol.REF.0', 'Parameter.Value.Post.Extraction.',
                                          'Parameter.Value.Derivatization.', 'Extract.Name', 'Protocol.REF.1',
                                          'Parameter.Value.Chromatography.Instrument.', 'Parameter.Value.Column.model.',
                                          'Parameter.Value.Column.type.', 'Labeled.Extract.Name', 'Label',
                                          'Protocol.REF.2',
                                          'Parameter.Value.Scan.polarity.', 'Parameter.Value.Scan.m/z.range.',
                                          'Parameter.Value.Instrument.', 'Parameter.Value.Ion.source.',
                                          'Parameter.Value.Mass.analyzer.', 'MS.Assay.Name', 'Raw.Spectral.Data.File',
                                          'Protocol.REF.3', 'Normalization.Name', 'Derived.Spectral.Data.File',
                                          'Protocol.REF.4',
                                          'Data.Transformation.Name', 'Metabolite.Assignment.File'])
        else:
            return pandas.DataFrame(columns=['Study', 'Sample.Name', 'Protocol.REF.0', 'Protocol.REF.1',
                                             'Parameter.Value.NMR.tube.type.', 'Parameter.Value.Solvent.',
                                             'Parameter.Value.Sample.pH.', 'Parameter.Value.Temperature.', 'Unit',
                                             'Label', 'Protocol.REF.2', 'Parameter.Value.Instrument.',
                                             'Parameter.Value.NMR.Probe.', 'Parameter.Value.Number.of.transients.',
                                             'Parameter.Value.Pulse.sequence.name.',
                                             'Acquisition.Parameter.Data.File', 'Protocol.REF.3', 'NMR.Assay.Name',
                                             'Free.Induction.Decay.Data.File', 'Protocol.REF.4',
                                             'Derived.Spectral.Data.File', 'Protocol.REF.5',
                                             'Data.Transformation.Name', 'Metabolite.Assignment.File'])



    @staticmethod
    def LCMS_maf_cleanup(df: pandas.DataFrame, study_id: str, maf_filename: str) -> pandas.DataFrame:
        """
        Remove any columns we don't want from the LCMS maf file, and rename the others so as to make them more receptive
        to being computed.

        :param df: LCMS maf file to clean up.
        :return: Dataframe with renamed columns and unwanted columns removed.
        """
        keep = ['database_identifier', 'chemical_formula', 'inchi', 'metabolite_identification', 'mass_to_charge',
                'fragmentation', 'modification', 'charge', 'retention_time', 'taxid', 'species', 'database',
                'database_version', 'reliability', 'uri', 'search_engine', 'search_engine_score',
                'smallmolecule_abundance_sub', 'smallmolecule_abundance_stdev_sub',
                'smallmolecule_abundance_std_error_sub']
        k = pandas.DataFrame(columns=keep)
        k = pandas.concat([k, df], sort=False)
        df = k[keep]
        df.insert(0, 'maf_filename', maf_filename)
        df.insert(0, 'study_id', study_id)
        return df

    @staticmethod
    def NMR_maf_cleanup(df: pandas.DataFrame, study_id: str, maf_filename: str) -> pandas.DataFrame:
        """
        Remove any columns we don't want from the LCMS maf file, and rename the others so as to make them more receptive
        to being computed.

        :param df: NMR maf file to clean up.
        :param study_id: The accession number of the study that the maf belongs to.
        :param maf_filename: Maf filename to save as a new column in the dataframe.
        :return: Dataframe with renamed columns and unwanted columns removed.
        """
        logger.info('hit NMR MAF cleanup')
        keep = ['database_identifier', 'chemical_formula', 'smiles', 'inchi', 'metabolite_identification',
                'chemical_shift', 'multiplicity', 'taxid', 'species', 'database', 'database_version',
                'reliability','uri', 'search_engine', 'search_engine_score', 'smallmolecule_abundance_sub',
                'smallmolecule_abundance_stdev_sub']
        k = pandas.DataFrame(columns=keep)
        try:
            k = pandas.concat([k, df], sort=False)
            df = k[keep]
            df.insert(0, 'maf_filename', maf_filename)
            df.insert(0, 'study_id', study_id)
        except Exception as e:
            logger.error(f'unexpected issue with cleaning up dataframe: {str(e)}')
        return df