import pandas


class DataFrameUtils:

    @staticmethod
    def NMR_assay_cleanup(df: pandas.DataFrame):
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
    def LCMS_assay_cleanup(df: pandas.DataFrame):
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
        k = k.append(df, sort=False)
        df = k[keep]
        df = df.rename(columns=rename)
        return df


    @staticmethod
    def sample_cleanup(df):
        '''
        Change / mapping sample file dataframe column name
        :param df:
        :return:
        '''
        keep = ['Study', 'Characteristics[Organism]', 'Characteristics[Organism part]', 'Protocol REF', 'Sample Name']
        rename = {'Characteristics[Organism]': 'Characteristics.Organism.',
                  'Characteristics[Organism part]': 'Characteristics.Organism.part.',
                  'Protocol REF': 'Protocol.REF',
                  'Sample Name': 'Sample.Name'}

        k = pandas.DataFrame(columns=keep)
        k = k.append(df, sort=False)
        df = k[keep]
        df = df.rename(columns=rename)
        return df

