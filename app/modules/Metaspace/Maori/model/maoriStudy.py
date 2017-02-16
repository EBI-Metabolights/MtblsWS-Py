

class MaoriStudy():
    """A Study coming from METASPACE Maori-Upload

    Attributes:
        SampleInformation
        SamplePreparation
        MsAnalysis
        SubmittedBy
        MetaspaceOptions
        AdditionalInformation

    """

    def __init__(self):
        self.Sample_Information = SampleInformation()
        self.SamplePreparation = SamplePreparation()
        self.MsAnalysis = MsAnalysis()
        self.SubmittedBy = SubmittedBy()
        self.MetaspaceOptions = MetaspaceOptions()
        self.AdditionalInformation = AdditionalInformation()


class SampleInformation():
    def __init__(self):
        self.Organism = ""                  # Required
        self.OrganismPart = ""              # Required
        self.Condition = ""                 # Required
        self.SampleGrowthConditions = ""    # Optional


class SamplePreparation():
    def __init__(self):
        self.SampleStabilisation = ""       # Required
        self.TissueModification = ""        # Required
        self.MALDIMatrix = ""               # Required
        self.MALDIMatrixApplication = ""    # Required


class MsAnalysis():
    def __init__(self):
        self.Polarity = ""                  # Required
        self.IonisationSource = ""          # Required
        self.Analyzer = ""                  # Required
        self.DetectorResolvingPower = DetectorResolvingPower()  # Optional


class DetectorResolvingPower():
    def __init__(self):
        self.mz = ""                        # Required
        self.ResolvingPower = ""            # Required


class SubmittedBy():
    def __init__(self):
        self.Institution = ""                   # Required
        self.Submitter = Person()               # Optional
        self.PrincipalInvestigator = Person()   # Optional


class Person():
    def __init__(self):
        self.Surname = ""                   # Required
        self.First_Name = ""                # Required
        self.Email = ""                     # Required


class MetaspaceOptions():
    def __init__(self):
        self.MetaboliteDatabase = ""        # Required
        self.DatasetName = ""               # Required
        self.AlphaTester = ""               # Optional


class AdditionalInformation():
    def __init__(self):
        self.PublicationDOI = ""                    # Optional
        self.SampleDescriptionFreetext = ""         # Optional
        self.SamplePreparationFreetext = ""         # Optional
        self.AdditionalInformationFreetext = ""     # Optional
        self.ExpectedMoleculesFreetext = ""         # Optional
