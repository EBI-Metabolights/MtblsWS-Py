class IsaApiWrapper:
    """Wrapper to hold all the information returned from the ISA API Client."""
    def __init__(self, isa_study, isa_inv, std_path, file_name, isa_sample_df, s_file, assays, assay_files):
        self._isa_study = isa_study
        self._isa_inv = isa_inv
        self._std_path = std_path
        self._file_name = file_name
        self._isa_sample_df = isa_sample_df
        self._s_file = s_file
        self._assays = assays
        self._assay_files = assay_files

    def is_complete(self):
        return self._isa_study is not None

    @property
    def isa_study(self):
        return self._isa_study

    @property
    def isa_sample_df(self):
        return self._isa_sample_df

    @property
    def file_name(self):
        return self._file_name

    @property
    def assays(self):
        return self._assays

    @property
    def isa_inv(self):
        return self._isa_inv

