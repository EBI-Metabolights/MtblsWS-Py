import pyopenms
from flask_restful import Resource


class ExtractMSSpectra(Resource):
    #  See: https://pypi.org/project/pyopenms/

    def get_spectrum(self, filepath):
        peak_list = []
        exp = pyopenms.MSExperiment()
        pyopenms.FileHandler().loadExperiment(filepath, exp)
        for spectrum in exp:
            for peak in spectrum:
                peak_list.append(peak.getMZ(), peak.getIntensity())

        return peak_list

    def create_mtbls_peak_list(self, filepath):
        peaks = self.get_spectrum(self, filepath)

