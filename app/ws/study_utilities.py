
class StudyUtils:
    """A collection of utility methods common to the study resource and study service."""

    @staticmethod
    def is_template_file(filename: str) -> bool:
        """
        Assesses whether a file is a template file to be preserved.
        :return: bool indicating whether it's a template file.
        """
        return filename.startswith("i_Investigation") or filename.startswith("s_MTBLS")

    @staticmethod
    def overwrite_investigation_file():
        pass
