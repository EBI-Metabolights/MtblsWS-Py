from flask import make_response


def metabolights_exception_handler(func):
    def exception_handler(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except MetabolightsException as e:
            data = {"content": None, "message": e.message, "err": str(e)}
            response = make_response(data, e.http_code)
            return response
        except Exception as e:
            data = {"content": None, "message": "Error while procession data", "err": str(e)}
            response = make_response(data, 400)
            return response

    return exception_handler


class MetabolightsException(Exception):

    def __init__(self, message: str = "", exception: Exception = None, http_code=400):
        super(MetabolightsException, self).__init__()
        self.message = message
        self.exception = exception
        self.http_code = http_code

    def __str__(self):
        if self.exception:
            return f"{str(self.__class__.__name__)}: {self.message}, http_code: {self.http_code} Cause -->: [{str(self.exception)}]"
        else:
            return f"{str(self.__class__.__name__)}: {self.message}, http_code: {self.http_code}"


class MetabolightsDBException(MetabolightsException):

    def __init__(self, message: str, exception: Exception = None, http_code=501):
        super(MetabolightsDBException, self).__init__(message, exception, http_code)


class MetabolightsFileOperationException(MetabolightsException):

    def __init__(self, message: str, exception: Exception = None, http_code=400):
        super(MetabolightsFileOperationException, self).__init__(message, exception, http_code)


class MetabolightsAuthorizationException(MetabolightsException):

    def __init__(self, message: str="", exception: Exception = None, http_code=401):
        super(MetabolightsAuthorizationException, self).__init__(message, exception, http_code)

    def __str__(self):
        if self.exception:
            return f"{str(self.__class__.__name__)}: {self.message}, http_code: {self.http_code} Cause -->: [{str(self.exception)}]"
        else:
            return f"{str(self.__class__.__name__)}: {self.message}, http_code: {self.http_code}"

INVESTIGATION_FILE_ROWS_LIST = [
    "ONTOLOGY SOURCE REFERENCE",
    "Term Source Name",
    "Term Source File",
    "Term Source Version",
    "Term Source Description",
    "INVESTIGATION",
    "Investigation Identifier",
    "Investigation Title",
    "Investigation Description",
    "Investigation Submission Date",
    "Investigation Public Release Date",
    "INVESTIGATION PUBLICATIONS",
    "Investigation PubMed ID",
    "Investigation Publication DOI",
    "Investigation Publication Author List",
    "Investigation Publication Title",
    "Investigation Publication Status",
    "Investigation Publication Status Term Accession Number",
    "Investigation Publication Status Term Source REF",
    "INVESTIGATION CONTACTS",
    "Investigation Person Last Name",
    "Investigation Person First Name",
    "Investigation Person Mid Initials",
    "Investigation Person Email",
    "Investigation Person Phone",
    "Investigation Person Fax",
    "Investigation Person Address",
    "Investigation Person Affiliation",
    "Investigation Person Roles",
    "Investigation Person Roles Term Accession Number",
    "Investigation Person Roles Term Source REF",
    "STUDY",
    "Study Identifier",
    "Study Title",
    "Study Description",
    "Study Submission Date",
    "Study Public Release Date",
    "Study File Name",
    "STUDY DESIGN DESCRIPTORS",
    "Study Design Type",
    "Study Design Type Term Accession Number",
    "Study Design Type Term Source REF",
    "STUDY PUBLICATIONS",
    "Study PubMed ID",
    "Study Publication DOI",
    "Study Publication Author List",
    "Study Publication Title",
    "Study Publication Status",
    "Study Publication Status Term Accession Number",
    "Study Publication Status Term Source REF",
    "STUDY FACTORS",
    "Study Factor Name",
    "Study Factor Type",
    "Study Factor Type Term Accession Number",
    "Study Factor Type Term Source REF",
    "STUDY ASSAYS",
    "Study Assay File Name",
    "Study Assay Measurement Type",
    "Study Assay Measurement Type Term Accession Number",
    "Study Assay Measurement Type Term Source REF",
    "Study Assay Technology Type",
    "Study Assay Technology Type Term Accession Number",
    "Study Assay Technology Type Term Source REF",
    "Study Assay Technology Platform",
    "STUDY PROTOCOLS",
    "Study Protocol Name",
    "Study Protocol Type",
    "Study Protocol Type Term Accession Number",
    "Study Protocol Type Term Source REF",
    "Study Protocol Description",
    "Study Protocol URI",
    "Study Protocol Version",
    "Study Protocol Parameters Name",
    "Study Protocol Parameters Name Term Accession Number",
    "Study Protocol Parameters Name Term Source REF",
    "Study Protocol Components Name",
    "Study Protocol Components Type",
    "Study Protocol Components Type Term Accession Number",
    "Study Protocol Components Type Term Source REF",
    "STUDY CONTACTS",
    "Study Person Last Name",
    "Study Person First Name",
    "Study Person Mid Initials",
    "Study Person Email",
    "Study Person Phone",
    "Study Person Fax",
    "Study Person Address",
    "Study Person Affiliation",
    "Study Person Roles",
    "Study Person Roles Term Accession Number",
    "Study Person Roles Term Source REF",
]

INVESTIGATION_FILE_ROWS_SET = set(INVESTIGATION_FILE_ROWS_LIST)