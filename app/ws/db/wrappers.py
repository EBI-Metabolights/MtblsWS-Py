import json
import logging
import os
import os.path
import time
from decimal import Decimal, ROUND_UP

from isatools import isatab

from app.ws.db import models
from app.ws.db.schemes import Study, User
from app.ws.db.settings import get_directory_settings
from app.ws.db.types import StudyStatus, UserRole, UserStatus
from app.ws.db.utils import date_str_to_int, datetime_to_int

logger = logging.getLogger(__file__)

MB_FACTOR = Decimal.from_float(1024.0 ** 2)


def create_study_model_from_db_study(db_study: Study):
    m_study = models.StudyModel(
        id=db_study.id,
        studyIdentifier=db_study.acc,
        obfuscationCode=db_study.obfuscationcode
    )

    m_study.studyStatus = StudyStatus(db_study.status).name
    m_study.studySize = db_study.studysize  # This value is different in DB and www.ebi.ac.uk
    size_in_mb = m_study.studySize / MB_FACTOR
    m_study.studyHumanReadable = str(size_in_mb.quantize(Decimal('.01'), rounding=ROUND_UP)) + "MB"
    m_study.publicStudy = StudyStatus(db_study.status) == StudyStatus.PUBLIC

    if db_study.submissiondate:
        m_study.studySubmissionDate = datetime_to_int(db_study.submissiondate)
    if db_study.releasedate:
        m_study.studyPublicReleaseDate = datetime_to_int(db_study.releasedate)
    if db_study.updatedate:
        m_study.updateDate = datetime_to_int(db_study.updatedate)

    if db_study.validations:
        db_study.validations = json.loads(db_study.validations)
        validation_entries_model = models.ValidationEntriesModel.parse_obj(db_study.validations)
        m_study.validations = validation_entries_model

    m_study.users = [get_user_model(x) for x in db_study.users]

    return m_study


def get_user_model(db_user: User):
    m_user = models.UserModel.from_orm(db_user)
    m_user.fullName = m_user.firstName + " " + m_user.lastName
    m_user.joinDate = datetime_to_int(m_user.joinDate)
    m_user.dbPassword = ""  # This value is set to empty string intentionally
    m_user.role = UserRole(int(m_user.role)).name
    m_user.status = UserStatus(int(m_user.status)).name
    return m_user


def update_study_model_from_directory(m_study: models.StudyModel, include_maf_files: bool = False,
                                      directory_settings=None):
    if not directory_settings:
        directory_settings = get_directory_settings()

    path = os.path.join(directory_settings.studies_folder, m_study.studyIdentifier)
    if not os.path.isdir(path):
        return
    investigation_file = os.path.join(path, 'i_Investigation.txt')

    if not os.path.exists(investigation_file) or not os.path.isfile(investigation_file):
        investigation_file = os.path.join(path, 'i_investigation.txt')
        if not os.path.exists(investigation_file) or not os.path.isfile(investigation_file):
            return

    with open(investigation_file) as f:
        start = time.time()
        investigation = isatab.load_investigation(f)

        end = time.time()
        logger.debug(f"Investigation file is read in {(end - start) / 1000.0} sec")
        if "studies" in investigation:
            studies = investigation["studies"]
            if studies:
                study = studies[0]
                study_title = get_value_with_column_name(study, "Study Title")
                study_description = get_value_with_column_name(study, "Study Description")
                study_submission_date = get_value_with_column_name(study, "Study Submission Date")
                study_release_date = get_value_with_column_name(study, "Study Public Release Date")
                study_file_name = get_value_with_column_name(study, "Study File Name")
                m_study.title = study_title
                m_study.description = study_description
                # !TODO check. this assignment overrides db data
                m_study.studySubmissionDate = date_str_to_int(study_submission_date)
                # !TODO check. this assignment overrides db data
                m_study.studyPublicReleaseDate = date_str_to_int(study_release_date)
                m_study.studyLocation = path
                fill_contacts(m_study, investigation)
                fill_descriptors(m_study, investigation)
                fill_factors(m_study, investigation)
                fill_publications(m_study, investigation)
                fill_protocols(m_study, investigation)
                fill_assays(m_study, investigation, path, include_maf_files)
                fill_sample_table(m_study, path)
                fill_organism(m_study)
                # TODO add validations to study!!!


def get_value_with_column_name(dataframe, column_name):
    try:
        ind = dataframe.columns.get_loc(column_name)
        return dataframe.values[0][ind]
    except KeyError:
        logger.warning(f"Column name {column_name} does not exist in frame")
        return None


def get_value_from_dict(series, column_name):
    if column_name in series:
        return series[column_name]
    return None


def fill_organism(m_study):
    organism_index = -1
    organism_part_index = -1
    for key, value in m_study.sampleTable.fields.items():
        if key.split("~")[1] == "characteristics[organism]":
            organism_index = value.index

    for key, value in m_study.sampleTable.fields.items():
        if key.split("~")[1] == "characteristics[organism part]":
            organism_part_index = value.index

    organism_dic = dict()
    for data in m_study.sampleTable.data:
        model = models.OrganismModel()
        if organism_index >= 0:
            model.organismName = data[organism_index]
        if organism_part_index >= 0:
            model.organismPart = data[organism_part_index]
        ind = model.organismName + model.organismPart
        if ind not in organism_dic:
            organism_dic[ind] = model
            m_study.organism.append(model)


def fill_sample_table(m_study, path):
    sample_file_name = "s_" + m_study.studyIdentifier + ".txt"
    file_path = os.path.join(path, sample_file_name)
    if os.path.isfile(file_path):
        with open(file_path) as f:
            sample = isatab.load_table(f)
            m_sample = models.TableModel()
            m_study.sampleTable = m_sample
            set_table_fields(m_sample.fields, sample)

            for i in range(sample.index.size):
                row = sample.iloc[i].to_list()
                m_sample.data.append(row)


def fill_assays(m_study, investigation, path, include_maf_files):
    if "s_assays" in investigation:
        items = investigation['s_assays'][0]
        index = 0
        for item in items.iterrows():
            model = models.AssayModel()
            index = index + 1
            model.assayNumber = index
            model.fileName = get_value_from_dict(item[1], "Study Assay File Name")
            technology = get_value_from_dict(item[1], "Study Assay Technology Type")
            model.technology = remove_ontology(technology)
            model.measurement = get_value_from_dict(item[1], "Study Assay Measurement Type")
            platform = get_value_from_dict(item[1], "Study Assay Technology Platform")
            model.platform = remove_ontology(platform)

            m_study.assays.append(model)
            file = os.path.join(path, model.fileName)
            if os.path.isfile(file):
                with open(file) as f:
                    table = isatab.load_table(f)
                    m_table = models.TableModel()
                    model.assayTable = m_table
                    maf_file_index = set_table_fields(m_table.fields, table, "metabolite assignment file")

                    for i in range(table.index.size):
                        row = table.iloc[i].to_list()
                        m_table.data.append(row)
                        if maf_file_index >= 0:
                            # Get MAF file name from first row
                            if not model.metaboliteAssignment:
                                maf_file_path = os.path.join(path, row[maf_file_index])
                                if os.path.exists(maf_file_path):
                                    assignment_model = models.MetaboliteAssignmentModel()
                                    model.metaboliteAssignment = assignment_model
                                    model.metaboliteAssignment.metaboliteAssignmentFileName = maf_file_path
                                    if include_maf_files:
                                        pass  # TODO fill metabolite alignment lines if needed (not needed now)
                    # end of method


def set_table_fields(fields, table, requested_field_name=None):
    requested_field_index = -1
    headers = table.columns.to_list()
    for i in range(len(headers)):
        key = str(i) + "~" + headers[i].lower()
        value = models.FieldModel()
        value.index = i
        data = headers[i]
        if requested_field_name and data.lower() == requested_field_name:
            requested_field_index = value.index

        value.fieldType = "basic"
        if "[" in data:
            data_list = data.split("[")
            value.fieldType = data_list[0]
            value.header = data_list[1].split("]")[0]
        else:
            value.header = data
        value.cleanHeader = value.header
        value.description = ""  # TODO How can be filled this value?
        fields[key] = value
    return requested_field_index


def remove_ontology(data: str):
    if not data:
        return data

    if ":" in data:
        result = data.split(":")
        result = [x for x in result if x]
        return result[-1]
    return data


def fill_descriptors(m_study, investigation):
    if "s_design_descriptors" in investigation:
        items = investigation['s_design_descriptors'][0]
        for item in items.iterrows():
            model = models.StudyDesignDescriptor()
            model.description = get_value_from_dict(item[1], "Study Design Type")
            m_study.descriptors.append(model)


def fill_factors(m_study, investigation):
    if "s_factors" in investigation:
        items = investigation['s_factors'][0]
        for item in items.iterrows():
            model = models.StudyFactorModel()
            model.name = get_value_from_dict(item[1], "Study Factor Name")
            m_study.factors.append(model)


def fill_protocols(m_study, investigation):
    if "s_protocols" in investigation:
        items = investigation['s_protocols'][0]
        for item in items.iterrows():
            model = models.ProtocolModel()

            model.name = get_value_from_dict(item[1], "Study Protocol Name")
            model.Description = get_value_from_dict(item[1], "Study Protocol Description")
            m_study.protocols.append(model)


def fill_publications(m_study, investigation):
    if "s_publications" in investigation:
        items = investigation['s_publications'][0]
        for item in items.iterrows():
            model = models.PublicationModel()

            model.pubmedId = get_value_from_dict(item[1], "Study PubMed ID")
            model.doi = get_value_from_dict(item[1], "Study Publication DOI")
            model.authorList = get_value_from_dict(item[1], "Study Publication Author List")
            model.title = get_value_from_dict(item[1], "Study Publication Title")
            model.abstractText = ""  # TODO how to fill abstract
            m_study.publications.append(model)


def fill_contacts(m_study, investigation):
    if "s_contacts" in investigation:
        contacts = investigation['s_contacts'][0]
        for item in contacts.iterrows():
            model = models.ContactModel()

            model.lastName = get_value_from_dict(item[1], "Study Person Last Name")
            model.firstName = get_value_from_dict(item[1], "Study Person First Name")
            model.midInitial = get_value_from_dict(item[1], "Study Person Mid Initials")
            model.email = get_value_from_dict(item[1], "Study Person Email")
            model.phone = get_value_from_dict(item[1], "Study Person Phone")
            model.address = get_value_from_dict(item[1], "Study Person Address")
            model.fax = get_value_from_dict(item[1], "Study Person Fax")
            model.role = get_value_from_dict(item[1], "Study Person Roles")
            model.affiliation = get_value_from_dict(item[1], "Study Person Affiliation")

            m_study.contacts.append(model)
