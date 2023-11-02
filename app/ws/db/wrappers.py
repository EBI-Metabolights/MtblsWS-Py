import datetime
import glob
import json
import logging
import os
import os.path

from isatools import isatab

from app.ws.db import models
from app.ws.db.models import TableModel, ValidationEntriesModel, ValidationEntryModel, BackupModel, IndexedUserModel, IndexedAssayModel
from app.ws.db.schemes import Study, User
from app.ws.db.types import StudyStatus, UserRole, UserStatus
from app.ws.db.utils import date_str_to_int, datetime_to_int

logger = logging.getLogger(__file__)

MB_FACTOR = 1024.0 ** 2
GB_FACTOR = 1024.0 ** 3


def create_study_model_from_db_study(db_study: Study):
    m_study = models.StudyModel(
        id=db_study.id,
        studyIdentifier=db_study.acc,
        obfuscationCode=db_study.obfuscationcode
    )

    m_study.studyStatus = StudyStatus(db_study.status).name

    m_study.studySize = int(db_study.studysize)  # This value is different in DB and www.ebi.ac.uk
    if m_study.studySize > GB_FACTOR:
        size_in_gb = m_study.studySize / GB_FACTOR
        m_study.studyHumanReadable = "%.2f" % round(size_in_gb, 2) + "GB"
    else:
        size_in_mb = m_study.studySize / MB_FACTOR
        m_study.studyHumanReadable = "%.2f" % round(size_in_mb, 2) + "MB"

    m_study.publicStudy = StudyStatus(db_study.status) == StudyStatus.PUBLIC

    if db_study.submissiondate:
        m_study.studySubmissionDate = datetime_to_int(db_study.submissiondate)
    if db_study.releasedate:
        m_study.studyPublicReleaseDate = datetime_to_int(db_study.releasedate)
    if db_study.updatedate:
        m_study.updateDate = datetime_to_int(db_study.updatedate)

    if db_study.validations:
        try:
            db_study.validations = json.loads(db_study.validations)
            validation_entries_model = models.ValidationEntriesModel.model_validate(db_study.validations)
            m_study.validations = validation_entries_model
        except Exception as e:
            logger.warning(f'{e.args}')

    m_study.users = [get_user_lite_model(x) for x in db_study.users]

    return m_study


def get_user_model(db_user: User):
    m_user = models.UserModel.model_validate(db_user)
    m_user.fullName = m_user.firstName + " " + m_user.lastName
    m_user.joinDate = datetime_to_int(m_user.joinDate)
    m_user.dbPassword = None  # This value is set to empty string intentionally
    m_user.role = UserRole(int(m_user.role)).name
    m_user.status = UserStatus(int(m_user.status)).name
    return m_user

def get_user_lite_model(db_user: User):
    m_user = models.UserLiteModel()
    m_user.firstName = db_user.firstname
    m_user.lastName = db_user.lastname
    m_user.affiliation = db_user.affiliation
    m_user.address = db_user.address
    return m_user

def update_users_for_indexing(m_study):
    new_indexed_user_list = []
    for user in m_study.users:
        indexed_user = IndexedUserModel.model_validate(user)

        new_indexed_user_list.append(indexed_user)

    m_study.users.clear()
    m_study.users.extend(new_indexed_user_list)


def update_assays_for_indexing(m_study):
    new_indexed_assay_list = []
    for assay in m_study.assays:
        indexed_assay = IndexedAssayModel.model_validate(assay)
        new_indexed_assay_list.append(indexed_assay)

    m_study.assays.clear()
    m_study.assays.extend(new_indexed_assay_list)


def update_study_model_from_directory(m_study: models.StudyModel, studies_root_path,
                                      optimize_for_es_indexing: bool = False, include_maf_files: bool = False,
                                      revalidate_study: bool = False, user_token_to_revalidate=None,
                                      title_and_description_only: bool = False):
    path = os.path.join(studies_root_path, m_study.studyIdentifier)
    if not os.path.isdir(path):
        return
    investigation_file = os.path.join(path, 'i_Investigation.txt')

    if not os.path.exists(investigation_file) or not os.path.isfile(investigation_file):
        investigation_file = os.path.join(path, 'i_investigation.txt')
        if not os.path.exists(investigation_file) or not os.path.isfile(investigation_file):
            return
    investigation = None
    with open(investigation_file, encoding="utf-8") as f:
        try:
            investigation = isatab.load_investigation(f)
        except Exception as e:
            logger.warning(f'{investigation_file} file is not opened with utf-8 encoding')
    if investigation is None:
        with open(investigation_file, encoding="latin-1") as f:
            try:
                investigation = isatab.load_investigation(f)
            except Exception as e:
                logger.error(f'{investigation_file} file is not opened with latin-1 encoding')
                message = f'{m_study.studyIdentifier} i_Investigation.txt file can not be loaded.'
                
    if not investigation:
        logger.error(f'{investigation_file} is not valid.')
    elif "studies" not in investigation:
        logger.error(f'No study is defined in {investigation_file}')

    if investigation and "studies" in investigation:
        studies = investigation["studies"]
        if studies:
            f_study = studies[0]
            create_study_model(m_study, path, f_study)
            fill_factors(m_study, investigation)
            fill_descriptors(m_study, investigation)
            fill_publications(m_study, investigation)
            if title_and_description_only:
                return
            fill_assays(m_study, investigation, path, include_maf_files)
            fill_sample_table(m_study, path)  # required for fill organism, later remove from model
            fill_organism(m_study)
            # fill_backups(m_study, path)

            fill_validations(m_study, path, revalidate_study, user_token_to_revalidate)

            if not optimize_for_es_indexing:
                fill_protocols(m_study, investigation)
                try:
                    fill_contacts(m_study, investigation)
                except Exception as ex:
                    logger.error(f'{m_study.studyIdentifier} contacts are not parsed successfully!')
            else:

                m_study.sampleTable = TableModel() # delete sample table data from model for indexing.
                m_study.contacts = []
                m_study.studyLocation = ""
                m_study.protocols = []
                m_study.obfuscationCode = ""
                update_users_for_indexing(m_study)
                update_assays_for_indexing(m_study)
            fill_derived_data(m_study)


def fill_derived_data(m_study: models.StudyModel):
    data = models.StudyDerivedData()
    m_study.derivedData = data
    data.organismNames = "::".join(set(organism.organismName for organism in m_study.organism if organism and organism.organismName)) if m_study.organism else ''
    data.organismParts = "::".join(set(organism.organismPart for organism in m_study.organism if organism and organism.organismPart)) if m_study.organism else ''
    data.country = m_study.users[0].address if m_study.users and m_study.users[0] else ""
    submission_date = datetime.datetime.fromtimestamp(m_study.studySubmissionDate / 1000)
    data.submissionMonth = submission_date.strftime("%Y-%m")
    data.submissionYear = submission_date.year
    release_date = datetime.datetime.fromtimestamp(m_study.studyPublicReleaseDate / 1000)
    data.releaseMonth = release_date.strftime("%Y-%m")
    data.releaseYear = release_date.year
    
    
def fill_backups(m_study, path):
    backup_path = os.path.join(path, "audit")
    os.makedirs(backup_path, exist_ok=True)
    backup_directories = [x[0] for x in os.walk(backup_path)]
    id = 0
    for directory in backup_directories:
        directory_name = os.path.basename(directory)
        if directory_name.isnumeric():
            backup_model = BackupModel()
            backup_model.backupId = str(directory_name)
            id = id + 1
            backup_model.folderPath = directory
            try:
                timestamp = int(datetime.datetime.strptime(directory_name, "%Y%m%d%H%M%S").strftime("%s"))
                backup_model.backupTimeStamp = timestamp
            except:
                pass
            m_study.backups.append(backup_model)


def create_study_model(m_study, path, study):
    study_title = get_value_with_column_name(study, "Study Title")
    study_description = get_value_with_column_name(study, "Study Description")
    study_submission_date = get_value_with_column_name(study, "Study Submission Date")
    study_release_date = get_value_with_column_name(study, "Study Public Release Date")
    study_file_name = get_value_with_column_name(study, "Study File Name")  # not used
    m_study.title = study_title
    m_study.description = study_description
    # !TODO check. this assignment overrides db data
    if study_submission_date:
        m_study.studySubmissionDate = date_str_to_int(study_submission_date)
    # !TODO check. this assignment overrides db data
    if study_release_date:
        m_study.studyPublicReleaseDate = date_str_to_int(study_release_date)
    m_study.studyLocation = path


def fill_validations(m_study, path, revalidate_study, user_token_to_revalidate):
    # TODO review validations mappings
    validation_entries_model = ValidationEntriesModel()
    m_study.validations = validation_entries_model

#     if revalidate_study:
#         results = validate_study(m_study.studyIdentifier, path, user_token_to_revalidate, m_study.obfuscationCode)
#         if results and "validation" in results:
#             validation_result = results["validation"]
#             if "status" in validation_result:
#                 validation_entries_model.status = validation_result["status"]
#             validation_entries_model.overriden = False
#             validation_entries_model.passedMinimumRequirement = False
#             if "validations" in validation_result:
#                 validations = validation_result["validations"]
#                 for section in validations:
#                     if "details" in section:
#                         for message in section["details"]:
#                             validation_entry_model = ValidationEntryModel()
#                             validation_entry_model.status = message["status"]
#                             validation_entry_model.description = message["description"]
#                             validation_entry_model.message = message["message"]
#                             validation_entry_model.group = message["section"]
#                             validation_entry_model.overriden = message["val_override"]
#                             validation_entries_model.entries.append(validation_entry_model)


def get_value_with_column_name(dataframe, column_name):
    try:
        ind = dataframe.columns.get_loc(column_name)
        return dataframe.values[0][ind]
    except KeyError:
        logger.warning(f"Column name {column_name} does not exist in frame")
        return None


def get_value_from_dict(series, column_name):
    if column_name in series:
        return series[column_name].strip()
    return None


def fill_organism(m_study):
    organism_index = -1
    organism_part_index = -1
    if m_study and m_study.sampleTable:
        if m_study.sampleTable.fields:
            for key, value in m_study.sampleTable.fields.items():
                if key.split("~")[1] == "characteristics[organism]":
                    organism_index = value.index

            for key, value in m_study.sampleTable.fields.items():
                if key.split("~")[1] == "characteristics[organism part]":
                    organism_part_index = value.index
        if m_study.sampleTable.data:
            organism_dic = {}
            for data in m_study.sampleTable.data:
                model = models.OrganismModel()
                if organism_index >= 0:
                    model.organismName = data[organism_index]
                if organism_part_index >= 0:
                    model.organismPart = data[organism_part_index]
                ind = model.organismName if model.organismName else ''
                ind += model.organismPart if model.organismPart else ''
                if ind not in organism_dic and ind:
                    organism_dic[ind] = model
                    m_study.organism.append(model)


def fill_sample_table(m_study, path):
    sample_files = glob.glob(os.path.join(path, "s_*.txt"))
    first_priority_path = os.path.join(path, "s_" + m_study.studyIdentifier + ".txt")
    second_priority_path = os.path.join(path, 's_Sample.txt')
    selected = sample_files[0] if sample_files else None
    if sample_files:
        if first_priority_path in sample_files:
            selected = first_priority_path
        elif second_priority_path in sample_files:
            selected = second_priority_path

    file_path = selected

    if file_path and os.path.exists(file_path) and os.path.isfile(file_path):
        sample = None
        try:
            with open(file_path, encoding="utf-8") as f:
                sample = isatab.load_table(f)
        except Exception as e:
            logger.warning(f"{file_path} is not opened with unicode encoding")

        if sample is None:
            with open(file_path, encoding="latin-1") as f:
                try:
                    sample = isatab.load_table(f)
                except Exception as e:
                    logger.warning(f"{file_path} is not opened with latin-1 encoding")
                    return

        m_sample = models.TableModel()
        m_study.sampleTable = m_sample
        _, valid_indices = set_table_fields(m_sample.fields, sample)

        for i in range(sample.index.size):
            row = sample.iloc[i].to_list()
            trimmed_row = [row[valid_ind] for valid_ind in valid_indices]
            m_sample.data.append(trimmed_row)


def fill_assays(m_study, investigation, path, include_maf_files):
    if "s_assays" in investigation and investigation['s_assays'] and investigation['s_assays'][0] is not None:
        items = investigation['s_assays'][0]
        assays = []
        for ind in range(len(items.index)):
            assay = items.iloc[ind]
            assays.append(assay)
        index = 0
        for item in assays:
            try:
                model = models.AssayModel()
                index = index + 1
                model.assayNumber = index
                model.fileName = get_value_from_dict(item, "Study Assay File Name")
                
                
                technology = get_value_from_dict(item, "Study Assay Technology Type")
                model.technology = remove_ontology(technology)
                model.measurement = get_value_from_dict(item, "Study Assay Measurement Type")
                platform = get_value_from_dict(item, "Study Assay Technology Platform")
                model.platform = remove_ontology(platform)
                m_study.assays.append(model)
                file = os.path.join(path, model.fileName)
                if os.path.exists(file) and os.path.isfile(file):      
                    
                    table = None
                    try:
                        with open(file, encoding="utf-8") as f:
                            table = isatab.load_table(f)
                    except Exception as ex:
                        logger.warning(f'{file} is not parsed with utf-8 encoding')

                    if table is None:
                        with open(file, encoding="latin-1", ) as f:
                            try:
                                table = isatab.load_table(f)
                            except Exception as ex:
                                logger.error(f'{file} is not parsed with latin-1 encoding. {str(ex)}')
                    if table is None:
                        continue

                    m_table = models.TableModel()
                    model.assayTable = m_table
                    maf_file_index, valid_indices = set_table_fields(m_table.fields, table,
                                                                    "metabolite assignment file")

                    for i in range(table.index.size):
                        try:
                            row = table.iloc[i].to_list()
                            trimmed_row = [row[valid_ind] for valid_ind in valid_indices]
                            m_table.data.append(trimmed_row)
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
                        except Exception as ex:
                            logger.error(f"{model.fileName} row {str(i + 1)} can not not parsed!")
                    # end of method
            except Exception as ex:
                logger.error(f"{model.fileName} can not be processed successfully!")


def set_table_fields(fields, table, requested_field_name=None):
    requested_field_index = -1
    headers = table.columns.to_list()
    valid_indices = []
    for i in range(len(headers)):
        if headers[i]:
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
            valid_indices.append(i)
    return requested_field_index, valid_indices


def remove_ontology(data: str):
    if not data:
        return data

    if ":" in data:
        result = data.split(":")
        result = [x for x in result if x]
        return result[-1]
    return data


def fill_descriptors(m_study, investigation):
    if "s_design_descriptors" in investigation and investigation['s_design_descriptors']:
        items = investigation['s_design_descriptors'][0]
        for item in items.iterrows():
            model = models.StudyDesignDescriptor()
            design_type = get_value_from_dict(item[1], "Study Design Type")
            ref = get_value_from_dict(item[1], "Study Design Type Term Source REF")
            model.description = design_type
            if ref:
                model.description = ref + ":" + design_type
            m_study.descriptors.append(model)


def fill_factors(m_study, investigation):
    if "s_factors" in investigation and investigation['s_factors']:
        items = investigation['s_factors'][0]
        for item in items.iterrows():
            model = models.StudyFactorModel()
            model.name = get_value_from_dict(item[1], "Study Factor Name")
            m_study.factors.append(model)


def fill_protocols(m_study, investigation):
    if "s_protocols" in investigation and investigation['s_protocols']:
        items = investigation['s_protocols'][0]
        for item in items.iterrows():
            model = models.ProtocolModel()

            model.name = get_value_from_dict(item[1], "Study Protocol Name")
            model.description = get_value_from_dict(item[1], "Study Protocol Description")
            m_study.protocols.append(model)


def fill_publications(m_study, investigation):
    if "s_publications" in investigation and investigation['s_publications']:
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
    if "s_contacts" in investigation and investigation['s_contacts']:
        items = investigation['s_contacts'][0]
        contacts = []
        for ind in range(len(items.index)):
            contact = items.iloc[ind]
            contacts.append(contact)
            
        for item in contacts:
            try:
                model = models.ContactModel()

                model.lastName = get_value_from_dict(item, "Study Person Last Name")
                model.firstName = get_value_from_dict(item, "Study Person First Name")
                model.midInitial = get_value_from_dict(item, "Study Person Mid Initials")
                model.email = get_value_from_dict(item, "Study Person Email")
                model.phone = get_value_from_dict(item, "Study Person Phone")
                model.address = get_value_from_dict(item, "Study Person Address")
                model.fax = get_value_from_dict(item, "Study Person Fax")
                model.role = get_value_from_dict(item, "Study Person Roles")
                model.affiliation = get_value_from_dict(item, "Study Person Affiliation")

                m_study.contacts.append(model)
            except Exception as ex:
                logger.error(f'{m_study.studyIdentifier} contact is not parsed successfully!')
