import json
import logging
import os
import pathlib
import datetime
import time
from xml.dom.minidom import Document, Element
from app.config import get_settings
from app.tasks.worker import send_email
from app.utils import MetabolightsDBException
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import ContactModel, MetaboLightsCompoundModel, PublicationModel, StudyModel
from app.ws.db.schemes import User
from app.ws.db.wrappers import update_study_model_from_directory
from app.ws.db_connection import get_all_studies, get_public_studies, add_metabolights_data
from app.ws.dom_utils import create_generic_element, create_generic_element_attribute
from app.ws.study.study_service import StudyService
from app.services.compound.compound_service import CompoundService
from app.ws.settings.utils import get_study_settings
import urllib

logger = logging.getLogger("wslog")

class EbEyeSearchService():
    
    study_root = get_study_settings().mounted_paths.study_metadata_files_root_path
    public_ftp_download = get_study_settings().mounted_paths.public_ftp_download_path
    metabolights_website_link = get_study_settings().metabolights_website_link
    europe_pmc_url = get_settings().external_dependencies.api.europe_pmc_api_url
    metabolite_list = []
    study_acc_list = []
    linked_studies = 0
    articles_linked = 0
    raw_files_list = get_settings().file_filters.raw_files_list
    derived_files_list = get_settings().file_filters.derived_files_list
    study_resource_name = "MetaboLights"
    study_resource_description = "MetaboLights is a database for Metabolomics experiments and derived information"
    eb_eye_public_studies_ebi = "eb_eye_metabolights_studies.xml"
    eb_eye_public_compounds_ebi = "eb_eye_metabolights_compounds.xml"
    eb_eye_public_studies_thomson = "thomsonreuters_metabolights_studies.xml"
    europe_pmc_study = "europe_PMC_metabolights_studies.xml"
    europe_pmc_provider_id = "1782"
    europe_pmc_pmid_src = "MED"
    content_type_xml = "xml"
    
    @staticmethod
    def export_public_studies(user_token: str, thomson_reuters: bool=False):
        start_time = time.time()
        study_list = get_public_studies()
        email = EbEyeSearchService.get_email_by_token(user_token=user_token)
        
        doc = Document()
        root = doc.createElement('database')
        doc.appendChild(root)
        doc = create_generic_element(doc, root, 'name', EbEyeSearchService.study_resource_name)
        doc = create_generic_element(doc, root, 'description', EbEyeSearchService.study_resource_description)
        doc = create_generic_element(doc, root, 'release', "1.0")
        doc = create_generic_element(doc, root, 'release_date', datetime.date.today().strftime('%Y-%m-%d'))
        doc = create_generic_element(doc, root, 'entry_count', str(len(study_list)))
        entries = doc.createElement('entries')
        root.appendChild(entries)
        i=0
        for study_id in study_list:
            logger.info(f"EB EYE search export processing for the study  - {study_id[0]}")
            doc = EbEyeSearchService.process_study(doc=doc, root=entries, study_id=study_id[0], thomson_reuters=thomson_reuters)
            i = i+1
        logger.info(f"processing completed for all the studies; Processed count  - {i}")
        xml_str = doc.toprettyxml(indent="")
        if thomson_reuters:
            add_metabolights_data(content_name=EbEyeSearchService.eb_eye_public_studies_thomson, data_format=EbEyeSearchService.content_type_xml, content=xml_str)
        else:
            add_metabolights_data(content_name=EbEyeSearchService.eb_eye_public_studies_ebi, data_format=EbEyeSearchService.content_type_xml, content=xml_str)
        logger.info("Data stored to DB!")
        processed_time = (time.time() - start_time)/60
        result = f"Processed study count - {i}; Process completed in {processed_time} minutes"
        send_email("EB EYE public studies export completed", result, None, email, None)
        return {"processed_studies": i, "completed_in": processed_time}
    
    @staticmethod
    def export_europe_pmc(user_token: str):
        start_time = time.time()
        study_list = get_public_studies()
        email = EbEyeSearchService.get_email_by_token(user_token=user_token)
        
        doc = Document()
        links = doc.createElement('links')
        doc.appendChild(links)
        i=0
        for study_id in study_list:
            logger.info(f"Exporting  the study {study_id[0]} for EuropePMC")
            doc, study_linked, pubs_count = EbEyeSearchService.process_study_for_europmc(doc=doc, root=links, study_id=study_id)
            if study_linked:
                EbEyeSearchService.linked_studies = EbEyeSearchService.linked_studies + 1
                EbEyeSearchService.articles_linked = EbEyeSearchService.articles_linked + pubs_count
            i = i+1
        logger.info(f"processing completed for all the studies; Processed count  - {i}")
        xml_str = doc.toprettyxml(indent="")        
        add_metabolights_data(content_name=EbEyeSearchService.europe_pmc_study, data_format=EbEyeSearchService.content_type_xml, content=xml_str)
        logger.info(f"Data stored to DB")
        processed_time = (time.time() - start_time)/60
        result = f"Processed study count - {i}; Process completed in {processed_time} minutes. \n  {EbEyeSearchService.linked_studies} Studies linked with EuropePMC. Those studies got {EbEyeSearchService.articles_linked} publications"
        logger.info(f"Processed study count - {i}; Process completed in {processed_time} minutes. \n  {EbEyeSearchService.linked_studies} Metabolights Studies linked with EuropePMC. Those studies got {EbEyeSearchService.articles_linked} publications")
        send_email("EuropePMC export processing completed", result, None, email, None)
        return {"processed_studies": i, "completed_in": processed_time}
    
    @staticmethod
    def europe_publication_report(user_token: str):
        start_time = time.time()
        study_list = get_public_studies()
        #study_list = ['MTBLS7519', 'MTBLS10757', 'MTBLS3923', 'MTBLS9845', 'MTBLS8577']
        email = EbEyeSearchService.get_email_by_token(user_token=user_token)
        study_str = "StudyId,HasPublication,PubMedId,DOI,DoiHit,TitleHit,PublicationStatus,StudyTitle"
        i=0
        for study in study_list:
            studyid = study[0]
            if i == 10:
                break
            logger.info(f"Exporting  the study {studyid} for EuropePMC")
            has_publication, pubmed_id, doi, doi_hit, title_hit, publication_status, study_title, api_output = EbEyeSearchService.query_europe_pmc(study_id=studyid)
            study_str = f"{study_str}\n{studyid},{has_publication},{pubmed_id},{doi},{doi_hit},{title_hit},{publication_status},\"{study_title}\",{api_output}"
            i = i+1
        logger.info(f"processing completed for all the studies; Processed count  - {i}")
        #xml_str = doc.toprettyxml(indent="")        
        #add_metabolights_data(content_name=EbEyeSearchService.europe_pmc_study, data_format=EbEyeSearchService.content_type_xml, content=xml_str)
        #logger.info(f"Data stored to DB")
        processed_time = (time.time() - start_time)/60
        server = os.uname()[1]
        result = f"Processed study count - {i}; Process completed in {processed_time} minutes. \n Processed by {server}"
        send_email("EuropePMC report processing completed", result, None, email, None)
        text_file = open("/net/isilonP/public/rw/homes/tc_cm01/test/study-publication.csv", "w")
        text_file.write(study_str)
        text_file.close()
        return {"processed_studies": i, "completed_in": processed_time}
    
    @staticmethod
    def query_europe_pmc(study_id: str):
        has_publication = 'No'
        pubmed_id = ''
        doi = ''
        doi_hit = 'None'
        title_hit = 'None'
        publication_status = 'None'
        study_title = ''
        api_output = ''
        try:
            study: StudyModel = StudyService.get_instance().get_study_with_detailed_user(study_id=study_id)
            update_study_model_from_directory(study, EbEyeSearchService.study_root)
            study_title = study.title
            publications = study.publications
            if  publications is not None and len(publications) > 0:
                has_publication = 'Yes'
                for publication in study.publications:
                    pubmed_id = publication.pubmedId
                    if EbEyeSearchService.check_for_empty(pubmed_id):
                        publication_status = 'Published'
                    else:
                        pubmed_id = 'None'
                    if EbEyeSearchService.check_for_empty(publication.doi):
                        doi = publication.doi
                        query = f'doi:{doi}'
                        output = EbEyeSearchService.europe_pmc_query(query=query)
                        hitcount = output['hitCount']
                        if hitcount > 0:
                            doi_hit = 'Success'
                            result_list = output['resultList']['result']
                            for result in result_list:
                                if result['source'] == 'MED':
                                    publication_status = 'Published'
                                    pmid = result['pmid']
                                    api_output = f'{pmid}'
                                elif result['source'] == 'PPR':
                                    publication_status = 'Preprint'
                        else:
                            doi_hit = 'Failed'
                            query = f'title:{publication.title}'
                            query = urllib.parse.quote(query)
                            output = EbEyeSearchService.europe_pmc_query(query=query)
                            hitcount = output['hitCount']
                            if hitcount == 1:
                                title_hit = 'Success'
                                result_list = output['resultList']['result']
                                for result in result_list:
                                    if result['source'] == 'MED':
                                        publication_status = 'Published'
                                        pmid = result['pmid']
                                        doi_output = result['doi']
                                        api_output = f'{pmid}{doi_output}'
                            else:
                                title_hit = 'Failed'
                    else:
                        query = f'title:{publication.title}'
                        query = urllib.parse.quote(query)
                        output = EbEyeSearchService.europe_pmc_query(query=query)
                        hitcount = output['hitCount']
                        if hitcount == 1:
                            title_hit = 'Success'
                            result_list = output['resultList']['result']
                            for result in result_list:
                                if result['source'] == 'MED':
                                    publication_status = 'Published'
                                    pmid = result['pmid']
                                    doi_output = result['doi']
                                    api_output = f'{pmid}{doi_output}'
                        else:
                            title_hit = 'Failed'
                                                  
        except Exception as ex:
            logger.error(f"Exception while processing study - {study_id}; reason: {str(ex)}")
        return has_publication, pubmed_id, doi, doi_hit, title_hit, publication_status, study_title, api_output
    
    @staticmethod
    def process_study_for_europmc(doc: Document, root: Element, study_id: str):
        pmid_found = []
        preprint_doi_found = []
        study_text_found = []
        pubs_count = 0
        study_linked = False
        try:
            study: StudyModel = StudyService.get_instance().get_public_study_with_detailed_user(study_id=study_id)
            update_study_model_from_directory(study, EbEyeSearchService.study_root)
            publications = study.publications
            if  publications is not None and len(publications) > 0:
                for publication in study.publications:
                    link = doc.createElement('link')
                    link.setAttribute(attname='providerId', value=EbEyeSearchService.europe_pmc_provider_id)
                    resource = doc.createElement('resource')
                    doc = create_generic_element(doc, resource, 'title', EbEyeSearchService.filter_non_printable(study.title))
                    doc = create_generic_element(doc, resource, 'url', f'{EbEyeSearchService.metabolights_website_link}/{study.studyIdentifier}')
                    link.appendChild(resource)
                    if EbEyeSearchService.check_for_empty(publication.pubmedId):
                        resource = doc.createElement('record')
                        doc = create_generic_element(doc, resource, 'source', EbEyeSearchService.europe_pmc_pmid_src)
                        doc = create_generic_element(doc, resource, 'id', publication.pubmedId)
                        link.appendChild(resource)
                        root.appendChild(link)
                        study_linked = True
                        pubs_count = pubs_count+1
                    else:
                        if EbEyeSearchService.check_for_empty(publication.doi):
                            query = f'doi:{publication.doi}'
                            output = EbEyeSearchService.europe_pmc_query(query=query)
                            hitcount = output['hitCount']
                            if hitcount == 1:
                                doc = create_generic_element(doc, link, 'doi', publication.doi)
                                root.appendChild(link)
                                study_linked = True
                                pubs_count = pubs_count+1
                            else:
                                query = f'title:{publication.title}'
                                query = urllib.parse.quote(query)
                                output = EbEyeSearchService.europe_pmc_query(query=query)
                                hitcount = output['hitCount']
                                if hitcount == 1:
                                    doc = create_generic_element(doc, link, 'doi', publication.doi)
                                    root.appendChild(link)
                                    study_linked = True
                                    pubs_count = pubs_count+1
        except Exception as ex:
            logger.error(f"Exception while processing study - {study_id}; reason: {str(ex)}")
        return doc, study_linked, pubs_count
    
    @staticmethod
    def europe_pmc_query(query: str):
        url = f'{EbEyeSearchService.europe_pmc_url}/search?query={query}&format=json'
        result = urllib.request.urlopen(url)
        content = result.read().decode('utf-8')
        output = json.loads(content)
        return output
                
    @staticmethod
    def get_study(study_id: str, thomson_reuters: bool=False):        
        doc = Document()
        root = doc.createElement('database')
        doc.appendChild(root)
        doc = create_generic_element(doc, root, 'name', EbEyeSearchService.study_resource_name)
        doc = create_generic_element(doc, root, 'description', EbEyeSearchService.study_resource_description)
        doc = create_generic_element(doc, root, 'release', "1.0")
        doc = create_generic_element(doc, root, 'release_date', datetime.date.today().strftime('%Y-%m-%d'))
        doc = create_generic_element(doc, root, 'entry_count', "1")
        entries = doc.createElement('entries')
        root.appendChild(entries)
        doc = EbEyeSearchService.process_study(doc=doc, root=entries, study_id=study_id, thomson_reuters=thomson_reuters)
        return doc
    
    @staticmethod
    def process_study(doc: Document, root: Element, study_id: str, thomson_reuters: bool):
        try:
            study: StudyModel = StudyService.get_instance().get_public_study_with_detailed_user(study_id=study_id)
            update_study_model_from_directory(study, EbEyeSearchService.study_root)
            entry = doc.createElement('entry')
            entry.setAttribute(attname='id', value=study.studyIdentifier)
            root.appendChild(entry)
            doc = create_generic_element(doc, entry, 'name', EbEyeSearchService.filter_non_printable(study.title))
            doc = create_generic_element(doc, entry, 'description', EbEyeSearchService.filter_non_printable(study.description))
            doc = EbEyeSearchService.add_cross_references(doc=doc, root=entry, study=study)
            doc = EbEyeSearchService.add_dates(doc=doc, root=entry, study=study)
            if thomson_reuters:
                doc = EbEyeSearchService.add_detailed_authors(doc=doc, root=entry, study=study)
                doc = EbEyeSearchService.add_structured_publication(doc=doc, root=entry, study=study)
            doc = EbEyeSearchService.add_additional_fields(doc=doc, root=entry, study=study, thomson_reuters=thomson_reuters)
        except Exception as ex:
            logger.error(f"Exception while processing study - {study_id}; reason: {str(ex)}")
        return doc
    
    @staticmethod
    def add_cross_references(doc: Document, root: Element, study: StudyModel):
        xrefs = doc.createElement('cross_references')
        for publication in study.publications:
            if publication.pubmedId != '':
                ref = doc.createElement('ref')
                ref.setAttribute(attname= 'dbkey', value= publication.pubmedId)
                ref.setAttribute(attname= 'dbname', value= 'pubmed')
                xrefs.appendChild(ref)
        for i in range(len(study.assays)):
            df_data_dict = StudyService.get_instance().get_study_maf_rows(study_id=study.studyIdentifier, sheet_number=i+1)
            rows_dict = df_data_dict['rows']
            if rows_dict is not None :
                xrefs = EbEyeSearchService.process_maf_rows(doc, xrefs, rows_dict)
        root.appendChild(xrefs)
        return doc
        
    @staticmethod
    def process_maf_rows(doc: Document, xrefs: Element, rows_dict):
        EbEyeSearchService.metabolite_list = []
        xref_list = []
        for row in rows_dict:
            database_identifier = row['database_identifier']
            metabolite_identification = row['metabolite_identification']
            if database_identifier != '' and metabolite_identification != '':
                metname = EbEyeSearchService.filter_non_printable(metabolite_identification)
                if metname not in EbEyeSearchService.metabolite_list:
                    EbEyeSearchService.metabolite_list.append(metname)
                db_id = database_identifier.strip()
                if db_id in xref_list:
                    continue
                else:
                    xref_list.append(db_id)
                    if db_id.startswith("CHEBI"):
                        ref = doc.createElement('ref')
                        ref.setAttribute(attname= 'dbkey', value= db_id)
                        ref.setAttribute(attname= 'dbname', value= 'ChEBI')
                        xrefs.appendChild(ref)
                        ref = doc.createElement('ref')
                        ref.setAttribute(attname= 'dbkey', value= db_id.replace('CHEBI:', 'MTBLC'))
                        ref.setAttribute(attname= 'dbname', value= 'MetaboLights')
                        xrefs.appendChild(ref)
                    elif db_id.startswith("CID"):
                        ref = doc.createElement('ref')
                        ref.setAttribute(attname= 'dbkey', value= db_id)
                        ref.setAttribute(attname= 'dbname', value= 'PubChem')
                        xrefs.appendChild(ref)
                    elif db_id.startswith("HMDB"):
                        ref = doc.createElement('ref')
                        ref.setAttribute(attname= 'dbkey', value= db_id)
                        ref.setAttribute(attname= 'dbname', value= 'HMDB')
                        xrefs.appendChild(ref)
                    elif db_id.startswith("HMDB"):
                        ref = doc.createElement('ref')
                        ref.setAttribute(attname= 'dbkey', value= db_id)
                        ref.setAttribute(attname= 'dbname', value= 'HMDB')
                        xrefs.appendChild(ref)
                    elif db_id.startswith("MTBLC"):
                        ref = doc.createElement('ref')
                        ref.setAttribute(attname= 'dbkey', value= db_id)
                        ref.setAttribute(attname= 'dbname', value= 'MTBLC')
                        xrefs.appendChild(ref)
                    elif db_id.startswith("LM"):
                        ref = doc.createElement('ref')
                        ref.setAttribute(attname= 'dbkey', value= db_id)
                        ref.setAttribute(attname= 'dbname', value= 'LIPID MAPS')
                        xrefs.appendChild(ref)
                    elif db_id.startswith("C"):
                        ref = doc.createElement('ref')
                        ref.setAttribute(attname= 'dbkey', value= db_id)
                        ref.setAttribute(attname= 'dbname', value= 'KEGG')
                        xrefs.appendChild(ref)
                    elif db_id.startswith("GMD"):
                        ref = doc.createElement('ref')
                        ref.setAttribute(attname= 'dbkey', value= db_id)
                        ref.setAttribute(attname= 'dbname', value= 'GOLM')
                        xrefs.appendChild(ref)
        return xrefs
    
    @staticmethod
    def add_dates(doc: Document, root: Element, study: StudyModel):
        dates_elem = doc.createElement('dates')
        date_elem = doc.createElement('date')
        date_elem.setAttribute(attname= 'type', value= 'submission')
        date_elem.setAttribute(attname= 'value', value= EbEyeSearchService.conertdate_date(study.studySubmissionDate))
        dates_elem.appendChild(date_elem)
        date_elem = doc.createElement('date')
        date_elem.setAttribute(attname= 'type', value= 'publication')
        date_elem.setAttribute(attname= 'value', value= EbEyeSearchService.conertdate_date(study.studyPublicReleaseDate))
        dates_elem.appendChild(date_elem)
        root.appendChild(dates_elem)
        return doc
    
    @staticmethod
    def add_additional_fields(doc: Document, root: Element, study: StudyModel, thomson_reuters: bool):
        additional_fields = doc.createElement('additional_fields')
        doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                               item_name='field', item_value='MetaboLights', attr_name='name', attr_value='repository')
        doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                               item_name='field', item_value='Metabolomics', attr_name='name', attr_value='omics_type')
        if thomson_reuters is False:
            doc = EbEyeSearchService.add_authors(doc=doc, additional_fields=additional_fields, study=study)
        doc = EbEyeSearchService.add_protocols(doc=doc, additional_fields=additional_fields, study=study)
        doc = EbEyeSearchService.add_assay(doc=doc, additional_fields=additional_fields, study=study)
        doc = EbEyeSearchService.add_files(doc=doc, additional_fields=additional_fields, study=study)
        doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                               item_name='field', item_value='', attr_name='name', attr_value='disease') # We currently do not capture this in a concise way
        doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                               item_name='field', item_value='', attr_name='name', attr_value='ptm_modification') # Proteins only?
        doc = EbEyeSearchService.add_submitter(doc=doc, additional_fields=additional_fields, study=study)
        
        doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                               item_name='field', item_value='Public', attr_name='name', attr_value='study_status')   
        doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                               item_name='field', item_value=f'{EbEyeSearchService.metabolights_website_link}/{study.studyIdentifier}', attr_name='name', attr_value='metabolights_link')
        doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                               item_name='field', item_value=f'{EbEyeSearchService.public_ftp_download}/{study.studyIdentifier}', attr_name='name', attr_value='ftp_download_link')
        
        doc = EbEyeSearchService.add_study_design_descriptors(doc=doc, additional_fields=additional_fields, study=study)
        doc = EbEyeSearchService.add_study_factors(doc=doc, additional_fields=additional_fields, study=study)
        doc = EbEyeSearchService.add_organism(doc=doc, additional_fields=additional_fields, study=study)
        if thomson_reuters is False:
            doc = EbEyeSearchService.add_publication(doc=doc, additional_fields=additional_fields, study=study)
        doc = EbEyeSearchService.add_metabolites(doc=doc, additional_fields=additional_fields, study=study)
        
        root.appendChild(additional_fields)
        return doc
    
    @staticmethod
    def filter_non_printable(str):
        return ''.join([c for c in str if ord(c) > 31 or ord(c) == 9])
    
    @staticmethod
    def check_for_empty(str):
        if str is not None and str != '':
            return True
        else:
            return False
    
    @staticmethod
    def conertdate_date(timestamp: int):
        timestamp = timestamp/1000 # converting to epoch
        if timestamp > 1325376000:
            date = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            return date
        else:
            return ""
    
    @staticmethod
    def add_authors(doc: Document, additional_fields: Element, study: StudyModel):
        for contact in study.contacts:
            if contact is not None:
                doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                       item_name='field', item_value=EbEyeSearchService.get_author(contact=contact), attr_name='name', 
                                                       attr_value='author')
        return doc
    
    @staticmethod
    def add_detailed_authors(doc: Document, root: Element, study: StudyModel):
        authorship = doc.createElement('authorship')
        for contact in study.contacts:
            if contact is not None:
                author = doc.createElement('author')
                name = contact.firstName + " " +contact.lastName
                if EbEyeSearchService.check_for_empty(name):
                     doc = create_generic_element(doc, author, 'name', name)
                affliation = contact.affiliation
                if EbEyeSearchService.check_for_empty(affliation):
                    doc = create_generic_element(doc, author, 'affliation', affliation)
                address = contact.address
                if EbEyeSearchService.check_for_empty(address):
                    doc = create_generic_element(doc, author, 'address', address)
                email =  contact.email
                if EbEyeSearchService.check_for_empty(email):
                    doc = create_generic_element(doc, author, 'email', email)
                phone = contact.phone
                if EbEyeSearchService.check_for_empty(phone):
                    doc = create_generic_element(doc, author, 'phone', phone)
                authorship.appendChild(author)
        root.appendChild(authorship)
        return doc
    
    @staticmethod
    def get_author(contact: ContactModel):
        complete_author = ""
        name = contact.firstName + " " +contact.lastName
        if EbEyeSearchService.check_for_empty(name):
            complete_author = complete_author + name + ". "
        if EbEyeSearchService.check_for_empty(contact.affiliation):
            complete_author = complete_author + contact.affiliation + ". "
        if EbEyeSearchService.check_for_empty(contact.address):
            complete_author = complete_author + contact.address + ". "
        if EbEyeSearchService.check_for_empty(contact.email):
            complete_author = complete_author + contact.email + ". "
        if EbEyeSearchService.check_for_empty(contact.phone):
            complete_author = complete_author + contact.phone + ". "
        return complete_author.strip()
    
    @staticmethod
    def add_protocols(doc: Document, additional_fields: Element, study: StudyModel):
        for protocol in study.protocols:
            if protocol is not None:
                doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                       item_name='field', item_value=EbEyeSearchService.filter_non_printable(protocol.description), attr_name='name', 
                                                       attr_value=EbEyeSearchService.format_protocol(protocol.name))
        return doc

    @staticmethod
    def add_assay(doc: Document, additional_fields: Element, study: StudyModel):
        techList = []
        platformList = []
        for assay in study.assays:
            if assay is not None:
                if EbEyeSearchService.check_for_empty(assay.technology) and assay.technology not in techList:
                    techList.append(assay.technology)
                    doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                       item_name='field', item_value=assay.technology, attr_name='name', 
                                                       attr_value='technology_type')
                if EbEyeSearchService.check_for_empty(assay.platform) and assay.platform not in platformList:
                    platformList.append(assay.platform)
                    doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                       item_name='field', item_value=assay.platform, attr_name='name', 
                                                       attr_value='instrument_platform')
        #TODO, MS: Autosampler model - Chromatography Instrument - Instrument
        #TODO, NMR: Autosampler model - Instrument
        return doc
    
    @staticmethod
    def add_submitter(doc: Document, additional_fields: Element, study: StudyModel):
        for user in study.users:
            if user is not None:
                submitter_name = EbEyeSearchService.filter_non_printable(user.firstName + " "+ user.lastName)
                doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                       item_name='field', item_value=submitter_name.strip(), attr_name='name', 
                                                       attr_value='submitter_name')
                doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                      item_name='field', item_value=user.email, attr_name='name', 
                                                      attr_value='submitter_email') 
                if EbEyeSearchService.check_for_empty(user.affiliation):
                    doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                       item_name='field', item_value=user.affiliation, attr_name='name', 
                                                       attr_value='submitter_affiliation')
                
        return doc
    
    @staticmethod
    def add_study_design_descriptors(doc: Document, additional_fields: Element, study: StudyModel):
        for descriptor in study.descriptors:
            if descriptor is not None:
                study_design_descriptor = descriptor.description
                if ":" in study_design_descriptor:
                    desc_list = study_design_descriptor.split(':')
                    doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                       item_name='field', item_value=desc_list[1], attr_name='name', 
                                                       attr_value='study_design')
                    doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                       item_name='field', item_value=desc_list[1], attr_name='name', 
                                                       attr_value='curator_keywords')
                else:
                    doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                       item_name='field', item_value=study_design_descriptor, attr_name='name', 
                                                       attr_value='study_design')
                    doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                       item_name='field', item_value=study_design_descriptor, attr_name='name', 
                                                       attr_value='curator_keywords')
        return doc
    
    @staticmethod
    def add_study_factors(doc: Document, additional_fields: Element, study: StudyModel):
        for factor in study.factors:
            if factor is not None:
                if EbEyeSearchService.check_for_empty(factor.name):
                    doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                       item_name='field', item_value=factor.name, attr_name='name', 
                                                       attr_value='study_factor')
        return doc
            
    @staticmethod
    def add_organism(doc: Document, additional_fields: Element, study: StudyModel):
        for organism in study.organism:
            if organism is not None:
                if EbEyeSearchService.check_for_empty(organism.organismName):
                    doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                       item_name='field', item_value=organism.organismName, attr_name='name', 
                                                       attr_value='organism')
                if EbEyeSearchService.check_for_empty(organism.organismPart):
                    doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                       item_name='field', item_value=organism.organismPart, attr_name='name', 
                                                       attr_value='organism_part')
        return doc
                    
    @staticmethod
    def add_publication(doc: Document, additional_fields: Element, study: StudyModel):
        for publication in study.publications:
            if publication is not None:
                complete_publication = EbEyeSearchService.process_publication(publication=publication)
                if EbEyeSearchService.check_for_empty(complete_publication):
                    doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                       item_name='field', item_value=complete_publication, attr_name='name', 
                                                       attr_value='publication')
        return doc
    
    @staticmethod
    def add_structured_publication(doc: Document, root: Element, study: StudyModel):
        publications_elem = doc.createElement('publications')
        for publication in study.publications:
            if publication is not None:
                publication_elem = doc.createElement('publication')
                title = publication.title
                pubmed = EbEyeSearchService.tidy_pubmed(publication.pubmedId)
                doi = EbEyeSearchService.tidy_doi(publication.doi)
                if EbEyeSearchService.check_for_empty(title.strip()):
                    doc = create_generic_element(doc, publication_elem, 'title', title.strip())
                if EbEyeSearchService.check_for_empty(pubmed):
                    doc = create_generic_element(doc, publication_elem, 'pubmed',pubmed)
                if EbEyeSearchService.check_for_empty(doi):
                    doc = create_generic_element(doc, publication_elem, 'doi',doi)
                publications_elem.appendChild(publication_elem)
        root.appendChild(publications_elem)
        return doc
                    
    @staticmethod
    def process_publication(publication: PublicationModel):
        complete_publication = ''
        sep = '.'
        pubmed = EbEyeSearchService.tidy_pubmed(publication.pubmedId)
        doi = EbEyeSearchService.tidy_doi(publication.doi)
        
        if EbEyeSearchService.check_for_empty(publication.title):
            complete_publication = publication.title.strip()
            if not complete_publication.endswith(sep):
                complete_publication = complete_publication + sep
        if EbEyeSearchService.check_for_empty(doi):
            complete_publication = complete_publication + " " + doi
            if not complete_publication.endswith(sep):
                complete_publication = complete_publication + sep
        if EbEyeSearchService.check_for_empty(pubmed):
            complete_publication = complete_publication + " PMID:" + pubmed
        if EbEyeSearchService.check_for_empty(complete_publication):
            complete_publication = complete_publication.strip()
        return complete_publication

    @staticmethod
    def tidy_doi(doi):
        tided = doi.lower()
        tided = tided.replace("https://","").replace("http://","").replace("dx.", "")
        tided = tided.replace("doi.org/","")
        tided = tided.replace("doi:","")
        tided = tided.replace("na","")
        tided = tided.replace("n/a","")
        return tided.strip()
    
    @staticmethod
    def tidy_pubmed(pubmed):
        tidied = pubmed.lower()
        tidied = tidied.replace("none","")
        tidied = tidied.replace("na","")
        tidied = tidied.replace("n/a","")
        return tidied.strip()
    
    @staticmethod
    def add_metabolites(doc: Document, additional_fields: Element, study: StudyModel):
        for metabolite in EbEyeSearchService.metabolite_list:
            if EbEyeSearchService.check_for_empty(metabolite) and len(metabolite) <= 8191:
                    doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                       item_name='field', item_value=metabolite, attr_name='name', 
                                                       attr_value='metabolite_name')
        return doc
    
    @staticmethod
    def add_files(doc: Document, additional_fields: Element, study: StudyModel):
        ignore_folders = ['AUDIT_FILES', 'INTERNAL_FILES']
        study_metadata_root = os.path.join(EbEyeSearchService.study_root, study.studyIdentifier)
        study_metadata_path = pathlib.Path(study_metadata_root)
        for item in study_metadata_path.iterdir():
            item_base = os.path.basename(item)
            if item.is_file():
                if item_base.endswith(".txt") or item_base.endswith(".tsv"):
                    public_ftp_study_full_path =  os.path.join(EbEyeSearchService.public_ftp_download, study.studyIdentifier, item_base)
                    #logger.debug(f"Metadata File - {public_ftp_study_full_path}")
                    doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                       item_name='field', item_value=public_ftp_study_full_path, attr_name='name', 
                                                       attr_value='dataset_file')
                    
            else: 
                if item_base not in ignore_folders:
                    files_list = list(item.rglob("*"))
                    for file in files_list:
                        if file.is_file():
                            file_ext = file.suffix
                            if file_ext.lower() in EbEyeSearchService.raw_files_list or file_ext.lower() in EbEyeSearchService.derived_files_list or file_ext.lower() == '.zip':
                                file_abs_path = str(file)
                                public_ftp_study_full_path = file_abs_path.replace(EbEyeSearchService.study_root, EbEyeSearchService.public_ftp_download)
                                doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                                        item_name='field', item_value=public_ftp_study_full_path, attr_name='name', 
                                                        attr_value='dataset_file')
                            
        return doc
            
    @staticmethod
    def format_protocol(str):
        if str is not None and str != '':
            str = str.replace(" ", "_")
            str = str.lower() + "_protocol"
            return str
        else:
            return ""
        
    @staticmethod
    def get_email_by_token(user_token: str):
        with DBManager.get_instance().session_maker() as db_session:
            user = db_session.query(User.email).filter(User.apitoken == user_token).first()
            if not user:
                raise MetabolightsDBException("No user")
            
            email = user.email
        return email
    
    @staticmethod
    def get_compound(compound_acc: str):
        doc = Document()
        root = doc.createElement('database')
        doc.appendChild(root)
        doc = create_generic_element(doc, root, 'name', EbEyeSearchService.study_resource_name)
        doc = create_generic_element(doc, root, 'description', EbEyeSearchService.study_resource_description)
        doc = create_generic_element(doc, root, 'release', "1.0")
        doc = create_generic_element(doc, root, 'release_date', datetime.date.today().strftime('%Y-%m-%d'))
        entries = doc.createElement('entries')
        root.appendChild(entries)
        doc = EbEyeSearchService.process_compound(doc=doc, root=entries, compound_acc=compound_acc)
        return doc
    
    @staticmethod
    def export_compounds(user_token: str):
        start_time = time.time()
        email = EbEyeSearchService.get_email_by_token(user_token=user_token)
        compound_list = CompoundService.get_instance().get_all_compounds()
        doc = Document()
        root = doc.createElement('database')
        doc.appendChild(root)
        doc = create_generic_element(doc, root, 'name', EbEyeSearchService.study_resource_name)
        doc = create_generic_element(doc, root, 'description', EbEyeSearchService.study_resource_description)
        doc = create_generic_element(doc, root, 'release', "1.0")
        doc = create_generic_element(doc, root, 'release_date', datetime.date.today().strftime('%Y-%m-%d'))
        doc = create_generic_element(doc, root, 'entry_count', str(len(compound_list)))
        entries = doc.createElement('entries')
        root.appendChild(entries)
        i=0
        for compound_acc in compound_list:
            logger.info(f"EB EYE search export processing starting for compound  - {compound_acc}")
            doc = EbEyeSearchService.process_compound(doc=doc, root=entries, compound_acc=compound_acc)
            logger.info(f"processing completed for the compound  - {compound_acc}")
            i = i+1
        logger.info(f"processing completed for all the compounds; Processed count  - {i}")
        xml_str = doc.toprettyxml(indent="")
        add_metabolights_data(content_name=EbEyeSearchService.eb_eye_public_compounds_ebi, data_format=EbEyeSearchService.content_type_xml, content=xml_str)
        logger.info("Data stored to DB!")
        processed_time = (time.time() - start_time)/60
        result = f"Processed compounds count - {i}; Process completed in {processed_time} minutes"
        send_email("EB EYE Compounds export completed", result, None, email, None)
        return {"processed_compounds": i, "completed_in": processed_time}
    
    @staticmethod
    def process_compound(doc: Document, root: Element, compound_acc: str):
        try:
            compound = CompoundService.get_instance().get_compound(compound_acc=compound_acc)
            entry = doc.createElement('entry')
            entry.setAttribute(attname='id', value=compound_acc)
            root.appendChild(entry)
            if EbEyeSearchService.check_for_empty(compound.name):
                doc = create_generic_element(doc, entry, 'name', EbEyeSearchService.filter_non_printable(compound.name))
            if EbEyeSearchService.check_for_empty(compound.description):
                doc = create_generic_element(doc, entry, 'description', EbEyeSearchService.filter_non_printable(compound.description))
            doc = EbEyeSearchService.add_cross_references_for_compound(doc=doc, root=entry, compound=compound)
            doc = EbEyeSearchService.add_dates_for_compound(doc=doc, root=entry, compound=compound)
            doc = EbEyeSearchService.add_additional_fields_for_compounds(doc=doc, root=entry, compound=compound) 
                
        except Exception as ex:
            logger.error(f"Exception while processing compound - {compound_acc}; reason: {str(ex)}")
        return doc
    
    @staticmethod
    def add_cross_references_for_compound(doc: Document, root: Element, compound: MetaboLightsCompoundModel):
        EbEyeSearchService.study_acc_list = []
        xrefs = doc.createElement('cross_references')
        for cross_reference in compound.crossReference:
            if cross_reference is not None:
                db_name = cross_reference.db.name
                if db_name.upper() == 'MTBLS':
                    if cross_reference.accession not in EbEyeSearchService.study_acc_list:
                        EbEyeSearchService.study_acc_list.append(cross_reference.accession)
                    ref = doc.createElement('ref')
                    ref.setAttribute(attname= 'dbkey', value= cross_reference.accession)
                    ref.setAttribute(attname= 'dbname', value= db_name.upper())
                    xrefs.appendChild(ref)
                    
                elif db_name.upper() == 'CHEBI':
                    if cross_reference.accession != compound.chebiId:
                        ref = doc.createElement('ref')
                        ref.setAttribute(attname= 'dbkey', value= cross_reference.accession)
                        ref.setAttribute(attname= 'dbname', value= db_name)
                        xrefs.appendChild(ref)
                else:
                    ref = doc.createElement('ref')
                    ref.setAttribute(attname= 'dbkey', value= cross_reference.accession)
                    ref.setAttribute(attname= 'dbname', value= db_name)
                    xrefs.appendChild(ref)
        # Add Chebi crossreference
        ref = doc.createElement('ref')
        ref.setAttribute(attname= 'dbkey', value= compound.chebiId)
        ref.setAttribute(attname= 'dbname', value= 'ChEBI')
        xrefs.appendChild(ref)
        root.appendChild(xrefs)
        return doc
        
    @staticmethod
    def add_dates_for_compound(doc: Document, root: Element, compound: MetaboLightsCompoundModel):
        dates_elem = doc.createElement('dates')
        date_elem = doc.createElement('date')
        date_elem.setAttribute(attname= 'type', value= 'last_modification')
        date_elem.setAttribute(attname= 'value', value= compound.updatedDate)
        dates_elem.appendChild(date_elem)
        root.appendChild(dates_elem)
        return doc
    
    @staticmethod
    def add_additional_fields_for_compounds(doc: Document, root: Element, compound: MetaboLightsCompoundModel):
        additional_fields = doc.createElement('additional_fields')
        if EbEyeSearchService.check_for_empty(compound.inchi):
            doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                               item_name='field', item_value=compound.inchi, attr_name='name', attr_value='inchi')
        if EbEyeSearchService.check_for_empty(compound.iupacNames):
            doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                               item_name='field', item_value=compound.iupacNames, attr_name='name', attr_value='iupac')
        if EbEyeSearchService.check_for_empty(compound.formula):
            doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                               item_name='field', item_value=compound.formula, attr_name='name', attr_value='formula')
        for met_species in compound.metSpecies:
            if EbEyeSearchService.check_for_empty(met_species.species):
                doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                               item_name='field', item_value=met_species.species, attr_name='name', attr_value='organism')
                #if met_species.species.lower() == 'reference compound':
                     # Todo
                #else:
                if met_species.speciesMember is not None:
                    doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                               item_name='field', item_value=met_species.speciesMember.speciesGroup.name, attr_name='name', attr_value='organism_group')
                    
        for study_acc in EbEyeSearchService.study_acc_list:
            doc = create_generic_element_attribute(doc=doc, root=additional_fields, 
                                               item_name='field', item_value=study_acc, attr_name='name', attr_value='study')
        root.appendChild(additional_fields)
        return doc