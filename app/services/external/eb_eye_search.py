from xml.dom.minidom import Document, Element
from app.ws.db.models import StudyModel
from app.ws.dom_utils import create_generic_element, create_generic_element_attribute
from app.ws.study.study_service import StudyService


class EbEyeSearchService():
    
    @staticmethod
    def process_study(study: StudyModel):
        doc = Document()
        root = doc.createElement('entry')
        root.setAttribute(attname='id', value=study.studyIdentifier)
        doc.appendChild(root)
        doc = create_generic_element(doc, root, 'name', EbEyeSearchService.filter_non_printable(study.title))
        doc = create_generic_element(doc, root, 'description', EbEyeSearchService.filter_non_printable(study.description))
        #doc = create_generic_element_attribute(doc, root, 'test-elem1', '74666', 'type', 'new')
        #doc = create_generic_element_attribute(doc, root, 'test-elem2', '', 'type', 'new')
        doc = EbEyeSearchService.add_cross_references(doc=doc, root=root, study=study)
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
                
        df_data_dict = StudyService.get_instance().get_study_maf_rows(study_id=study.studyIdentifier, sheet_number=1)
        rows_dict = df_data_dict['rows']
        if rows_dict is not None :
            xrefs = EbEyeSearchService.process_maf_rows(doc, xrefs, rows_dict)
        root.appendChild(xrefs)
        return doc
        
    @staticmethod
    def process_maf_rows(doc: Document, xrefs: Element, rows_dict):
        metabolite_list = []
        xref_list = []
        for row in rows_dict:
            database_identifier = row['database_identifier']
            metabolite_identification = row['metabolite_identification']
            if database_identifier != '' and metabolite_identification != '':
                metname = EbEyeSearchService.filter_non_printable(metabolite_identification)
                if metname not in metabolite_list:
                    metabolite_list.append(metname)
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
        return xrefs
    
    @staticmethod
    def filter_non_printable(str):
        return ''.join([c for c in str if ord(c) > 31 or ord(c) == 9])