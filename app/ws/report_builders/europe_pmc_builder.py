import logging
from datetime import datetime

import pandas
import requests
import xmltodict
from cascadict import CascaDict
from fuzzywuzzy import fuzz
from flask import current_app as app
from typing import List

from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient

logger = logging.getLogger('wslog')


class EuropePmcReportBuilder:
    """Class that builds the EuropePMC Report. The report is a result of cross referencing the publication information
    that submitters give us with externally sourced publication information found in EuropePMC. This allows us to check
    for discrepancies / differences."""

    def __init__(self, study_list: List[str], user_token: str, wsc: WsClient, iac: IsaApiClient):
        """Init method
        :param priv_list: A list of studies to iterate over, throwing each at europePMC.
        :param user_token: User token for use with javawebservice, must be curator or will have failed prior.
        :param wsc: WsClient that interfaces with the java webservice.
        :param iac: IsaApiClient, used to get study information.
        """
        self.study_list = study_list
        self.user_token = user_token
        self.wsc = wsc
        self.iac = iac
        self.session = requests.Session()
        self.europe_pmc_url = 'https://www.ebi.ac.uk/europepmc/webservices/rest/search'
        self.headers_register = {
            'article': {'Accept': 'application/json'},
            'citation_ref': {'Accept': 'application/xml'}
        }
        self.base_params = CascaDict({
            'resultType': 'core',
            'format': 'JSON',
            'cursorMark': '*',
            'pageSize': '5',
            'fromSearchPost': False,
            'query': ''
        })

    def build(self) -> str:
        """
        Initialise a new session object as we will be pinging the europeepmc API a lot. Iterates over the list of study
        ID's, gets the study information on our end via the isa api client, and then pings the europepmc api with the
        study title as the query term. If the result is correct, a new row is created with both metabolights and
        europepmc information. If not, a row with just metabolights information is created.
        """

        list_of_result_dicts = []
        for study_id in self.study_list:
            self.session.headers.update(self.headers_register['article'])
            # kind of unsavoury to do this iteratively but saves me writing another method that does much the same thing
            is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = \
                self.wsc.get_permissions(study_id, self.user_token)
            isa_study, isa_inv, std_path = self.iac.get_isa_study(study_id, self.user_token,
                                                                  skip_load_tables=True,
                                                                  study_location=study_location)

            title = isa_study.title
            publications = isa_study.publications
            fresh_params = self.base_params.cascade({'query': title, 'format': 'JSON'})
            # params['query'] = title
            # here we just search the article title rather than the specific publication
            europepmc_study_search_results = self.session.get(self.europe_pmc_url, params=fresh_params).json()
            logger.info(europepmc_study_search_results['resultList'])
            culled_results = [
                result for result
                in europepmc_study_search_results['resultList']['result']
                if fuzz.ratio(result['title'], title) > 80
            ]
            # if fuzz.ratio(title, europepmc_study_search_results['resultList']['result'][0]['title']) > 80:
            if len(culled_results) > 0:
                for publication in publications:
                    # fuzzy match the titles as there can be slight differences

                    result = self.has_mapping(publication, culled_results)
                    if result:
                        temp_dict = {
                                'Identifier': study_id,
                                'Title': title,
                                'Submission Date': submission_date,
                                'Status': study_status,
                                'Release Date': release_date,
                                'Pubmed ID': publication.pubmed_id,
                                'DOI': publication.doi,
                                'Author List': publication.author_list,
                                'Publication Date': result['journalInfo']['printPublicationDate'],
                                'Citation Reference': '',
                                'Publication in MTBLS': publication.title,
                                'Publication in EuropePMC': result['journalInfo']['journal']['title'],
                                'Publication the same?': True,
                                'Released before curated?': self.assess_if_trangressed(
                                    study_status, result['journalInfo']['journal'])
                            }

                    else:
                        temp_dict = {
                                'Identifier': study_id,
                                'Title': title,
                                'Submission Date': submission_date,
                                'Status': study_status,
                                'Release Date': release_date,
                                'Pubmed ID': publication.pubmed_id,
                                'DOI': publication.doi,
                                'Author List': publication.author_list,
                                'Publication Date': '',
                                'Citing Reference': '',
                                'Publication in MTBLS': publication.title,
                                'Publication in EuropePMC': 'N/A',
                                'Publication the same?': False,
                                'Released before curation finished?': ''
                            }
                    list_of_result_dicts.append(temp_dict)
            if len(publications) is 0:
                temp_dict = {
                    'Identifier': study_id,
                    'Title': title,
                    'Submission Date': submission_date,
                    'Status': study_status,
                    'Release Date': release_date,
                    'Pubmed ID': '',
                    'DOI': '',
                    'Author List': '',
                    'Publication Date': '',
                    'Citing Reference': '',
                    'Publication in MTBLS': '',
                    'Publication in EuropePMC': '',
                    'Publication the same?': '',
                    'Released before curation finished?': ''
                }
                list_of_result_dicts.append(temp_dict)

        try:
            path = app.config.get('MTBLS_PRIVATE_FTP_ROOT') + app.config.get('REPORTING PATH') + 'global/europepmc.csv'
            report_dataframe = pandas.DataFrame(list_of_result_dicts,
                                                columns=['Identifier', 'Title', 'Submission Date',
                                                         'Status', 'Release Date', 'PubmedID', 'DOI', 'Author List',
                                                         'Publication Date', 'Citing Reference', 'Publication in MTBLS',
                                                         'Publication in EuropePMC',
                                                         'Released before curation finished?']
                                                )
            report_dataframe.to_csv(path, sep='\t')
            msg = 'EuropePMC report successfully saved to {0}'.format(path)
        except Exception as e:
            msg = 'Problem in building and saving europe pmc report: {0}'.format(e)
            logger.error(msg)

        return msg

    @staticmethod
    def has_mapping(publication, resultset):
        """Check whether a given publication has a match in the europePMC resultset"""
        for result in resultset:
            if result['source'] is 'PPR': #preprint so doesnt have an actual title.
                continue
            else:
                if fuzz.ratio(result['journalInfo']['journal']['title'], publication.title) > 90:
                    return result
        return None

    @staticmethod
    def assess_if_trangressed(status, europe_pmc_publication) -> bool:
        """Check whether the journal has been published despite study not being public."""
        journal_publication_date = datetime.strptime(europe_pmc_publication['printPublicationDate'], '%Y-%m-%d')
        now = datetime.now()
        return status.upper() is not 'PUBLIC' and now > journal_publication_date

    def get_citation_reference(self, title) -> str:
        """Cascade a new param dict to use in the request and update the headers to XML as the search endpoint on the
        EuropePMC API only returns the bibliographicCitation information if you specify the DC format. Turn the
        resulting XML string into a dict, and then return the citation from that dict.

        :param title: Article title to get citation for
        :return: Bibliographic citation as string."""
        fresh_params = self.base_params.cascade({'format': 'DC', 'query': title})
        self.session.headers.update(self.headers_register['citation_ref'])
        response_xml_dict = xmltodict.parse(self.session.get(self.europe_pmc_url, params=fresh_params).text)

        return response_xml_dict['responseWrapper']['rdf:RDF']['rdf:Description']['dcterms:bibliographicCitation'][0]
