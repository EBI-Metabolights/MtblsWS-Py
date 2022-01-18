import logging
from datetime import datetime

import pandas
import requests
import xmltodict
from cascadict import CascaDict
from fuzzywuzzy import fuzz
from flask import current_app as app, abort
from typing import List, Union

from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient

logger = logging.getLogger('wslog')


class EuropePmcReportBuilder:
    """Class that builds the EuropePMC Report. The report is a result of cross referencing the publication information
    that submitters give us with externally sourced publication information found in EuropePMC. This allows us to check
    for discrepancies / differences."""

    def __init__(self, study_list: List[str], user_token: str, wsc: WsClient, iac: IsaApiClient):
        """Init method
        Sets up a headers register (as we are hitting the same endpoint twice, but with different formats) and a set of
        base parameters for requests to the europePMC API.

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
        Get a list of result dicts (each of which represent a row) and try to build a dataframe out of them. If
        successful, save that dataframe as a csv file to our reporting directory, and return a message indicating
        success. If not successful, log the error, and return a message indicating failure.

        :return: A message as a string indicating success or failure.
        """
        list_of_result_dicts = [row for study in self.study_list for row in self.process(study)]
        path = app.config.get('MTBLS_PRIVATE_FTP_ROOT') + '/' + app.config.get('REPORTING_PATH') + 'global/europepmc.csv'
        try:

            report_dataframe = pandas.DataFrame(list_of_result_dicts,
                                                columns=['Identifier', 'Title', 'Submission Date',
                                                         'Status', 'Release Date', 'PubmedID', 'DOI', 'Author List',
                                                         'Publication Date', 'Citation Reference',
                                                         'Publication in MTBLS', 'Journal in EuropePMC',
                                                         'Released before curated?']
                                                )
            report_dataframe.to_csv(path, sep='\t')
            msg = 'EuropePMC report successfully saved to {0}'.format(path)
            logger.info(msg)
        except Exception as e:
            msg = 'Problem in building and saving europe pmc report: {0}'.format(e)
            logger.error(msg)
            abort(500, msg)

        return msg

    def process(self, study_id) -> List:
        """
        Process an individual study_id from the study list. First ping our java webservice to get some basic information
        about the study. Then we ping the IsaApi client so that we can get title and publication information.
        We then iterate over the publications from the IAC, pinging europePMC for each one, creating a dict for each.

        :param study_id: current study_id to process.
        :return: List of Dicts that each represent a row in the generated report.
        """
        row_dicts = []
        self.session.headers.update(self.headers_register['article'])
        # kind of unsavoury to do this iteratively but saves me writing another method that does much the same thing
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = self.wsc.get_permissions(study_id, self.user_token)

        base_return_dict = CascaDict({
            'Identifier': study_id,
            'Title': 'N/A',
            'Submission Date': submission_date,
            'Status': study_status,
            'Release Date': release_date,
            'PubmedID': 'N/A',
            'DOI': 'N/A',
            'Author List': 'N/A',
            'Publication Date': 'N/A',
            'Citing Reference': 'N/A',
            'Publication in MTBLS': 'N/A',
            'Journal in EuropePMC': 'N/A',
            'Released before curation finished?': 'N/A'
        })

        isa_study, isa_inv, std_path = self.iac.get_isa_study(study_id, self.user_token,skip_load_tables=True,
                                                              study_location=study_location, failing_gracefully=True)

        # if get_isa_study has failed, isa_study will come back as None, and so we won't have any publication
        # information to work with. So we just return the very basic dict.
        if isa_study is None:
            row_dicts.append(base_return_dict)
            return row_dicts

        title = isa_study.title
        publications = isa_study.publications

        fresh_params = self.base_params.cascade({'query': title, 'format': 'JSON'})
        # here we just search the article title rather than the specific publication
        europepmc_study_search_results = self.session.get(self.europe_pmc_url, params=fresh_params).json()
        # if there is an issue with query then just return the basic details dict.
        if 'resultList' not in europepmc_study_search_results:
            row_dicts.append(base_return_dict.cascade({'Title': title}))
            return row_dicts

        culled_results = [
            result for result
            in europepmc_study_search_results['resultList']['result']
            if fuzz.ratio(result['title'], title) > 80
        ]
        if len(culled_results) > 0:
            for pub in publications:
                logger.info(pub)
                result = self.has_mapping(pub, culled_results)
                if result:
                    logger.info('hit ' + str(result))
                    temp_dict = base_return_dict.cascade({
                        'Title': title, 'PubmedId': result['pmid'], 'DOI': pub.doi, 'Author List': pub.author_list,
                        'Publication Date': result['journalInfo']['printPublicationDate'],
                        'Citation Reference': self.get_citation_reference(title), 'Publication in MTBLS': pub.title,
                        'Journal in EuropePMC': result['journalInfo']['journal']['title'],
                        'Released before curated?': self.assess_if_trangressed(
                            study_status, result['journalInfo']['journal'])
                    })
                else:
                    temp_dict = base_return_dict.cascade({
                        'Title': title, 'PubmedId': pub.pubmed_id, 'DOI': pub.doi, 'Author List': pub.author_list,
                        'Publication Date': 'N/A',
                        'Citation Reference': self.get_citation_reference(title), 'Publication in MTBLS': pub.title,
                        'Journal in EuropePMC': 'N/A', 'Publication the same?': False,
                        'Released before curated?': 'N/A'
                    })
                row_dicts.append(temp_dict)
        if len(publications) is 0:
            row_dicts.append(base_return_dict)

        return row_dicts

    @staticmethod
    def has_mapping(publication, resultset):
        """Check whether a given publication has a match in the europePMC resultset"""
        for result in resultset:
            logger.info(result['source'] + str(len(result['source'])))
            if result['source'] == 'PPR': #preprint so doesnt have an actual title.

                continue
            else:
                score = fuzz.ratio(result['title'], publication.title)
                logger.info('HASMAPPING: ' + str(score) + 'MTB: ' + publication.title + '/PMC: ' +
                            result['title'])
                if score > 80:
                    return result
        return None

    @staticmethod
    def assess_if_trangressed(status, europe_pmc_publication) -> Union[bool, str]:
        """Check whether the journal has been published despite study not being public."""
        logger.info('HERE')
        logger.info('ASSESSIF' + europe_pmc_publication)
        if 'printPublicationDate' in europe_pmc_publication:
            journal_publication_date = datetime.strptime(europe_pmc_publication['printPublicationDate'], '%Y-%m-%d')
            logger.info('ASSESSIF' + str(journal_publication_date))
            now = datetime.now()
            return status.upper() is not 'PUBLIC' and now > journal_publication_date
        else:
            return 'No publication date given.'

    def get_citation_reference(self, title) -> str:
        """Cascade a new param dict to use in the request and update the session headers to XML as the search endpoint
        on the EuropePMC API only returns the bibliographicCitation information if you specify the DC format (which is
        a kind of XML). Turn the resulting XML string into a dict, and then return the citation from that dict.

        :param title: Article title to get citation for
        :return: Bibliographic citation as string."""
        fresh_params = self.base_params.cascade({'format': 'DC', 'query': title})
        self.session.headers.update(self.headers_register['citation_ref'])
        response = self.session.get(self.europe_pmc_url, params=fresh_params)
        response_xmldict = xmltodict.parse(response.text)
        # type is infuriatingly not consistent in responses from europepmc so we have to handle it ourselves.
        if type(response_xmldict['responseWrapper']['rdf:RDF']['rdf:Description']) is list:
            return response_xmldict['responseWrapper']['rdf:RDF']['rdf:Description'][0]['dcterms:bibliographicCitation']
        else:
            return response_xmldict['responseWrapper']['rdf:RDF']['rdf:Description']['dcterms:bibliographicCitation']
