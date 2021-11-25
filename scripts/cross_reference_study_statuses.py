

import requests

def get_all_studies():

    headers = {'Accept': 'application/json', 'User_token': '4ae0c06f-a5de-41a0-bcf9-7e573c63f515'}

    session = requests.Session()
    session.headers.update(headers)
    studies = session.get('https://www.ebi.ac.uk:443/metabolights/ws/studies/private').json()['content']
    mtb_pub_url = 'https://www.ebi.ac.uk:443/metabolights/ws/studies/{0}/publications'
    study_and_publication_dict = {}
    for study in studies:

        publication_info = session.get(mtb_pub_url.format(study)).json()
        if publication_info['publications']:
            study_and_publication_dict.update({study: publication_info['publications']})
        else:
            study_and_publication_dict.update({study: []})


    print('stop')


get_all_studies()