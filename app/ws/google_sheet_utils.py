
import urllib

import gspread
import numpy as np
import pandas as pd
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from owlready2 import urllib
import psycopg2
from app.config import get_settings

from app.services.storage_service.acl import Acl
from app.services.storage_service.storage_service import StorageService
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study
from app.ws.db.types import StudyStatus
from app.ws.db_connection import get_study_info, get_study_by_type, get_public_studies
from app.ws.misc_utilities.dataframe_utils import DataFrameUtils
from app.ws.mtblsWSclient import WsClient
from app.ws.study.commons import create_ftp_folder
from app.ws.study.user_service import UserService
from app.ws.utils import log_request, writeDataToFile


def curation_log_database_query():
    try:
        settings = get_settings()
        params = settings.database.connection.dict()
        with psycopg2.connect(**params) as conn:
            sql = open('./resources/updateDB.sql', 'r').read()
            data = pd.read_sql_query(sql, conn)

        percentage_known = round(
            data['maf_known'].astype('float').div(data['maf_rows'].replace(0, np.nan)).fillna(0) * 100, 2)

        data.insert(data.shape[1] - 1, column="percentage known", value=percentage_known)

        google_df = getGoogleSheet(get_settings().google.sheets.mtbls_curation_log, 'Database Query',
                                   get_settings().google.connection.google_sheet_api.dict())

        data.columns = google_df.columns

        replaceGoogleSheet(data, get_settings().google.sheets.mtbls_curation_log, 'Database Query',
                           get_settings().google.connection.google_sheet_api.dict())

    except Exception as e:
        print(e)

        
def getGoogleSheet(url, worksheetName, credentials_dict):
    '''
    get google sheet
    :param url: url of google sheet
    :param worksheetName: work sheet name
    :return: data frame
    '''
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials._from_parsed_json_keyfile(credentials_dict, scope)
    gc = gspread.authorize(credentials)
    # wks = gc.open('Zooma terms').worksheet('temp')
    wks = gc.open_by_url(url).worksheet(worksheetName)
    content = wks.get_all_records()
    # max_rows = len(wks.get_all_values())
    df = pd.DataFrame(content)
    return df


def replaceGoogleSheet(df, url, worksheetName, credentials_dict):
    '''
    replace the old google sheet with new data frame, old sheet will be clear
    :param df: dataframe
    :param url: url of google sheet
    :param worksheetName: work sheet name
    :return: Nan
    '''
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials._from_parsed_json_keyfile(credentials_dict, scope)
    gc = gspread.authorize(credentials)
    wks = gc.open_by_url(url).worksheet(worksheetName)
    wks.clear()
    set_with_dataframe(wks, df)