import requests
import pandas as pd
from python_calamine.pandas import pandas_monkeypatch
pandas_monkeypatch()


class EbayAPI:

    def __init__(self, client_id: str, client_secret: str, dev_id: str, test: bool = False):
        self.client_id = client_id
        self.client_secret = client_secret
        self.dev_id = dev_id
        self.base_url = None
        self.df = None
        self.session = requests.Session()
        if test:
            # sandbox url
            self.base_url = 'https://api.sandbox.ebay.com'
        else:
            # production url
            self.base_url = 'https://api.ebay.com'

    def fetch_access_token(self):
        uri = '/identity/v1/oauth2/token'

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        payload = {
            'grant_type': 'client_credentials'
        }

        response = requests.post(
            self.base_url + uri,
            headers=headers,
            auth=(self.client_id, self.client_secret),
            data=payload
        )

        print(response.json())

    def read_excel(self, excel_filename: str, sheet: str = 'Listings'):
        """
        Read excel & convert to dataframe
        :param excel_filename:
        :param sheet:
        :return:
        """
        self.df = pd.read_excel(excel_filename, sheet_name=sheet, header=3, engine="calamine")
        self.df = self.df.dropna(how='all')
        print(self.df)


e = EbayAPI(client_id='MBNirist-listings-SBX-64d901dbc-f48550d5',
            client_secret='SBX-4d901dbcf472-f97a-44a6-8b95-7fed',
            dev_id='813cd451-631c-46e7-8ab5-94bf72be1305',
            test=True
            )

e.read_excel('uploud.xlsx')
