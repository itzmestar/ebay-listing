import os
import logging
import time

import requests
import pandas as pd
from datetime import datetime
from python_calamine.pandas import pandas_monkeypatch
import xml.etree.ElementTree as ET
from requests_toolbelt.multipart.encoder import MultipartEncoder
from ebaysdk.trading import Connection as Trading

pandas_monkeypatch()

# Configure logging
logging_file = os.path.join(
        os.getcwd(),
        os.path.splitext(os.path.basename(__file__))[0] +
        datetime.now().strftime("%y%m%d") + '.log'
    )
log_format = '%(asctime)s : %(levelname)s : %(funcName)s : %(message)s'
logging.basicConfig(
    filename=logging_file,
    filemode='a',
    format=log_format,
    level=logging.DEBUG
)


class EbayAPI:

    def __init__(self, client_id: str, client_secret: str, dev_id: str, test: bool = False):
        self.client_id = client_id
        self.client_secret = client_secret
        self.dev_id = dev_id
        self.base_url = None
        self.df = None
        self.token = {}
        self.test = test
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
            'scope': ["https://api.ebay.com/oauth/api_scope", "https://api.ebay.com/oauth/api_scope/sell.inventory", "https://api.ebay.com/oauth/api_scope/sell.marketing", "https://api.ebay.com/oauth/api_scope/sell.account", "https://api.ebay.com/oauth/api_scope/sell.fulfillment"],
            'grant_type': 'client_credentials'
        }

        response = requests.post(
            self.base_url + uri,
            headers=headers,
            auth=(self.client_id, self.client_secret),
            data=payload
        )

        if response.ok:
            self.token = response.json()
        logging.info(self.token)

    def read_excel(self, excel_filename: str, sheet: str = 'Listings'):
        """
        Read excel & convert to dataframe
        :param excel_filename:
        :param sheet:
        :return:
        """
        logging.info('started')
        self.df = pd.read_excel(excel_filename, sheet_name=sheet, header=3, engine="calamine")
        self.df = self.df.dropna(how='all')
        print(self.df)
        logging.info('finished')

    def _get_xml_request(self):
        token = self.token.get('access_token')
        upload_Pictures_XML = (
            '<?xml version="1.0" encoding="utf-8"?>\n'
            '<UploadSiteHostedPicturesRequest xmlns="urn:ebay:apis:eBLBaseComponents">\n\t'
            '<RequesterCredentials>\n\t\t'
            f'<ebl:eBayAuthToken xmlns:ebl="urn:ebay:apis:eBLBaseComponents">{token}</ebl:eBayAuthToken>\n\t\t'
            '</RequesterCredentials>\n\t'
            '<PictureSet>Supersize</PictureSet>\n'
            '</UploadSiteHostedPicturesRequest>\n'
        )
        logging.debug(upload_Pictures_XML)
        return upload_Pictures_XML

    @staticmethod
    def add_image_as_attachment(filename, request):
        try:
            with open(filename, 'rb') as f:
                file_content = f.read()
            return {
                'request': request,
                'file': (os.path.basename(filename), file_content),
            }
        except Exception as e:
            logging.exception(e)
            return None

    def upload_image1(self, filename: str):
        logging.info("started")
        uri = '/ws/api.dll'
        headers = {
            "SOAPAction": "",
            "X-EBAY-API-SESSION-CERTIFICATE": f"{self.client_id};{self.dev_id};{self.client_secret}",
            "X-EBAY-API-COMPATIBILITY-LEVEL": "967",
            "X-EBAY-API-DEV-NAME": self.dev_id,
            "X-EBAY-API-APP-NAME": self.client_id,
            "X-EBAY-API-CERT-NAME": self.client_secret,
            "X-EBAY-API-SITEID": "0",
            "X-EBAY-API-DETAIL-LEVEL": "0",
            "X-EBAY-API-CALL-NAME": "UploadSiteHostedPictures",
            'Content-Type': 'application/xml'
        }

        request_xml = self._get_xml_request()

        try:
            parts = self.add_image_as_attachment(filename, request_xml)
            if parts:
                multipart_data = MultipartEncoder(fields=parts)
                headers['Content-Type'] = multipart_data.content_type
                response = requests.post(self.base_url + uri, data=multipart_data, headers=headers)
                logging.info(response.text)
            '''files = [('file', (os.path.basename(filename), open(filename, 'rb'), 'image/jpg'))]
            response = requests.post(self.base_url + uri, data=request_xml, headers=headers, files=files)
            logging.info(response.content)'''
        except Exception as ex:
            logging.exception(ex)

    def upload_image(self, filename):
        token = self.token.get('access_token')
        if self.test:
            domain = 'api.sandbox.ebay.com'
        else:
            domain = 'api.ebay.com'
        api = Trading(domain=domain,
                      appid=self.client_id, devid=self.dev_id, certid=self.client_secret,
                      token=token, config_file=None)

        files = [('file', (os.path.basename(filename), open(filename, 'rb'), 'image/jpg'))]

        response = api.execute('UploadSiteHostedPictures',
                               {"PictureSet": "Supersize"},
                               files=files
                               )
        return response.content

    def bulk_create_or_replace_inventory_item(self, inventory_items: list):
        logging.info("started")
        uri = '/sell/inventory/v1/bulk_create_or_replace_inventory_item'

        payload = {
            "requests": inventory_items
        }
        token = self.token.get('access_token')
        headers = {
            'Content-Language': 'en-US',
            'Content-Type': 'application/json',
            'Authorization': f'IAF {token}'
        }

        try:
            response = requests.post(self.base_url + uri, headers=headers, json=payload)
            logging.debug(response.json())
        except Exception as e:
            logging.exception(e)

    def _generate_inventory_payload(self, row):
        return {}

    def workflow(self):
        logging.info("Started")

        inventory_items = []

        # iterate over dataframe
        for index, row in self.df.iterrows():
            try:
                print(row)
                payload = self._generate_inventory_payload(row)
                inventory_items.append(payload)
                # for each 20 items, send a listing request
                if index % 19 == 0:
                    self.bulk_create_or_replace_inventory_item(inventory_items)
                    # empty the inventory_items
                    inventory_items.clear()

                # 2-minute pause after every 145 listings
                if index % 144 == 0:
                    print("Sleeping for 2 min...")
                    time.sleep(120)
            except Exception as e:
                logging.exception(e)

        # if items in inventory_items then list them
        if inventory_items:
            self.bulk_create_or_replace_inventory_item(inventory_items)
            # empty the inventory_items
            inventory_items.clear()


items = [
        {
            "sku": "Bsistuecf",
            "locale": "en_US",
            "product": {
                "title": "Boston Terriers Collector Plate &quot;All Ears by Dan Hatala - The Danbury Mint",
                "aspects": {
                    "Country/Region of Manufacture": [
                        "United States"
                    ]
                },
                "description": "All Ears by Dan Hatala. A limited edition from the collection entitled 'Boston Terriers'. Presented by The Danbury Mint.",
            },
            "condition": "USED_EXCELLENT",
            "conditionDescription": "Mint condition. Kept in styrofoam case. Never displayed.",
            "availability": {
                "shipToLocationAvailability": {
                    "quantity": 2
                }
            }
        },
        {
            "sku": "Jiiiaassh",
            "locale": "en_US",
            "product": {
                "title": "JOE PAVELSKI 2015-16 BOBBLEHEAD NHL SAN JOSE SHARKS 25TH ANNIVERSARY",
                "aspects": {
                    "Team": [
                        "San Jose Sharks"
                    ],
                    "Player": [
                        "Joe Pavelski"
                    ],
                    "Pre & Post Season": [
                        "Regular Season"
                    ],
                    "Product": [
                        "Bobblehead"
                    ],
                    "Country/Region of Manufacture": [
                        "China"
                    ],
                    "Brand": [
                        "Success Promotions"
                    ],
                    "UPC": [
                        "Does not apply"
                    ]
                },
                "description": "Joe Pavelski bobble head from 2015-16 season, the 25th season of the San Jose Sharks. New in box.",
            },
            "condition": "NEW",
            "availability": {
                "shipToLocationAvailability": {
                    "quantity": 1
                }
            }
        }
    ]

e = EbayAPI(client_id='MBNirist-listings-SBX-64d901dbc-f48550d5',
            client_secret='SBX-4d901dbcf472-f97a-44a6-8b95-7fed',
            dev_id='813cd451-631c-46e7-8ab5-94bf72be1305',
            test=True
            )

e.read_excel('uploud.xlsx')
#e.fetch_access_token()
#e.upload_image1('t.jpg')
#e.bulk_create_or_replace_inventory_item(items)
e.workflow()

