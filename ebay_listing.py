import os
import logging
import time
import json
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from python_calamine.pandas import pandas_monkeypatch
import xml.etree.ElementTree as ET
from requests_toolbelt.multipart.encoder import MultipartEncoder
from ebaysdk.trading import Connection as Trading
from requests_oauthlib import OAuth2Session
import urllib.parse as urlparse
from pprint import pformat
import argparse
import configparser

__version__ = "v2.0.1"

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
CONFIG = None
environment = 'sandbox'
SCOPE = ["https://api.ebay.com/oauth/api_scope",
         "https://api.ebay.com/oauth/api_scope/sell.inventory",
         "https://api.ebay.com/oauth/api_scope/sell.marketing",
         "https://api.ebay.com/oauth/api_scope/sell.account",
         "https://api.ebay.com/oauth/api_scope/sell.fulfillment"
         #"https://api.ebay.com/oauth/api_scope/metadata.insights"
         ]
Condition_ID_MAPPING = {
    '1000': 'NEW',
    '1500': 'NEW_OTHER',
    '1750': 'NEW_WITH_DEFECTS',
    '2000': 'CERTIFIED_REFURBISHED',
    '2010': 'EXCELLENT_REFURBISHED',
    '2020': 'VERY_GOOD_REFURBISHED',
    '2030': 'GOOD_REFURBISHED',
    '2500': 'SELLER_REFURBISHED',
    '2750': 'LIKE_NEW',
    '3000': 'USED_EXCELLENT',
    '4000': 'USED_VERY_GOOD',
    '5000': 'USED_GOOD',
    '6000': 'USED_ACCEPTABLE',
    '7000': 'FOR_PARTS_OR_NOT_WORKING',
}
EXCEL_COL_MAPPING = {
    'action': '*Action(SiteID=UK|Country=LT|Currency=GBP|Version=1193)',
    'sku': 'Custom label (SKU)',
    'categoryId': 'Category ID',
    'storeCategoryNames': 'Category name',
    'product.title': 'Title',
    '': 'Relationship',
    '': 'Relationship details',
    'product.epid': 'P:EPID',
    'pricingSummary.auctionStartPrice': 'Start price',
    'availableQuantity': 'Quantity',
    'condition': 'Condition ID',
    'conditionDescription': 'Description',
    'format': 'Format',
    'listingDuration': 'Duration',
    'listingPolicies.bestOfferTerms.bestOfferEnabled': 'Best Offer Enabled',
    '': 'Best Offer Auto Accept Price',
    '': 'Minimum Best Offer Price',
    'tax.vatPercentage': 'VAT%',
    '': 'Immediate pay required',
    '': 'Location',
    '': 'Shipping service 1 option',
    '': 'Shipping service 1 cost',
    '': 'Shipping service 1 priority',
    '': 'Shipping service 2 option',
    '': 'Shipping service 2 cost',
    '': 'Shipping service 2 priority',
    '': 'Max dispatch time',
    '': 'Returns accepted option',
    '': 'Returns within option',
    '': 'Refund option',
    '': 'Return shipping cost paid by',
    '': 'Shipping profile name',
    '': 'Return profile name',
    '': 'Payment profile name',
    '': 'ProductCompliancePolicyID',
    #'product.brand': 'C:Brand',
    #'product.aspects.Type': 'C:Type',
    #'product.aspects.Size': 'C:Size',
    #'product.aspects.Colour': 'C:Colour',
    #'product.aspects.Style': 'C:Style',
    #'product.aspects.Department': 'C:Department',
    #'product.aspects.Inside Leg': 'C:Inside Leg',
    #'product.aspects.Waist Size': 'C:Waist Size',
    #'product.aspects.Fit': 'C:Fit',
    #'product.aspects.Fabric Type': 'C:Fabric Type',
    #'product.aspects.Features': 'C:Features',
    #'product.aspects.Model': 'C:Model',
    #'product.aspects.Fabric Wash': 'C:Fabric Wash',
    #'product.aspects.Theme': 'C:Theme',
    #'product.aspects.Size Type': 'C:Size Type',
    #'product.aspects.Closure': 'C:Closure',
    #'product.aspects.Material': 'C:Material',
    #'product.aspects.Vintage': 'C:Vintage',
    '': 'Product Safety Pictograms',
    '': 'Product Safety Statements',
    '': 'Product Safety Component',
    'regulatory.manufacturer.companyName': 'Manufacturer Name',
    'regulatory.manufacturer.addressLine1': 'Manufacturer AddressLine1',
    'regulatory.manufacturer.addressLine2': 'Manufacturer AddressLine2',
    'regulatory.manufacturer.city': 'Manufacturer City',
    'regulatory.manufacturer.country': 'Manufacturer Country',
    'regulatory.manufacturer.postalCode': 'Manufacturer PostalCode',
    'regulatory.manufacturer.stateOrProvince': 'Manufacturer StateOrProvince',
    'regulatory.manufacturer.phone': 'Manufacturer Phone',
    'regulatory.manufacturer.email': 'Manufacturer Email',
    'regulatory.responsiblePersons.companyName': 'Responsible Person 1',
    'regulatory.responsiblePersons.types': 'Responsible Person 1 Type',
    'regulatory.responsiblePersons.addressLine1': 'Responsible Person 1 AddressLine1',
    'regulatory.responsiblePersons.addressLine2': 'Responsible Person 1 AddressLine2',
    'regulatory.responsiblePersons.city': 'Responsible Person 1 City',
    'regulatory.responsiblePersons.country': 'Responsible Person 1 Country',
    'regulatory.responsiblePersons.postalCode': 'Responsible Person 1 PostalCode',
    'regulatory.responsiblePersons.stateOrProvince': 'Responsible Person 1 StateOrProvince',
    'regulatory.responsiblePersons.phone': 'Responsible Person 1 Phone',
    'regulatory.responsiblePersons.email': 'Responsible Person 1 Email',
    #'product.aspects.Product Line': 'C:Product Line',
    #'product.aspects.Accents': 'C:Accents',
    #'product.aspects.Country/Region of Manufacture': 'C:Country/Region of Manufacture',
    #'product.aspects.Rise': 'C:Rise',
    #'product.aspects.Pattern': 'C:Pattern',
    #'product.aspects.Handmade': 'C:Handmade',
    #'product.aspects.Personalise': 'C:Personalise',
    #'product.aspects.Garment Care': 'C:Garment Care',
    'product.mpn': 'C:MPN',
    #'product.aspects.Personalisation Instructions': 'C:Personalisation Instructions',
    #'product.aspects.Pocket Type': 'C:Pocket Type',
    #'product.aspects.Season': 'C:Season',
    #'product.aspects.Unit Quantity': 'C:Unit Quantity',
    #'product.aspects.Unit Type': 'C:Unit Type',
}


class EbayAPI:

    def __init__(self, client_id: str, client_secret: str, dev_id: str, test: bool = False):
        self.client_id = client_id
        self.client_secret = client_secret
        self.dev_id = dev_id
        self.base_url = None
        self.df = None
        self.oauth_client = None
        self.token = None
        self.token_client_credentials = None
        self.test = test
        self.state = None
        self.product_aspects_column_list = []
        if test:
            # sandbox url
            self.base_url = 'https://api.sandbox.ebay.com'
            self.base_auth_url = 'https://auth.sandbox.ebay.com'
            self.redirect_uri = "MB_Nirista-MBNirist-listin-zplsvkijr"
            self.token_file = 'ebay_sandbox_api_token.json'
        else:
            # production url
            self.base_url = 'https://api.ebay.com'
            self.base_auth_url = 'https://auth.ebay.com'
            self.redirect_uri = ''
            self.token_file = 'ebay_api_token.json'
        self.token_url = self.base_url + '/identity/v1/oauth2/token'
        self.token_loader()

    def token_saver(self, token):
        logging.debug("Saving token")
        self.token = token
        with open(self.token_file, 'w') as f:
            json.dump(token, fp=f)

    def token_loader(self):
        if not os.path.isfile(self.token_file):
            self.authorize()
            return
        logging.debug("Loading token")
        with open(self.token_file, 'r') as f:
            self.token = json.load(f)

        self.refresh_token()

    def authorize(self):
        AUTHORIZATION_BASE_URL = self.base_auth_url + '/oauth2/authorize'

        extra = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'response_type': 'code'
        }
        kwargs = {
            'response_type': 'code'
        }
        self.oauth_client = OAuth2Session(
            self.client_id,
            scope=SCOPE,
            redirect_uri=self.redirect_uri,
            auto_refresh_kwargs=extra,
            token_updater=self.token_saver,
        )
        authorization_url, self.state = self.oauth_client.authorization_url(AUTHORIZATION_BASE_URL)
        print("Please go here and authorize: {}".format(authorization_url))

        redirect_response = input("Copy & Paste the full redirect URL here then press 'Enter':")

        query = urlparse.urlparse(redirect_response).query
        params = dict(urlparse.parse_qsl(query))

        body = {
            'grant_type': 'authorization_code',
            'code': params.get('code'),
            'redirect_uri': self.redirect_uri
        }
        # Fetch the access token
        self.fetch_access_token(body)

        self.token_saver(self.token)

    def fetch_access_token(self, body: dict = None):
        """
        https://developer.ebay.com/api-docs/static/oauth-auth-code-grant-request.html
        :param body:
        :return:
        """
        uri = '/identity/v1/oauth2/token'

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        payload = {
            'scope': SCOPE,
            'grant_type': 'client_credentials'
        }
        if body:
            payload = body

        response = requests.post(
            self.base_url + uri,
            headers=headers,
            auth=(self.client_id, self.client_secret),
            data=payload
        )

        if response.ok:
            if payload.get('grant_type') == 'client_credentials':
                self.token_client_credentials = response.json()
                logging.info(self.token_client_credentials)
            else:
                self.token = response.json()
                logging.info(self.token)
                return True
        else:
            logging.error(response.content)
        return False

    def refresh_token(self):
        """
        https://developer.ebay.com/api-docs/static/oauth-refresh-token-request.html
        :return:
        """
        logging.debug("Refreshing token...")
        refresh_token = self.token.get('refresh_token')
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'scope': SCOPE
        }

        if self.fetch_access_token(body=payload):
            self.token_saver(self.token)

    def fetch_item_aspects(self):
        """
        https://developer.ebay.com/api-docs/commerce/taxonomy/resources/category_tree/methods/fetchItemAspects
        :return:
        """
        url = self.base_url + '/commerce/taxonomy/v1/get_default_category_tree_id?marketplace_id=EBAY_GB'
        self.fetch_access_token(body=None)

        token = self.token_client_credentials.get('access_token')
        headers = {
            'Content-Language': 'en-US',
            'Content-Type': 'application/json',
            'Authorization': f'IAF {token}'
        }

        response = requests.get(url, headers=headers)
        logging.debug(response.content)
        if response.ok:
            data = response.json()
            logging.debug(data)
            category_tree_id = data.get('categoryTreeId')

            url = self.base_url + f'/commerce/taxonomy/v1/category_tree/{category_tree_id}/fetch_item_aspects'

            response = requests.get(url, headers=headers)
            data = response.json()
            logging.debug(data)

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
        #print(self.df)
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
        """
        https://developer.ebay.com/devzone/xml/docs/reference/ebay/uploadsitehostedpictures.html
        :param filename:
        :return:
        """
        logging.info(f"uploading {filename}")
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
                return response.text
            '''files = [('file', (os.path.basename(filename), open(filename, 'rb'), 'image/jpg'))]
            response = requests.post(self.base_url + uri, data=request_xml, headers=headers, files=files)
            logging.info(response.content)'''
        except Exception as ex:
            logging.exception(ex)
            return None

    def upload_image(self, filename):
        """
        https://developer.ebay.com/devzone/xml/docs/reference/ebay/uploadsitehostedpictures.html
        :param filename:
        :return:
        """
        logging.info(f"uploading {filename}")
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
        logging.debug(response.text)
        return response

    def bulk_create_or_replace_inventory_item(self, inventory_items: list):
        """
        https://developer.ebay.com/api-docs/sell/inventory/resources/inventory_item/methods/bulkCreateOrReplaceInventoryItem#h3-request-headers

        :param inventory_items:
        :return:
        """
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

    def bulk_create_offer(self, offer_items: list):
        """
        https://developer.ebay.com/api-docs/sell/inventory/resources/offer/methods/bulkCreateOffer
        :param offer_items:
        :return:
        """
        logging.info("started")
        uri = '/sell/inventory/v1/bulk_create_offer'

        payload = {
            "requests": offer_items
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

    @staticmethod
    def _get_condition_enum(condition_id: str):
        for key, val in Condition_ID_MAPPING.items():
            if key in condition_id:
                return val
        return None

    def _get_image_full_url(self, image_name_path: str):
        """
        Upload image & return full url of image
        :param sku:
        :return:
        """

        # upload image
        response = self.upload_image1(image_name_path)

        if not response:
            return None

        # Parse the XML data
        tree = ET.ElementTree(ET.fromstring(response))
        root = tree.getroot()

        # Define the namespace
        namespace = {'ns': 'urn:ebay:apis:eBLBaseComponents'}

        # Find the FullURL element
        full_url = root.find('.//ns:FullURL', namespace)
        # Print the FullURL value
        if full_url is not None:
            logging.debug(full_url.text)
            return full_url.text
        else:
            logging.error("FullURL element not found.")
            return None

    @staticmethod
    def list_images_in_directory(directory_path):
        # List of image file extensions to filter
        image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp'}

        # List all files in the given directory
        files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))
                 and os.path.splitext(f)[1].lower() in image_extensions]
        files.sort()

        files = list(map(lambda f: os.path.join(directory_path, f), files))

        return files

    def _generate_images_urls(self, sku: str):
        images_urls = []
        try:
            images_base_directory = CONFIG[environment]['photo_directory']
            images_directory = os.path.join(images_base_directory, sku)
            for image in self.list_images_in_directory(images_directory):
                try:
                    image_full_url = self._get_image_full_url(image)
                    if image_full_url:
                        images_urls.append(image_full_url)
                except Exception as e:
                    logging.exception(e)
        except Exception as e:
            logging.exception(e)
        return images_urls

    def _generate_product_aspects_column_list(self):
        logging.debug("generating product.aspects column list")
        try:
            # Get list of column names
            column_names = self.df.columns.tolist()
            self.product_aspects_column_list = [c for c in column_names if c.startswith('C:')]
        except Exception as e:
            logging.exception(e)

    def _generate_product_aspects(self, row):
        aspects = {}
        for col in self.product_aspects_column_list:
            key = col[2:]
            value = row.get(col)
            if not value or pd.isnull(value):
                continue
            if isinstance(value, str):
                value = value.split('||')
                aspects[key] = value
            else:
                try:
                    value = int(value)
                except:
                    pass
                aspects[key] = [value]

        return aspects

    def _generate_inventory_payload(self, row):
        sku = row.get(EXCEL_COL_MAPPING['sku'])
        if not sku:
            return None
        payload = {
            "locale": "en_US",
            'sku': sku,
            'conditionDescription': row.get(EXCEL_COL_MAPPING['conditionDescription']),
        }

        condition_enum = self._get_condition_enum(row.get(EXCEL_COL_MAPPING['condition']))
        if condition_enum:
            payload['condition'] = condition_enum

        product = {}
        title = row.get(EXCEL_COL_MAPPING['product.title'])
        if title:
            product['title'] = title
        epid = row.get(EXCEL_COL_MAPPING['product.epid'])
        if epid and not pd.isnull(epid):
            product['epid'] = epid

        mpn = row.get(EXCEL_COL_MAPPING['product.mpn'])
        if mpn and mpn.lower() != 'does not apply' and not pd.isnull(mpn):
            product['mpn'] = mpn

        image_urls = self._generate_images_urls(sku)
        if image_urls:
            product['imageUrls'] = image_urls
        product['aspects'] = self._generate_product_aspects(row)
        payload['product'] = product

        logging.debug(pformat(payload))
        return payload

    def _generate_offer_payload(self, row):
        sku = row.get(EXCEL_COL_MAPPING['sku'])
        if not sku:
            return None
        current_datetime_utc = datetime.now(timezone.utc)
        future_datetime_utc = current_datetime_utc + timedelta(days=7)
        formatted_datetime = future_datetime_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        payload = {
            'sku': sku,
            'marketplaceId': 'EBAY_GB',
            'listingStartDate': formatted_datetime,
        }
        quantity = row.get(EXCEL_COL_MAPPING['availableQuantity'])
        if quantity:
            try:
                quantity = int(quantity)
                payload['availableQuantity'] = quantity
            except:
                pass

        categoryId = row.get(EXCEL_COL_MAPPING['categoryId'])
        if categoryId:
            try:
                categoryId = str(int(categoryId))
            except:
                pass
            payload['categoryId'] = categoryId

        format = row.get(EXCEL_COL_MAPPING['format'])
        if format:
            if 'fixed' in format.lower():
                payload['format'] = 'FIXED_PRICE'
            else:
                payload['format'] = 'AUCTION'
        else:
            payload['format'] = 'FIXED_PRICE'

        listingDuration = row.get(EXCEL_COL_MAPPING['listingDuration'])
        if listingDuration:
            payload['listingDuration'] = listingDuration

        bestOfferEnabled = row.get(EXCEL_COL_MAPPING['listingPolicies.bestOfferTerms.bestOfferEnabled'])
        if bestOfferEnabled:
            try:
                bestOfferEnabled = bool(bestOfferEnabled)
                payload['listingPolicies'] = {
                    'bestOfferTerms': {
                        'bestOfferEnabled': bestOfferEnabled
                    }
                }
            except:
                pass

        auctionStartPrice = row.get(EXCEL_COL_MAPPING['pricingSummary.auctionStartPrice'])
        if auctionStartPrice:
            try:
                payload['pricingSummary'] = {
                    'auctionStartPrice': {
                        'currency': 'GBP',
                        'value': str(auctionStartPrice)
                    }
                }
            except:
                pass
        manufacturer = {}
        addressLine1 = row.get(EXCEL_COL_MAPPING['regulatory.manufacturer.addressLine1'])
        if addressLine1:
            manufacturer['addressLine1'] = addressLine1

        addressLine2 = row.get(EXCEL_COL_MAPPING['regulatory.manufacturer.addressLine2'])
        if addressLine2:
            manufacturer['addressLine2'] = addressLine2

        city = row.get(EXCEL_COL_MAPPING['regulatory.manufacturer.city'])
        if city:
            manufacturer['city'] = city

        companyName = row.get(EXCEL_COL_MAPPING['regulatory.manufacturer.companyName'])
        if companyName:
            manufacturer['companyName'] = companyName

        country = row.get(EXCEL_COL_MAPPING['regulatory.manufacturer.country'])
        if country:
            manufacturer['country'] = country

        email = row.get(EXCEL_COL_MAPPING['regulatory.manufacturer.email'])
        if email:
            manufacturer['email'] = email

        phone = row.get(EXCEL_COL_MAPPING['regulatory.manufacturer.phone'])
        if phone:
            try:
                manufacturer['phone'] = str(int(phone))
            except:
                pass

        postalCode = row.get(EXCEL_COL_MAPPING['regulatory.manufacturer.postalCode'])
        if postalCode:
            try:
                manufacturer['postalCode'] = str(int(postalCode))
            except:
                pass

        stateOrProvince = row.get(EXCEL_COL_MAPPING['regulatory.manufacturer.stateOrProvince'])
        if stateOrProvince:
            manufacturer['stateOrProvince'] = stateOrProvince

        regulatory = {}
        if manufacturer:
            regulatory['manufacturer'] = manufacturer

        responsiblePersons = {}
        addressLine1 = row.get(EXCEL_COL_MAPPING['regulatory.responsiblePersons.addressLine1'])
        if addressLine1:
            responsiblePersons['addressLine1'] = addressLine1

        addressLine2 = row.get(EXCEL_COL_MAPPING['regulatory.responsiblePersons.addressLine2'])
        if addressLine2:
            responsiblePersons['addressLine2'] = addressLine2

        city = row.get(EXCEL_COL_MAPPING['regulatory.responsiblePersons.city'])
        if city:
            responsiblePersons['city'] = city

        companyName = row.get(EXCEL_COL_MAPPING['regulatory.responsiblePersons.companyName'])
        if companyName:
            responsiblePersons['companyName'] = companyName

        country = row.get(EXCEL_COL_MAPPING['regulatory.responsiblePersons.country'])
        if country:
            responsiblePersons['country'] = country

        email = row.get(EXCEL_COL_MAPPING['regulatory.responsiblePersons.email'])
        if email:
            responsiblePersons['email'] = email

        phone = row.get(EXCEL_COL_MAPPING['regulatory.responsiblePersons.phone'])
        if phone:
            try:
                responsiblePersons['phone'] = str(int(phone))
            except:
                pass

        postalCode = row.get(EXCEL_COL_MAPPING['regulatory.responsiblePersons.postalCode'])
        if postalCode:
            try:
                responsiblePersons['postalCode'] = str(int(postalCode))
            except:
                pass

        stateOrProvince = row.get(EXCEL_COL_MAPPING['regulatory.responsiblePersons.stateOrProvince'])
        if stateOrProvince:
            responsiblePersons['stateOrProvince'] = stateOrProvince

        types = row.get(EXCEL_COL_MAPPING['regulatory.responsiblePersons.types'])
        if types:
            responsiblePersons['types'] = ['EU_RESPONSIBLE_PERSON']

        if responsiblePersons:
            regulatory['responsiblePersons'] = [responsiblePersons]

        payload['regulatory'] = regulatory

        storeCategoryNames = row.get(EXCEL_COL_MAPPING['storeCategoryNames'])
        if storeCategoryNames:
            payload['storeCategoryNames'] = [storeCategoryNames]

        vatPercentage = row.get(EXCEL_COL_MAPPING['tax.vatPercentage'])
        if vatPercentage:
            try:
                vatPercentage = int(vatPercentage)
                payload['tax'] = {
                    'vatPercentage': vatPercentage,
                    'applyTax': True
                }
            except:
                pass

        logging.debug(pformat(payload))
        return payload

    def list_items(self):
        logging.info("Started")

        self._generate_product_aspects_column_list()

        inventory_items = []
        offer_payloads = []

        # iterate over dataframe
        for index, row in self.df.iterrows():
            try:
                # print(row)
                payload = self._generate_inventory_payload(row)
                # payload = None
                if payload:
                    inventory_items.append(payload)
                offer_payload = self._generate_offer_payload(row)
                if offer_payload:
                    offer_payloads.append(offer_payload)
                # for each 20 items, send a listing request
                if (index+1) % 20 == 0:
                    self.bulk_create_or_replace_inventory_item(inventory_items)
                    # empty the inventory_items
                    inventory_items.clear()

                    self.bulk_create_offer(offer_payloads)
                    offer_payloads.clear()

                # 2-minute pause after every 145 listings
                if (index+1) % 145 == 0:
                    print("Sleeping for 2 min...")
                    logging.info("Sleeping for 2 min...")
                    time.sleep(120)
            except Exception as ex:
                logging.exception(ex)

        # if items in inventory_items then list them
        if inventory_items:
            self.bulk_create_or_replace_inventory_item(inventory_items)
            # empty the inventory_items
            inventory_items.clear()
        if offer_payloads:
            self.bulk_create_offer(offer_payloads)
            offer_payloads.clear()

    def workflow(self, excel_file):
        self.read_excel(excel_file)
        self.list_items()


def load_config(config_file):
    config = configparser.RawConfigParser()
    config.optionxform = lambda option: option
    config.read(config_file)
    return config


def main(args):
    global CONFIG, environment

    # check config file
    config_file = args.ini

    CONFIG = load_config(config_file)

    if args.test:
        logging.info(f"Running in sandbox environment: {__version__}")
        environment = 'sandbox'

    else:
        logging.info(f"Running in production environment: {__version__}")
        environment = 'production'

    ebay = EbayAPI(
        client_id=CONFIG[environment]['client_id'],
        client_secret=CONFIG[environment]['client_secret'],
        dev_id=CONFIG[environment]['dev_id'],
        test=args.test
    )
    ebay.workflow(CONFIG[environment]['excel_name_with_path'])


if __name__ == '__main__':
    """
        Execution starts here.
    """

    parser = argparse.ArgumentParser(description='Ebay Listing Script')
    parser.add_argument('-i', '--ini', help='config filename', type=str, required=False, default='ebay_listing.ini')
    parser.add_argument('-t', '--test', action='store_true', help="To run in sandbox environment, provide this flag")

    args = parser.parse_args()

    main(args)
