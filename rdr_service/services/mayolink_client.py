from dataclasses import dataclass
from datetime import datetime
import json
import httplib2
import logging
from typing import List
from werkzeug.exceptions import ServiceUnavailable
import xml.etree.ElementTree as ET
import xmltodict

import pytz

from rdr_service import config
from rdr_service.api_util import RDR_AND_PTC, open_cloud_file
from rdr_service.app_util import check_auth


@dataclass
class MayolinkQuestion:
    code: str
    prompt: str
    answer: str


@dataclass
class MayolinkTestPassthroughFields:
    field1: str = ''
    field2: str = ''
    field3: str = ''
    field4: str = ''


@dataclass
class MayoLinkTest:
    code: str
    name: str
    comments: str = None
    passthrough_fields: MayolinkTestPassthroughFields = None
    questions: List[MayolinkQuestion] = None


@dataclass
class MayoLinkOrder:
    collected_datetime_utc: datetime
    number: str
    medical_record_number: str
    last_name: str
    sex: str
    address1: str
    address2: str
    city: str
    state: str
    postal_code: str
    phone: str
    race: str
    report_notes: str = ''
    tests: List[MayoLinkTest] = None
    comments: str = ''


class MayoLinkClient:
    def __init__(self, credentials_key='default'):
        self.namespace = "http://orders.mayomedicallaboratories.com"
        self.endpoint = config.getSetting(config.MAYOLINK_ENDPOINT)

        self.username, self.pw, self.account = self._get_credentials(credentials_key=credentials_key)

    @classmethod
    def _get_credentials(cls, credentials_key):
        credentials_bucket_name = config.CONFIG_BUCKET
        credentials_file_name = config.getSetting(config.MAYOLINK_CREDS)
        with open_cloud_file("/" + credentials_bucket_name + "/" + credentials_file_name) as file:
            credentials_json = json.load(file)
            if credentials_key in credentials_json:
                credentials = credentials_json[credentials_key]
            else:
                # If the key is not found, that likely means the file is still a legacy version
                # (where the entire file was one set of credentials)
                credentials = credentials_json

        return credentials.get('username'), credentials.get('password'), credentials.get('account')

    def post(self, order: MayoLinkOrder):
        check_auth(RDR_AND_PTC)
        xml = self.__order_to_mayo_xml__(order)
        return self.__post__(xml)

    def __post__(self, xml):
        http = httplib2.Http()
        http.add_credentials(self.username, self.pw)

        try:
            response, content = http.request(
                self.endpoint, method="POST", headers={"Content-type": "application/xml"}, body=xml
            )
            if response['status'] == "201":
                result = self._xml_to_dict(content)
                return result
            else:
                logging.error(content)
                raise ServiceUnavailable("Mayolink service return {} rather than 201".format(response['status']))
        except httplib2.HttpLib2Error:
            logging.error('HttpLib2Error exception encountered', exc_info=True)
        except OSError:
            logging.error('OSError exception encountered', exc_info=True)

        raise ServiceUnavailable("Mayolink service unavailable, please re-try later")

    def __order_to_mayo_xml__(self, order: MayoLinkOrder):
        order_dict = self._dict_from_order(order)
        orders_element = ET.Element("orders")
        orders_element.set('xmlns', self.namespace)
        tree_root = self.create_xml_tree_from_dict(orders_element, order_dict)
        request = ET.tostring(tree_root, encoding='UTF-8', method='xml')
        return request

    def _dict_from_order(self, order: MayoLinkOrder):
        order_dict = {
            'order': {
                'collected': str(self._convert_to_central_time(order.collected_datetime_utc)),
                'account': self.account,
                'number': order.number,
                'patient': {
                    'medical_record_number': order.medical_record_number,
                    'first_name': '*',
                    'last_name': order.last_name,
                    'middle_name': '',
                    'birth_date': '3/3/1933',
                    'sex': order.sex,
                    'address1': order.address1,
                    'address2': order.address2,
                    'city': order.city,
                    'state': order.state,
                    'postal_code': order.postal_code,
                    'phone': order.phone,
                    'account_number': None,
                    'race': order.race,
                    'ethnic_group': None,
                },
                'physician': {'name': 'None', 'phone': None, 'npi': None},  # must be a string value, not None.
                'report_notes': order.report_notes,
                'tests': [],
                'comments': ''
            }
        }

        if order.tests:
            order_dict['order']['tests'] = [self.dict_from_test(test) for test in order.tests]

        return order_dict

    def dict_from_test(self, test: MayoLinkTest):
        test_data = {
            'code': test.code,
            'name': test.name,
            'comments': test.comments
        }

        if test.questions:
            test_data['questions'] = [
                {
                    'question': {
                        'code': question.code,
                        'prompt': question.prompt,
                        'answer': question.answer
                    }
                }
                for question in test.questions
            ]
        if test.passthrough_fields:
            test_data['client_passthrough_fields'] = {
                'field1': test.passthrough_fields.field1,
                'field2': test.passthrough_fields.field2,
                'field3': test.passthrough_fields.field3,
                'field4': test.passthrough_fields.field4
            }

        return {'test': test_data}

    def _xml_to_dict(self, content):
        result = xmltodict.parse(content)
        return result

    def create_xml_tree_from_dict(self, root, dict_tree: dict):
        if type(dict_tree) == dict:
            for k, v in dict_tree.items():
                if type(v) != list:
                    self.create_xml_tree_from_dict(ET.SubElement(root, k), v)
                else:
                    sub_element = ET.SubElement(root, k)
                    for item in v:
                        self.create_xml_tree_from_dict(sub_element, item)
            return root
        elif dict_tree is not None:
            root.text = str(dict_tree)

    @classmethod
    def _convert_to_central_time(cls, timestamp: datetime):
        # Set the timezone as UTC if it's a naive datetime
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=pytz.utc)

        return timestamp.astimezone(pytz.timezone('US/Central'))
