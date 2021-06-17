import json
import httplib2
import logging
import xml.etree.ElementTree as ET
import xmltodict
from werkzeug.exceptions import ServiceUnavailable

from rdr_service import config
from rdr_service.api_util import RDR_AND_PTC, open_cloud_file
from rdr_service.app_util import auth_required


class MayoLinkApi:
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

    @auth_required(RDR_AND_PTC)
    def post(self, order):
        xml = self.__dict_to_mayo_xml__(order)
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

    def __dict_to_mayo_xml__(self, order):
        order['order']['account'] = self.account
        orders_element = ET.Element("orders")
        orders_element.set('xmlns', self.namespace)
        tree_root = self.create_xml_tree_from_dict(orders_element, order)
        request = ET.tostring(tree_root, encoding='UTF-8', method='xml')
        return request

    def _xml_to_dict(self, content):
        result = xmltodict.parse(content)
        return result

    def create_xml_tree_from_dict(self, root, dict_tree):
        if type(dict_tree) == dict:
            for k, v in dict_tree.items():
                if type(v) != list:
                    self.create_xml_tree_from_dict(ET.SubElement(root, k), v)
                else:
                    sub_element = ET.SubElement(root, k)
                    for item in v:
                        self.create_xml_tree_from_dict(sub_element, item)
            return root
        else:
            root.text = str(dict_tree)
