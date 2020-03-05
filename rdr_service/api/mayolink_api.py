import json
import xml.etree.ElementTree as ET
import httplib2
import xmltodict
from werkzeug.exceptions import ServiceUnavailable

from rdr_service import config
from rdr_service.api_util import RDR_AND_PTC, open_cloud_file
from rdr_service.app_util import auth_required


class MayoLinkApi:
    def __init__(self):
        self.namespace = "http://orders.mayomedicallaboratories.com"
        self.config_bucket = config.CONFIG_BUCKET
        self.config = config.getSetting(config.MAYOLINK_CREDS)
        self.path = "/" + self.config_bucket + "/" + self.config
        self.endpoint = config.getSetting(config.MAYOLINK_ENDPOINT)
        # For now I can not figure out how to use google cloud on dev_appserver, comment out the
        # below and manually add self.username, etc.
        with open_cloud_file(self.path) as file_path:
            self.creds = json.load(file_path)
        self.username = self.creds.get("username")
        self.pw = self.creds.get("password")
        self.account = self.creds.get("account")

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
                ServiceUnavailable("Mayolink service return {} rather than 201".format(response['status']))
        except httplib2.HttpLib2Error:
            pass
        except OSError:
            pass

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
