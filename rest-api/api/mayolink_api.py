import json
import os
import xml.etree.cElementTree as etree

import cloudstorage
import config
import httplib2
import xmltodict
from api.base_api import UpdatableApi
from api_util import RDR
from app_util import auth_required


class MayoLinkApi(UpdatableApi):

  def __init__(self):
    self.namespace = 'http://orders.mayomedicallaboratories.com'
    self.config_bucket = config.CONFIG_BUCKET
    self.config = config.getSetting(config.MAYOLINK_CREDS)
    self.path = '/' + self.config_bucket + '/' + self.config
    self.endpoint = config.getSetting(config.MAYOLINK_ENDPOINT)
    with cloudstorage.open(self.path, 'r') as file_path:
      self.creds = json.load(file_path)
      self.username = self.creds.get('username')
      self.pw = self.creds.get('password')
      self.account = self.creds.get('account')

  @auth_required(RDR)
  def post(self, order):
    xml = self.__dict_to_mayo_xml__(order)
    return self.__post__(xml)

  def __post__(self, xml):
    http = httplib2.Http()
    http.add_credentials(self.username, self.pw)

    response, content = http.request(self.endpoint,
                                     method="POST",
                                     headers={'Content-type': 'application/xml'},
                                     body=xml)
    if response['status'] != 201:
      pass

    result = self._xml_to_dict(content)
    return result

  def __dict_to_mayo_xml__(self, order):
    base_dir = os.path.abspath(os.path.dirname(__file__)[:-3])
    data_dir = os.path.join(base_dir, 'data')
    tree = etree.parse(os.path.join(data_dir, 'mayo_order.xml'))
    root = tree.getroot()
    # A super lame way to do this, sorry ? :-)
    root[0][0].text = order['order']['collected']
    root[0][1].text = str(self.account)
    root[0][2].text = order['order']['number']
    root[0][3][0].text = order['order']['patient']['medical_record_number']
    root[0][3][1].text = order['order']['patient']['first_name']
    root[0][3][2].text = order['order']['patient']['last_name']
    root[0][3][3].text = order['order']['patient']['middle_name']
    root[0][3][4].text = order['order']['patient']['birth_date']
    root[0][3][5].text = order['order']['patient']['gender']
    root[0][3][6].text = order['order']['patient']['address1']
    root[0][3][7].text = order['order']['patient']['address2']
    root[0][3][8].text = order['order']['patient']['city']
    root[0][3][9].text = order['order']['patient']['state']
    root[0][3][10].text = order['order']['patient']['postal_code']
    root[0][3][11].text = order['order']['patient']['phone']
    root[0][3][12].text = order['order']['patient']['account_number']
    root[0][3][13].text = order['order']['patient']['race']
    root[0][3][14].text = order['order']['patient']['ethnic_group']
    root[0][4][0].text = order['order']['physician']['name']
    root[0][4][1].text = order['order']['physician']['phone']
    root[0][4][2].text = order['order']['physician']['npi']
    root[0][5].text = order['order']['report_notes']
    root[0][6][0][0].text = order['order']['tests']['test']['code']
    root[0][6][0][1].text = order['order']['tests']['test']['name']
    root[0][6][0][2].text = order['order']['tests']['test']['comments']
    root[0][7].text = order['order']['comments']

    request = etree.tostring(root)
    return request

  def _xml_to_dict(self, content):
    result = xmltodict.parse(content)
    return result

