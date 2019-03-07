import json
import urllib2
from xml.etree import ElementTree as ET

import cloudstorage
import config
from api.base_api import UpdatableApi
from api_util import RDR
from app_util import auth_required


mayo_test_api = 'https://test.orders.mayocliniclabs.com/orders/create.xml'
# get response
response = urllib2.urlopen(mayo_test_api)
root = ET.parse(response).getroot()

# iterate
items = root.findall('orders/order')
for item in items:
  print item


class MayoLinkApi(UpdatableApi):

  def __init__(self):
    super(MayoLinkApi, self).__init__(MayoLinkApi(), get_returns_children=True)
    self.namespace = 'http://orders.mayomedicallaboratories.com'
    self.config_bucket = config.getSetting(config.CONFIG_BUCKET)
    self.config = config.getSetting(config.MAYOLINK_CREDS)
    self.path = self.config_bucket + '/' + self.config
    self.endpoint = config.getSetting(config.MAYOLINK_ENDPOINT)
    with cloudstorage.open(self.path, 'r') as file_path:
      self.creds = json.load(file_path)


  @auth_required(RDR)
  def post(self, order):
    pass
