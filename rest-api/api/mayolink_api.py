import json
import logging

import cloudstorage
import config
from api_util import RDR
from app_util import auth_required


# mayo_test_api = 'https://test.orders.mayocliniclabs.com/orders/create.xml'
# # get response
# response = urllib2.urlopen(mayo_test_api)
# root = ET.parse(response).getroot()
#
# # iterate
# items = root.findall('orders/order')
# for item in items:
#   print item


class MayoLinkApi:

  def __init__(self):
    # super(MayoLinkApi, self).__init__(DvOrderDao())
    self.namespace = 'http://orders.mayomedicallaboratories.com'
    self.config_bucket = config.CONFIG_BUCKET
    self.config = config.getSetting(config.MAYOLINK_CREDS)
    self.path = '/' + self.config_bucket + '/' + self.config
    self.endpoint = config.getSetting(config.MAYOLINK_ENDPOINT)
    with cloudstorage.open(self.path, 'r') as file_path:
      self.creds = json.load(file_path)
      username = self.creds.get('username')
      pw = self.creds.get('password')
      account = self.creds.get('account')

      logging.warn('File from bucket recieved: account {}'.format(account))

    print self.creds


  @auth_required(RDR)
  def post(self, order):
    print order
